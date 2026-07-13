/* ==========================================================================
   Spårläge — live SL traffic map (rail, metro, tram, bus, boat)

   Data sources:
   - Stations + departures: SL Transport API (transport.integration.sl.se)
     JSON, no key required.
   - Live vehicle GPS: Trafiklab GTFS-RT VehiclePositions feed (protobuf),
     requires a Trafiklab API key with both "GTFS Regional Realtime" and
     "GTFS Regional Static data" added to the project (stored client-side
     in localStorage — fine for a personal hobby project, not meant for
     public distribution since the key is visible in the network tab).

   How mode classification actually works (confirmed live, not guessed):
   - Stations: /v1/stop-points gives each stop_area a `type` — RAILWSTN,
     METROSTN, TRAMSTN, BUSTERM, SHIPBER/FERRYBER — which maps directly to
     a mode. Rail does NOT distinguish Pendeltåg from Roslagsbanan at the
     station level; that split isn't available without a site->line join
     SL's API doesn't expose, so rail stations show one merged color.
   - Vehicles: the real-time feed almost never populates trip.routeId or
     vehicle.label (confirmed: 13 of 1065 vehicles had a routeId at all).
     The only reliable path is joining each vehicle's trip_id against
     SL's static GTFS schedule (routes.txt + trips.txt), which DOES carry
     real route names — this lets Pendeltåg/Roslagsbanan be split
     correctly at the vehicle level, unlike stations.
   ========================================================================== */

const SL_SITES_URL = 'https://transport.integration.sl.se/v1/sites?expand=true';
const SL_STOP_POINTS_URL = 'https://transport.integration.sl.se/v1/stop-points';
const SL_DEPARTURES_URL = (siteId) => `https://transport.integration.sl.se/v1/sites/${siteId}/departures`;
const GTFS_RT_URL = (key) => `https://opendata.samtrafiken.se/gtfs-rt/sl/VehiclePositions.pb?key=${key}`;
const GTFS_STATIC_URL = (key) => `https://opendata.samtrafiken.se/gtfs/sl/sl.zip?key=${key}`; // uses staticApiKey, not apiKey
const TRAIN_TRIPS_CACHE_KEY = 'sl_mode_trips_v2';
const TRAIN_TRIPS_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000; // static schedule data changes daily at most

const VEHICLE_POLL_MS = 15000; // GTFS-RT vehicle positions update ~every 2s server-side, but poll politely
const STOCKHOLM_CENTER = [59.334, 18.06];
const BUS_STATION_MIN_ZOOM = 14; // SL has ~10k+ bus stops — only render up close, or the map is unreadable

// Confirmed live from window.DEBUG_LAST_STOP_POINTS: stop_area.type takes
// exactly these 6 values across the whole SL network.
const STOP_AREA_TYPE_TO_MODE = {
  RAILWSTN: 'rail',   // Pendeltåg + Roslagsbanan share this at station level —
                       // splitting them requires route data we don't have per-site
  METROSTN: 'metro',
  TRAMSTN: 'tram',
  BUSTERM: 'bus',
  SHIPBER: 'boat',
  FERRYBER: 'boat',
};
// When a site serves multiple modes (interchange), show the most significant one.
const MODE_PRIORITY = ['rail', 'metro', 'tram', 'boat', 'bus'];

// Vehicle-level modes: pendeltåg/roslagsbanan ARE split here, since we can
// join trip_id -> route name via static GTFS. metro/tram/bus/boat are
// bucketed by GTFS extended route_type (see classifyRoute()).
const VEHICLE_MODES = ['pendeltag', 'roslagsbanan', 'metro', 'tram', 'bus', 'boat'];

let activeModes = new Set(VEHICLE_MODES); // controls both vehicle + station visibility

// ---- Minimal GTFS-Realtime schema (subset needed for VehiclePositions) ----
// Transcribed field-for-field from the official spec so the wire format
// decodes correctly: https://github.com/MobilityData/gtfs-realtime-bindings
const GTFS_RT_PROTO = `
syntax = "proto2";
package transit_realtime;

message FeedMessage {
  required FeedHeader header = 1;
  repeated FeedEntity entity = 2;
}
message FeedHeader {
  required string gtfs_realtime_version = 1;
  optional uint64 timestamp = 3;
}
message FeedEntity {
  required string id = 1;
  optional VehiclePosition vehicle = 4;
}
message VehiclePosition {
  optional TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 8;
  optional Position position = 2;
  optional uint64 timestamp = 5;
}
message Position {
  required float latitude = 1;
  required float longitude = 2;
  optional float bearing = 3;
  optional float speed = 5;
}
message TripDescriptor {
  optional string trip_id = 1;
  optional string route_id = 5;
}
message VehicleDescriptor {
  optional string id = 1;
  optional string label = 2;
}
`;

// ---- State ----
let map;
let apiKey = localStorage.getItem('trafiklab_api_key') || '';               // realtime (VehiclePositions)
let staticApiKey = localStorage.getItem('trafiklab_static_api_key') || '';  // static GTFS (routes/trips)
let trainTripMap = {};          // trip_id -> mode, from static GTFS
let stationMarkers = new Map(); // siteId -> { marker, mode }
let vehicleMarkers = new Map(); // vehicleId -> Leaflet marker
let FeedMessageType = null;     // protobufjs decoded type, set once on init
let vehiclePollTimer = null;

// ==========================================================================
// Boot
// ==========================================================================
document.addEventListener('DOMContentLoaded', () => {
  initMap();
  wireSetupModal();
  wireBoard();

  loadStations();     // works with no API key

  if (!apiKey || !staticApiKey) {
    document.getElementById('setup-overlay').removeAttribute('hidden');
  }
  if (apiKey) {
    startVehiclePolling();
  } else {
    setStatus('warn', 'Ingen API-nyckel');
  }
  if (staticApiKey) {
    loadTrainTripClassification().then(() => {
      if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
    });
  } else {
    console.warn('[gtfs-static] no static-data key set — vehicles will show as "other" until one is added');
  }

  document.getElementById('debug-toggle').addEventListener('change', () => {
    // Force an immediate re-render with the new debug mode
    if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
  });

  map.on('zoomend', refreshAllStationVisibility);
  wireModeCheckboxes();

  registerServiceWorker();
});

function wireModeCheckboxes() {
  document.querySelectorAll('.mode-checkbox').forEach(input => {
    input.addEventListener('change', () => {
      const mode = input.dataset.mode;
      if (input.checked) activeModes.add(mode);
      else activeModes.delete(mode);
      refreshAllStationVisibility();
      if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
    });
  });
}

function initMap() {
  map = L.map('map', { zoomControl: true, attributionControl: true })
    .setView(STOCKHOLM_CENTER, 11);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO · Data: Trafiklab / SL',
    subdomains: 'abcd',
    maxZoom: 19,
  }).addTo(map);
}

function setStatus(kind, text) {
  const dot = document.getElementById('status-dot');
  const label = document.getElementById('status-text');
  dot.className = kind; // 'live' | 'warn' | 'error' | ''
  label.textContent = text;
}

// ==========================================================================
// Setup modal (API key)
// ==========================================================================
function wireSetupModal() {
  const overlay = document.getElementById('setup-overlay');
  const input = document.getElementById('setup-key-input');
  const staticInput = document.getElementById('setup-static-key-input');
  input.value = apiKey;
  staticInput.value = staticApiKey;

  document.getElementById('setup-save').addEventListener('click', () => {
    const val = input.value.trim();
    const staticVal = staticInput.value.trim();
    if (!val && !staticVal) return;

    if (val) {
      apiKey = val;
      localStorage.setItem('trafiklab_api_key', apiKey);
      startVehiclePolling();
    }
    if (staticVal) {
      staticApiKey = staticVal;
      localStorage.setItem('trafiklab_static_api_key', staticApiKey);
      loadTrainTripClassification().then(() => {
        if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
      });
    }
    overlay.setAttribute('hidden', '');
  });

  document.getElementById('setup-skip').addEventListener('click', () => {
    overlay.setAttribute('hidden', '');
    setStatus('warn', 'Endast stationer (ingen nyckel)');
  });
}

// ==========================================================================
// Stations (SL Transport API — no key needed)
// ==========================================================================
async function loadStations() {
  try {
    const areaIdToMode = await loadAreaModeMap();

    const res = await fetch(SL_SITES_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const sites = await res.json();

    window.DEBUG_LAST_SITES = sites; // inspect in console: window.DEBUG_LAST_SITES[0]
    console.log(`[stations] fetched ${sites.length} SL sites total`);

    let placed = 0;
    sites.forEach(site => {
      const mode = areaIdToMode ? resolveSiteMode(site, areaIdToMode) : (isTrainSite(site) ? 'rail' : null);
      if (!mode) return;
      addStationMarker(site, mode);
      placed++;
    });
    console.log(`[stations] placed ${placed} station markers across all modes`);
  } catch (err) {
    console.error('[stations] failed to load', err);
  }
}

// A site can serve multiple modes (e.g. T-Centralen: rail + metro). Pick
// the highest-priority mode present so the marker reflects the more
// significant service there.
function resolveSiteMode(site, areaIdToMode) {
  if (!Array.isArray(site.stop_areas)) return null;
  const modesHere = new Set(site.stop_areas.map(id => areaIdToMode.get(id)).filter(Boolean));
  return MODE_PRIORITY.find(m => modesHere.has(m)) || null;
}

// stop_area.type is confirmed (via window.DEBUG_LAST_STOP_POINTS) to take
// exactly 6 values covering every mode — see STOP_AREA_TYPE_TO_MODE.
async function loadAreaModeMap() {
  try {
    const res = await fetch(SL_STOP_POINTS_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const stopPoints = await res.json();
    window.DEBUG_LAST_STOP_POINTS = stopPoints; // inspect: window.DEBUG_LAST_STOP_POINTS[0]
    console.log(`[stations] fetched ${stopPoints.length} SL stop points`);

    const map = new Map();
    stopPoints.forEach(sp => {
      if (!sp.stop_area) return;
      const mode = STOP_AREA_TYPE_TO_MODE[sp.stop_area.type];
      if (mode) map.set(sp.stop_area.id, mode);
    });

    console.log(`[stations] ${map.size} stop_area ids mapped to a mode`);
    return map.size ? map : null;
  } catch (err) {
    console.error('[stations] /stop-points failed, falling back to isTrainSite() guess on /sites', err);
    return null;
  }
}

// Isolated on purpose — the exact field names in /v1/sites?expand=true
// weren't confirmed ahead of time. This checks a few plausible shapes.
// Inspect window.DEBUG_LAST_SITES[0] in devtools to correct if needed.
function isTrainSite(site) {
  const candidateLists = [site.stop_points, site.lines, site.metro_stations].filter(Boolean);
  for (const list of candidateLists) {
    if (Array.isArray(list) && list.some(item =>
        (item.transport_mode || item.mode || '').toString().toUpperCase().includes('TRAIN'))) {
      return true;
    }
  }
  // Fallback: some site payloads may list modes directly
  if (Array.isArray(site.transport_modes)) {
    return site.transport_modes.some(m => (m || '').toUpperCase().includes('TRAIN'));
  }
  return false;
}

function getSiteLatLng(site) {
  // Defensive against a couple of plausible coordinate shapes
  if (typeof site.lat === 'number' && typeof site.lon === 'number') return [site.lat, site.lon];
  if (typeof site.latitude === 'number' && typeof site.longitude === 'number') return [site.latitude, site.longitude];
  if (site.location && typeof site.location.lat === 'number') return [site.location.lat, site.location.lon];
  return null;
}

function addStationMarker(site, mode) {
  const latlng = getSiteLatLng(site);
  if (!latlng) {
    console.warn('[stations] no coordinates found for site, skipping', site);
    return;
  }
  const icon = L.divIcon({ className: `station-marker station-${mode}`, iconSize: [11, 11] });
  const marker = L.marker(latlng, { icon, keyboard: false })
    .bindTooltip(site.name || 'Station', { direction: 'top', offset: [0, -6] });

  marker.on('click', () => openBoard(site));
  stationMarkers.set(site.id, { marker, mode });
  updateStationMarkerVisibility(marker, mode);
}

// Rail/metro/tram/boat stations follow the legend checkboxes only. Bus
// stops additionally require zoom >= BUS_STATION_MIN_ZOOM — SL has
// thousands of them, so showing them at city-wide zoom would drown
// everything else out.
function updateStationMarkerVisibility(marker, mode) {
  const modeEnabled = mode === 'rail'
    ? (activeModes.has('pendeltag') || activeModes.has('roslagsbanan'))
    : activeModes.has(mode);
  const zoomOk = mode !== 'bus' || map.getZoom() >= BUS_STATION_MIN_ZOOM;
  const shouldShow = modeEnabled && zoomOk;
  const isShown = map.hasLayer(marker);
  if (shouldShow && !isShown) marker.addTo(map);
  if (!shouldShow && isShown) map.removeLayer(marker);
}

function refreshAllStationVisibility() {
  for (const { marker, mode } of stationMarkers.values()) {
    updateStationMarkerVisibility(marker, mode);
  }
}

// ==========================================================================
// Departure board
// ==========================================================================
function wireBoard() {
  document.getElementById('board-close').addEventListener('click', closeBoard);
}

function openBoard(site) {
  const board = document.getElementById('board');
  document.getElementById('board-station-name').textContent = site.name || 'Station';
  board.classList.add('open');
  board.setAttribute('aria-hidden', 'false');
  loadDepartures(site);
}

function closeBoard() {
  const board = document.getElementById('board');
  board.classList.remove('open');
  board.setAttribute('aria-hidden', 'true');
}

async function loadDepartures(site) {
  const empty = document.getElementById('board-empty');
  const table = document.getElementById('board-table');
  const rows = document.getElementById('board-rows');
  empty.hidden = false;
  empty.textContent = 'Hämtar avgångar…';
  table.hidden = true;
  rows.innerHTML = '';

  try {
    const res = await fetch(SL_DEPARTURES_URL(site.id));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    window.DEBUG_LAST_DEPARTURES = data;

    const departures = extractDepartures(data);

    if (!departures.length) {
      empty.textContent = 'Inga kommande avgångar hittades för den här stationen just nu.';
      return;
    }

    departures
      .sort((a, b) => new Date(a.time) - new Date(b.time))
      .slice(0, 25)
      .forEach(dep => rows.appendChild(renderDepartureRow(dep)));

    empty.hidden = true;
    table.hidden = false;
  } catch (err) {
    console.error('[departures] failed', err);
    empty.textContent = 'Kunde inte hämta avgångar (se konsolen för detaljer).';
  }
}

// Isolated for the same reason as other schema-guess functions — adjust
// against window.DEBUG_LAST_DEPARTURES if the shape differs from what's
// assumed here. No longer mode-filtered: since every station type is
// clickable now, show whatever departs from that specific site.
function extractDepartures(data) {
  const list = Array.isArray(data) ? data : (data.departures || []);
  if (!Array.isArray(list)) {
    console.warn('[departures] unexpected response shape', data);
    return [];
  }
  return list.map(dep => ({
    line: (dep.line && (dep.line.designation || dep.line.name)) || dep.designation || '?',
    destination: dep.destination || (dep.direction) || '',
    time: dep.expected || (dep.departure && dep.departure.time) || dep.scheduled,
    deviation: !!(dep.deviations && dep.deviations.length),
  }));
}

function renderDepartureRow(dep) {
  const tr = document.createElement('tr');
  const timeLabel = formatDepartureTime(dep.time);
  tr.innerHTML = `
    <td><span class="dep-line">${escapeHtml(dep.line)}</span></td>
    <td>${escapeHtml(dep.destination)}</td>
    <td class="dep-time ${dep.deviation ? 'deviation' : ''}">${timeLabel}</td>
  `;
  return tr;
}

function formatDepartureTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return '—';
  const diffMin = Math.round((d - new Date()) / 60000);
  if (diffMin <= 0) return 'nu';
  if (diffMin < 60) return `${diffMin} min`;
  return d.toLocaleTimeString('sv-SE', { hour: '2-digit', minute: '2-digit' });
}

function escapeHtml(str) {
  return String(str ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

// ==========================================================================
// Live vehicle positions (GTFS-RT protobuf, requires API key)
// ==========================================================================
function startVehiclePolling() {
  if (!window.protobuf) {
    console.error('[vehicles] protobufjs did not load — check network/CDN access');
    setStatus('error', 'protobufjs saknas');
    return;
  }
  const root = protobuf.parse(GTFS_RT_PROTO).root;
  FeedMessageType = root.lookupType('transit_realtime.FeedMessage');

  fetchVehiclePositions();
  if (vehiclePollTimer) clearInterval(vehiclePollTimer);
  vehiclePollTimer = setInterval(fetchVehiclePositions, VEHICLE_POLL_MS);
}

async function fetchVehiclePositions() {
  try {
    const res = await fetch(GTFS_RT_URL(apiKey));
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status} ${text}`.trim());
    }
    const buf = new Uint8Array(await res.arrayBuffer());
    const message = FeedMessageType.decode(buf);
    const obj = FeedMessageType.toObject(message, { defaults: true });

    const vehicles = (obj.entity || [])
      .map(e => e.vehicle)
      .filter(Boolean);

    window.DEBUG_LAST_VEHICLES = vehicles; // inspect in console to tune classifyVehicle()
    renderVehicles(vehicles);
    setStatus('live', `${vehicles.length} fordon`);
  } catch (err) {
    // A CORS failure surfaces here as a generic "Failed to fetch" TypeError —
    // the browser hides the real reason from JS. Check the Network tab for
    // a request that fails before any response headers arrive.
    console.error('[vehicles] fetch failed', err);
    if (err instanceof TypeError) {
      setStatus('error', 'Nätverksfel — se konsolen (troligen CORS)');
    } else {
      setStatus('error', err.message || 'Fel vid hämtning');
    }
  }
}

// ==========================================================================
// Train trip classification (static GTFS: trip_id -> route -> mode)
//
// The real-time VehiclePositions feed almost never populates trip.routeId
// or vehicle.label (confirmed live: 13 of 1065 vehicles had a routeId at
// all). trip_id IS populated reliably, so the only solid way to know which
// vehicles are Pendeltåg/Roslagsbanan is to join trip_id against the static
// GTFS schedule, which does carry real route names.
//
// This downloads SL's full static GTFS zip (all modes — buses, metro, tram,
// boats, trains), extracts just routes.txt + trips.txt, and keeps only the
// trip_ids belonging to train routes. That's a heavier one-time fetch, so
// it's cached in localStorage for 24h (static schedules don't change more
// often than that).
// ==========================================================================
async function loadTrainTripClassification() {
  const cached = readTrainTripCache();
  if (cached) {
    trainTripMap = cached;
    console.log(`[gtfs-static] using cached classification (${Object.keys(trainTripMap).length} train trips)`);
    return;
  }

  if (!window.JSZip || !window.Papa) {
    console.error('[gtfs-static] JSZip/PapaParse missing — check CDN access');
    return;
  }

  console.log('[gtfs-static] downloading SL static GTFS feed (one-time, ~daily)…');
  try {
    const res = await fetch(GTFS_STATIC_URL(staticApiKey));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const zip = await JSZip.loadAsync(await res.arrayBuffer());

    const routesCsv = await zip.file('routes.txt').async('string');
    const tripsCsv = await zip.file('trips.txt').async('string');

    const routes = Papa.parse(routesCsv, { header: true, skipEmptyLines: true }).data;
    const trips = Papa.parse(tripsCsv, { header: true, skipEmptyLines: true }).data;

    const routeClassification = new Map(); // route_id -> 'pendeltag' | 'roslagsbanan'
    routes.forEach(r => {
      const kind = classifyRoute(r);
      if (kind) routeClassification.set(r.route_id, kind);
    });
    console.log(`[gtfs-static] ${routeClassification.size} train routes found in routes.txt`);

    const map = {};
    trips.forEach(t => {
      const kind = routeClassification.get(t.route_id);
      if (kind) map[t.trip_id] = kind;
    });
    console.log(`[gtfs-static] ${Object.keys(map).length} train trips mapped`);

    trainTripMap = map;
    writeTrainTripCache(map);
  } catch (err) {
    console.error('[gtfs-static] failed to load/parse static GTFS — vehicles will show as "other"', err);
  }
}

// Isolated for visibility/debugging. Pendeltåg/Roslagsbanan are matched by
// name text (SL's public route names reliably say so). Everything else is
// bucketed by GTFS extended route_type: values are either the basic 0-12
// GTFS types, or an "extended" type where the hundreds digit gives the
// category (100=rail, 400=metro, 700=bus, 900=tram, 1000=water) per the
// Google Transit extended route types spec. If this misclassifies
// something, inspect window.DEBUG_LAST_ROUTES (set manually via a
// breakpoint in loadTrainTripClassification) and adjust here.
function classifyRoute(route) {
  const haystack = `${route.route_long_name || ''} ${route.route_short_name || ''} ${route.route_desc || ''}`.toLowerCase();
  if (haystack.includes('roslagsban')) return 'roslagsbanan';
  if (haystack.includes('pendeltåg') || haystack.includes('pendeltag')) return 'pendeltag';

  const rt = parseInt(route.route_type, 10);
  if (Number.isNaN(rt)) return null;
  const bucket = rt < 100 ? rt : Math.floor(rt / 100) * 100;
  switch (bucket) {
    case 0: case 900: return 'tram';
    case 1: case 400: return 'metro';
    case 3: case 700: return 'bus';
    case 4: case 1000: return 'boat';
    default: return null; // includes unmatched rail (100/2) — rare, stays "other"
  }
}

function readTrainTripCache() {
  try {
    const raw = localStorage.getItem(TRAIN_TRIPS_CACHE_KEY);
    if (!raw) return null;
    const { fetchedAt, map } = JSON.parse(raw);
    if (Date.now() - fetchedAt > TRAIN_TRIPS_CACHE_MAX_AGE_MS) return null;
    return map;
  } catch {
    return null;
  }
}

function writeTrainTripCache(map) {
  try {
    localStorage.setItem(TRAIN_TRIPS_CACHE_KEY, JSON.stringify({ fetchedAt: Date.now(), map }));
  } catch (err) {
    console.warn('[gtfs-static] could not cache classification (localStorage full?)', err);
  }
}

// trip_id -> mode, joined against the static GTFS classification built in
// loadTrainTripClassification(). This is the only reliable path — see the
// header comment on why route_id/label aren't usable.
function classifyVehicle(v) {
  const tripId = v.trip && v.trip.tripId;
  return (tripId && trainTripMap[tripId]) || 'other';
}

function renderVehicles(vehicles) {
  const showUnclassified = document.getElementById('debug-toggle').checked;
  const seen = new Set();

  vehicles.forEach(v => {
    if (!v.position) return;
    const kind = classifyVehicle(v);
    if (kind === 'other') {
      if (!showUnclassified) return;
    } else if (!activeModes.has(kind)) {
      return;
    }

    const id = (v.vehicle && v.vehicle.id) || (v.trip && v.trip.tripId) || `${v.position.latitude},${v.position.longitude}`;
    seen.add(id);
    const latlng = [v.position.latitude, v.position.longitude];

    let marker = vehicleMarkers.get(id);
    if (!marker) {
      const icon = L.divIcon({
        className: `train-marker-wrap train-${kind}`,
        html: '<div class="train-marker-pulse"></div><div class="train-marker-dot"></div>',
        iconSize: [22, 22],
      });
      marker = L.marker(latlng, { icon, keyboard: false, zIndexOffset: 500 }).addTo(map);
      vehicleMarkers.set(id, marker);
    } else {
      marker.setLatLng(latlng);
    }

    const label = (v.vehicle && v.vehicle.label) || id;
    const speedKmh = v.position.speed != null ? Math.round(v.position.speed * 3.6) : null;
    marker.bindPopup(`
      <p class="popup-title">${escapeHtml(label)}</p>
      <p class="popup-meta">${kind === 'other' ? 'Okänd linjetyp' : kind}${speedKmh != null ? ` · ${speedKmh} km/h` : ''}</p>
    `);
  });

  // Drop markers for vehicles no longer present in this poll
  for (const [id, marker] of vehicleMarkers) {
    if (!seen.has(id)) {
      map.removeLayer(marker);
      vehicleMarkers.delete(id);
    }
  }
}

// ==========================================================================
// PWA install support
// ==========================================================================
function registerServiceWorker() {
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('sw.js').catch(err =>
      console.warn('[sw] registration failed', err));
  }
}
