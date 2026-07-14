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
     METROSTN, TRAMSTN, BUSTERM, SHIPBER/FERRYBER. This is a good first
     pass but NOT enough on its own: Roslagsbanan stop_areas are typed
     TRAMSTN (it's narrow-gauge/light-rail, so SL buckets it with real
     trams), which would otherwise misclassify it. The authoritative fix:
     join the static GTFS stop_times.txt against the same train trip_ids
     already identified for vehicle classification, giving the *actual*
     station list per line (pendeltag vs roslagsbanan) rather than relying
     on the type field. See buildRailAreaOverrides().
   - Vehicles: the real-time feed almost never populates trip.routeId or
     vehicle.label (confirmed: 13 of 1065 vehicles had a routeId at all).
     The only reliable path is joining each vehicle's trip_id against
     SL's static GTFS schedule (routes.txt + trips.txt), which DOES carry
     real route names.
   ========================================================================== */

const SL_SITES_URL = 'https://transport.integration.sl.se/v1/sites?expand=true';
const SL_STOP_POINTS_URL = 'https://transport.integration.sl.se/v1/stop-points';
const SL_DEPARTURES_URL = (siteId) => `https://transport.integration.sl.se/v1/sites/${siteId}/departures`;
const GTFS_RT_URL = (key) => `https://opendata.samtrafiken.se/gtfs-rt/sl/VehiclePositions.pb?key=${key}`;
const GTFS_STATIC_URL = (key) => `https://opendata.samtrafiken.se/gtfs/sl/sl.zip?key=${key}`; // uses staticApiKey, not apiKey
const RESROBOT_LOCATION_URL = (query, key) =>
  `https://api.resrobot.se/v2.1/location.name?input=${encodeURIComponent(query)}&type=SA&format=json&accessId=${key}`;
const RESROBOT_TRIP_URL = (params, key) => {
  const qs = new URLSearchParams({ ...params, format: 'json', accessId: key });
  return `https://api.resrobot.se/v2.1/trip?${qs.toString()}`;
};
// ResRobot's Product.catCode (1-9) maps roughly onto our existing mode
// palette, so trip legs can reuse the same colors as the live map.
const CAT_CODE_TO_MODE = { '5': 'metro', '6': 'tram', '7': 'bus', '8': 'boat' }; // 1,2,4 (various trains) fall back to 'pendeltag' color
const TRAIN_TRIPS_CACHE_KEY = 'sl_mode_trips_v3';
const TRAIN_TRIPS_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000; // static schedule data changes daily at most
const LEGEND_PREFS_KEY = 'sparlage_legend_prefs_v1';

const VEHICLE_POLL_MS = 15000; // GTFS-RT vehicle positions update ~every 2s server-side, but poll politely
const STOCKHOLM_CENTER = [59.334, 18.06];
const BUS_STATION_MIN_ZOOM = 14; // SL has ~10k+ bus stops — only render up close, or the map is unreadable

// Confirmed live from window.DEBUG_LAST_STOP_POINTS: stop_area.type takes
// exactly these 6 values across the whole SL network. RAILWSTN is used as
// a "pendeltag" placeholder until the GTFS-derived override (see above)
// corrects it — most RAILWSTN stops genuinely are Pendeltåg, so this is a
// reasonable pre-override guess, not the final answer.
const STOP_AREA_TYPE_TO_MODE = {
  RAILWSTN: 'pendeltag',
  METROSTN: 'metro',
  TRAMSTN: 'tram',   // includes Roslagsbanan until overridden — see above
  BUSTERM: 'bus',
  SHIPBER: 'boat',
  FERRYBER: 'boat',
};
// When a site serves multiple modes (interchange), show the most significant one.
const MODE_PRIORITY = ['pendeltag', 'roslagsbanan', 'metro', 'tram', 'boat', 'bus'];

// Vehicle-level modes: pendeltåg/roslagsbanan ARE split here, since we can
// join trip_id -> route name via static GTFS. metro/tram/bus/boat are
// bucketed by GTFS extended route_type (see classifyRoute()).
const VEHICLE_MODES = ['pendeltag', 'roslagsbanan', 'metro', 'tram', 'bus', 'boat'];

let activeModes = new Set(VEHICLE_MODES); // controls both vehicle + station visibility
let vehiclesVisible = true; // master toggle, independent of per-mode filters

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
let resrobotApiKey = localStorage.getItem('trafiklab_resrobot_api_key') || ''; // ResRobot (journey planner)
let journeyFrom = null; // { label, lat, lon, extId? } — extId set only when a real stop was picked
let journeyTo = null;
let trainTripMap = {};          // trip_id -> mode, from static GTFS
let railAreaModeOverride = new Map(); // stop_area id -> 'pendeltag' | 'roslagsbanan', authoritative (from stop_times.txt join)
let stopPointsCache = null;     // cached /v1/stop-points response, shared between station classification and the override join
let gidToAreaId = new Map();    // stop-point gid -> stop_area id, used to join GTFS stop_id against SL's site model
let stationMarkers = new Map(); // siteId -> { marker, mode, site }
let vehicleMarkers = new Map(); // vehicleId -> Leaflet marker
let FeedMessageType = null;     // protobufjs decoded type, set once on init
let vehiclePollTimer = null;

// ==========================================================================
// Boot
// ==========================================================================
document.addEventListener('DOMContentLoaded', () => {
  wireSetupModal();
  wireJourneyPanel();
  wireStartScreen();
  registerServiceWorker();
});

function wireStartScreen() {
  document.getElementById('start-map').addEventListener('click', startMapMode);
  document.getElementById('start-journey').addEventListener('click', startJourneyMode);
}

// The full, heavier path: map, ~6500 stations, live vehicles, static GTFS
// classification. Only runs once the person actually chooses it — this is
// what the loading overlay covers, since it's the slow part.
async function startMapMode() {
  document.getElementById('start-screen').setAttribute('hidden', '');
  document.getElementById('loading-overlay').removeAttribute('hidden');
  document.getElementById('titlebar').removeAttribute('hidden');
  document.getElementById('legend').removeAttribute('hidden');

  initMap();
  wireBoard();

  loadLegendPrefs(); // must run before loadStations() — activeModes affects which markers get created
  syncLegendCheckboxesToState();
  cleanupStaleLocalStorageKeys();

  if (!apiKey || !staticApiKey || !resrobotApiKey) {
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

  document.getElementById('vehicles-toggle').addEventListener('change', (e) => {
    vehiclesVisible = e.target.checked;
    saveLegendPrefs();
    if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
  });

  document.getElementById('debug-toggle').addEventListener('change', () => {
    // Force an immediate re-render with the new debug mode
    if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
  });

  map.on('zoomend', refreshAllStationVisibility);
  wireModeCheckboxes();

  await loadStations(); // loading overlay stays up until the initial station markers are placed
  document.getElementById('loading-overlay').setAttribute('hidden', '');
}

// The lightweight path: the journey planner needs no map, no station data,
// and no vehicle polling at all — it's pure ResRobot API calls triggered
// by typing, so there's nothing to preload and no loading overlay needed.
function startJourneyMode() {
  document.getElementById('start-screen').setAttribute('hidden', '');

  const panel = document.getElementById('journey-panel');
  panel.classList.add('standalone', 'open');
  panel.setAttribute('aria-hidden', 'false');
  document.getElementById('journey-back').removeAttribute('hidden');
  document.getElementById('journey-close').setAttribute('hidden', '');
  // Reloading is the simplest reliable way back to a clean start screen —
  // avoids having to hand-unwind map/polling state that was never started
  // in this path anyway.
  document.getElementById('journey-back').addEventListener('click', () => location.reload(), { once: true });

  if (!resrobotApiKey) {
    document.getElementById('setup-overlay').removeAttribute('hidden');
  }
}

function wireModeCheckboxes() {
  document.querySelectorAll('.mode-checkbox').forEach(input => {
    input.addEventListener('change', () => {
      const mode = input.dataset.mode;
      if (input.checked) activeModes.add(mode);
      else activeModes.delete(mode);
      saveLegendPrefs();
      refreshAllStationVisibility();
      if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
    });
  });
}

// First-time visitors get a lighter default (just the two trains — the
// core use case) rather than every mode at once, since rendering ~6500
// sites plus hundreds of vehicles all at startup is what was causing the
// lag. Returning visitors get back whatever they last had on.
function loadLegendPrefs() {
  try {
    const raw = localStorage.getItem(LEGEND_PREFS_KEY);
    if (raw) {
      const prefs = JSON.parse(raw);
      activeModes = new Set(prefs.activeModes);
      vehiclesVisible = prefs.vehiclesVisible !== false;
      return;
    }
  } catch (err) {
    console.warn('[legend] could not read saved preferences, using defaults', err);
  }
  activeModes = new Set(['roslagsbanan']);
  vehiclesVisible = true;
}

function saveLegendPrefs() {
  try {
    localStorage.setItem(LEGEND_PREFS_KEY, JSON.stringify({
      activeModes: [...activeModes],
      vehiclesVisible,
    }));
  } catch (err) {
    console.warn('[legend] could not save preferences (localStorage full?)', err);
  }
}

function syncLegendCheckboxesToState() {
  document.getElementById('vehicles-toggle').checked = vehiclesVisible;
  document.querySelectorAll('.mode-checkbox').forEach(input => {
    input.checked = activeModes.has(input.dataset.mode);
  });
}

// The classification cache used to live in localStorage under these keys
// before moving to IndexedDB (see idbGet/idbSet) — each could hold a ~150k
// entry JSON blob, likely a real contributor to the quota-exceeded errors
// seen in testing. Harmless to remove if already gone.
function cleanupStaleLocalStorageKeys() {
  ['sl_mode_trips_v1', 'sl_mode_trips_v2', 'sl_mode_trips_v3'].forEach(key => {
    try { localStorage.removeItem(key); } catch { /* ignore */ }
  });
}

function initMap() {
  map = L.map('map', {
    zoomControl: true,
    attributionControl: true,
    // Leaflet's smooth zoom uses a CSS transform transition, which can get
    // interrupted/frozen mid-animation on a page with thousands of marker
    // DOM elements — confirmed live via the clean per-mode color banding
    // seen when zoomed all the way out (each mode's markers, created
    // together in the same batch, froze at a similar wrong position).
    // Disabling it trades the smooth zoom transition for eliminating that
    // whole class of bug: zoom now jumps between levels instantly instead.
    zoomAnimation: false,
  }).setView(STOCKHOLM_CENTER, 11);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO · Data: Trafiklab / SL',
    subdomains: 'abcd',
    maxZoom: 19,
  }).addTo(map);

  // If the container's final pixel size isn't settled at the exact moment
  // L.map() runs (common in full-screen layouts, especially mobile where
  // the address bar changing height shifts the viewport after load),
  // Leaflet's internal pixel-origin cache goes stale — every marker then
  // sits off by a fixed screen-pixel amount, which is invisible up close
  // but represents more real-world distance the further you zoom out.
  // Forcing a recalculation after load (and on resize) fixes this class
  // of bug outright.
  // Confirmed live (via console): Leaflet's own per-marker position sync
  // doesn't reliably keep up with zoom changes in this app — markers were
  // found sitting at stale screen positions from a previous zoom level,
  // while map.latLngToContainerPoint() correctly reported where they
  // SHOULD be. Forcing a hard view reset fixes it instantly and exactly
  // (verified against multiple markers). Wired to zoomend/moveend AND
  // called directly after applyRailOverrides() — markers recreated there
  // happen asynchronously, well after page load, and won't get corrected
  // by a zoom event unless one happens to fire afterward.
  map.on('zoomend moveend', forceViewResync);

  setTimeout(() => map.invalidateSize(), 200);
  window.addEventListener('resize', () => map.invalidateSize());
  window.addEventListener('orientationchange', () => map.invalidateSize());
}

// This uses Leaflet's private _resetView because it's the one thing
// empirically confirmed to fix the exact observed drift bug; if a future
// Leaflet version removes/renames it, this silently becomes a no-op rather
// than erroring. Guarded against re-entrancy because _resetView itself
// fires moveend/zoomend, which would otherwise call this function again.
let resettingView = false;
function forceViewResync() {
  if (resettingView || !map) return;
  resettingView = true;
  try {
    map._resetView(map.getCenter(), map.getZoom(), true);
  } catch (err) {
    console.warn('[map] _resetView unavailable, positions may drift on zoom', err);
  } finally {
    resettingView = false;
  }
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
  const resrobotInput = document.getElementById('setup-resrobot-key-input');
  input.value = apiKey;
  staticInput.value = staticApiKey;
  resrobotInput.value = resrobotApiKey;

  document.getElementById('setup-save').addEventListener('click', () => {
    const val = input.value.trim();
    const staticVal = staticInput.value.trim();
    const resrobotVal = resrobotInput.value.trim();
    if (!val && !staticVal && !resrobotVal) return;

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
    if (resrobotVal) {
      resrobotApiKey = resrobotVal;
      localStorage.setItem('trafiklab_resrobot_api_key', resrobotApiKey);
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

// A site can serve multiple modes (e.g. T-Centralen: rail + metro). The
// GTFS-derived override (pendeltag/roslagsbanan, authoritative) is checked
// first; falls back to the stop_area.type-based guess otherwise.
function resolveSiteMode(site, areaIdToMode) {
  if (!Array.isArray(site.stop_areas)) return null;
  for (const areaId of site.stop_areas) {
    if (railAreaModeOverride.has(areaId)) return railAreaModeOverride.get(areaId);
  }
  const modesHere = new Set(site.stop_areas.map(id => areaIdToMode.get(id)).filter(Boolean));
  return MODE_PRIORITY.find(m => modesHere.has(m)) || null;
}

// stop_area.type is confirmed (via window.DEBUG_LAST_STOP_POINTS) to take
// exactly 6 values covering every mode — see STOP_AREA_TYPE_TO_MODE.
// Fetches once and caches (stopPointsCache/gidToAreaId) since both station
// classification and the later GTFS override join need the same data.
async function loadAreaModeMap() {
  try {
    const stopPoints = await loadStopPoints();
    if (!stopPoints) return null;

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

async function loadStopPoints() {
  if (stopPointsCache) return stopPointsCache;
  const res = await fetch(SL_STOP_POINTS_URL);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const stopPoints = await res.json();
  window.DEBUG_LAST_STOP_POINTS = stopPoints; // inspect: window.DEBUG_LAST_STOP_POINTS[0]
  console.log(`[stations] fetched ${stopPoints.length} SL stop points`);

  stopPoints.forEach(sp => {
    if (sp.gid != null && sp.stop_area) gidToAreaId.set(String(sp.gid), sp.stop_area.id);
  });

  stopPointsCache = stopPoints;
  return stopPoints;
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
  const icon = L.divIcon({ className: `station-marker station-${mode}`, iconSize: [12, 12], iconAnchor: [6, 6] });
  const marker = L.marker(latlng, { icon, keyboard: false })
    .bindTooltip(site.name || 'Station', { direction: 'top', offset: [0, -6] });

  marker.on('click', () => openBoard(site));
  stationMarkers.set(site.id, { marker, mode, site });
  updateStationMarkerVisibility(marker, mode);
}

// Bus stops additionally require zoom >= BUS_STATION_MIN_ZOOM — SL has
// thousands of them, so showing them at city-wide zoom would drown
// everything else out. Every other mode just follows its checkbox.
function updateStationMarkerVisibility(marker, mode) {
  const modeEnabled = activeModes.has(mode);
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
// Minimal IndexedDB key-value helper
//
// The trip classification map now covers every mode (not just trains),
// which grew it to ~150k entries — confirmed live to exceed localStorage's
// quota (~5-10MB), causing writeTrainTripCache to silently fail every
// time and forcing a full static GTFS re-download on every page load.
// IndexedDB has a much larger quota (typically a share of available disk
// space), so the cache is stored there instead.
// ==========================================================================
const IDB_NAME = 'sparlage-cache';
const IDB_STORE = 'kv';

function idbOpen() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(IDB_STORE);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function idbGet(key) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readonly');
    const req = tx.objectStore(IDB_STORE).get(key);
    req.onsuccess = () => resolve(req.result ?? null);
    req.onerror = () => reject(req.error);
  });
}

async function idbSet(key, value) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readwrite');
    tx.objectStore(IDB_STORE).put(value, key);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}
//
// The real-time VehiclePositions feed almost never populates trip.routeId
// or vehicle.label (confirmed live: 13 of 1065 vehicles had a routeId at
// all). trip_id IS populated reliably, so the only solid way to know which
// vehicles are Pendeltåg/Roslagsbanan is to join trip_id against the static
// GTFS schedule, which does carry real route names.
//
// This ALSO derives the station-level override (railAreaModeOverride) by
// joining the same train trip_ids against stop_times.txt, which lists every
// stop each trip actually calls at — see buildRailAreaOverrides(). This is
// what corrects Roslagsbanan showing up as "Spårvagn" (its stop_area.type
// is TRAMSTN, since it's narrow-gauge/light-rail — not a bug in the join,
// just how SL categorizes the physical infrastructure).
//
// Downloads SL's full static GTFS zip (all modes — buses, metro, tram,
// boats, trains) once, cached in localStorage for 24h (static schedules
// don't change more often than that).
// ==========================================================================
async function loadTrainTripClassification() {
  const cached = await readTrainTripCache();
  if (cached) {
    trainTripMap = cached.tripMap;
    railAreaModeOverride = new Map(Object.entries(cached.areaOverride).map(([k, v]) => [Number(k), v]));
    console.log(`[gtfs-static] using cached classification (${Object.keys(trainTripMap).length} trips, ${railAreaModeOverride.size} station areas)`);
    applyRailOverrides();
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

    const routeClassification = new Map(); // route_id -> 'pendeltag' | 'roslagsbanan' | other modes
    routes.forEach(r => {
      const kind = classifyRoute(r);
      if (kind) routeClassification.set(r.route_id, kind);
    });
    console.log(`[gtfs-static] ${routeClassification.size} classified routes found in routes.txt`);

    const map = {};
    trips.forEach(t => {
      const kind = routeClassification.get(t.route_id);
      if (kind) map[t.trip_id] = kind;
    });
    console.log(`[gtfs-static] ${Object.keys(map).length} trips mapped`);

    trainTripMap = map;

    // Now derive the station-level override by joining stop_times.txt
    // against just the pendeltag/roslagsbanan trip_ids from the map above.
    const stopTimesCsv = await zip.file('stop_times.txt').async('string');
    await buildRailAreaOverrides(stopTimesCsv, map);

    await writeTrainTripCache(map, railAreaModeOverride);
    applyRailOverrides();
  } catch (err) {
    console.error('[gtfs-static] failed to load/parse static GTFS — vehicles will show as "other", station rail/tram split may be imprecise', err);
  }
}

// Joins stop_times.txt (trip_id -> stop_id, one row per stop visited) against
// the already-classified train trips, then converts each GTFS stop_id to
// its SL stop_area id (via gidToAreaId, built in loadStopPoints()) so it can
// override station rendering. Only look at pendeltag/roslagsbanan trips —
// stop_times.txt covers the whole network and is large, so filtering during
// the parse step keeps memory use down.
async function buildRailAreaOverrides(stopTimesCsv, tripMap) {
  await loadStopPoints(); // ensures gidToAreaId is populated
  if (!gidToAreaId.size) {
    console.warn('[gtfs-static] gidToAreaId empty — station override will be skipped');
    return;
  }

  const relevantTripIds = new Set(
    Object.entries(tripMap).filter(([, mode]) => mode === 'pendeltag' || mode === 'roslagsbanan').map(([id]) => id)
  );
  console.log(`[gtfs-static] joining stop_times.txt against ${relevantTripIds.size} pendeltag/roslagsbanan trips…`);

  const override = new Map();
  let matchedRows = 0;
  await new Promise((resolve) => {
    Papa.parse(stopTimesCsv, {
      header: true,
      skipEmptyLines: true,
      step: (row) => {
        const r = row.data;
        if (!relevantTripIds.has(r.trip_id)) return;
        const areaId = gidToAreaId.get(String(r.stop_id));
        if (areaId == null) return;
        override.set(areaId, tripMap[r.trip_id]);
        matchedRows++;
      },
      complete: resolve,
    });
  });

  railAreaModeOverride = override;
  console.log(`[gtfs-static] derived ${override.size} station area overrides from ${matchedRows} matched stop_times rows`);
}

// Re-styles already-rendered station markers once the authoritative
// GTFS-derived classification arrives (which can take a few seconds after
// boot, since it requires downloading and parsing the full static feed).
function applyRailOverrides() {
  let updated = 0;
  for (const [siteId, entry] of stationMarkers) {
    const { site, mode: currentMode } = entry;
    let overrideMode = null;
    for (const areaId of site.stop_areas || []) {
      if (railAreaModeOverride.has(areaId)) { overrideMode = railAreaModeOverride.get(areaId); break; }
    }
    if (overrideMode && overrideMode !== currentMode) {
      map.removeLayer(entry.marker);
      addStationMarker(site, overrideMode); // recreates the marker + overwrites this Map entry
      updated++;
    }
  }
  if (updated) {
    console.log(`[stations] corrected ${updated} station markers using GTFS-derived classification`);
    forceViewResync();
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

async function readTrainTripCache() {
  try {
    const cached = await idbGet(TRAIN_TRIPS_CACHE_KEY);
    if (!cached) return null;
    const { fetchedAt, tripMap, areaOverride } = cached;
    if (Date.now() - fetchedAt > TRAIN_TRIPS_CACHE_MAX_AGE_MS) return null;
    if (!tripMap || !areaOverride) return null; // stale shape from an older cache version
    return { tripMap, areaOverride };
  } catch (err) {
    console.warn('[gtfs-static] could not read cached classification', err);
    return null;
  }
}

async function writeTrainTripCache(tripMap, areaOverrideMap) {
  try {
    const areaOverride = Object.fromEntries(areaOverrideMap);
    await idbSet(TRAIN_TRIPS_CACHE_KEY, { fetchedAt: Date.now(), tripMap, areaOverride });
  } catch (err) {
    console.warn('[gtfs-static] could not cache classification', err);
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
  if (!vehiclesVisible) {
    for (const marker of vehicleMarkers.values()) map.removeLayer(marker);
    vehicleMarkers.clear();
    return;
  }

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
    const bearing = v.position.bearing != null ? v.position.bearing : 0;

    // Recreated fresh every poll rather than repositioned via setLatLng —
    // confirmed live that stations (created once, never setLatLng'd) stay
    // correctly positioned at every zoom level while setLatLng-updated
    // vehicle markers drift. Recreating trades a little extra work every
    // 15s for using the same marker-creation path that's proven reliable.
    const existing = vehicleMarkers.get(id);
    if (existing) map.removeLayer(existing);

    const icon = L.divIcon({
      className: `train-marker-wrap train-${kind}`,
      html: `<div class="train-marker-pulse"></div><div class="train-marker-dot" style="transform:rotate(${bearing}deg)"></div>`,
      iconSize: [22, 22],
      iconAnchor: [11, 11],
    });
    const marker = L.marker(latlng, { icon, keyboard: false }).addTo(map);
    vehicleMarkers.set(id, marker);

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

// ==========================================================================
// Journey planner (ResRobot: address/stop -> address/stop, with walking legs)
// ==========================================================================
function wireJourneyPanel() {
  const toggle = document.getElementById('journey-toggle');
  const panel = document.getElementById('journey-panel');
  const closeBtn = document.getElementById('journey-close');

  toggle.addEventListener('click', () => {
    const opening = !panel.classList.contains('open');
    panel.classList.toggle('open', opening);
    panel.setAttribute('aria-hidden', String(!opening));
    toggle.classList.toggle('active', opening);
  });
  closeBtn.addEventListener('click', () => {
    panel.classList.remove('open');
    panel.setAttribute('aria-hidden', 'true');
    toggle.classList.remove('active');
  });

  wireJourneyAutocomplete('from');
  wireJourneyAutocomplete('to');

  document.getElementById('journey-swap').addEventListener('click', () => {
    [journeyFrom, journeyTo] = [journeyTo, journeyFrom];
    document.getElementById('journey-from').value = journeyFrom ? journeyFrom.label : '';
    document.getElementById('journey-to').value = journeyTo ? journeyTo.label : '';
  });

  document.getElementById('journey-use-location').addEventListener('click', useCurrentLocationAsFrom);

  document.getElementById('journey-form').addEventListener('submit', (e) => {
    e.preventDefault();
    searchTrips();
  });
}

function useCurrentLocationAsFrom() {
  const btn = document.getElementById('journey-use-location');
  if (!navigator.geolocation) {
    console.warn('[journey] geolocation not available in this browser');
    return;
  }
  btn.classList.add('active');
  navigator.geolocation.getCurrentPosition(
    (pos) => {
      journeyFrom = {
        label: 'Min plats',
        lat: pos.coords.latitude,
        lon: pos.coords.longitude,
      };
      document.getElementById('journey-from').value = journeyFrom.label;
    },
    (err) => {
      console.error('[journey] geolocation failed', err);
      btn.classList.remove('active');
      alert('Kunde inte hämta din plats. Kontrollera platsbehörighet i webbläsaren.');
    },
    { enableHighAccuracy: true, timeout: 10000 }
  );
}

// Debounced address/stop autocomplete against ResRobot's location.name
// (type=SA — stops AND addresses combined, confirmed in the OpenAPI spec).
function wireJourneyAutocomplete(which) {
  const input = document.getElementById(`journey-${which}`);
  const list = document.getElementById(`journey-${which}-suggestions`);
  let debounceTimer = null;

  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    const query = input.value.trim();
    if (which === 'from') journeyFrom = null; // typing invalidates a previous geolocation/pick
    else journeyTo = null;

    if (query.length < 2) {
      list.hidden = true;
      return;
    }
    if (!resrobotApiKey) {
      list.hidden = true;
      return;
    }
    debounceTimer = setTimeout(() => fetchSuggestions(query, list, which, input), 300);
  });

  document.addEventListener('click', (e) => {
    if (!list.contains(e.target) && e.target !== input) list.hidden = true;
  });
}

async function fetchSuggestions(query, list, which, input) {
  try {
    const res = await fetch(RESROBOT_LOCATION_URL(query, resrobotApiKey));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    window.DEBUG_LAST_LOCATION_SEARCH = data;

    // Confirmed live: each entry is EITHER a StopLocation (real transit
    // stop) OR a CoordLocation with type "ADR" (address). The earlier
    // version only handled StopLocation and silently dropped every
    // address — that was the actual bug, not a ResRobot limitation.
    const entries = (data.stopLocationOrCoordLocation || [])
      .map(e => {
        if (e.StopLocation) return { ...e.StopLocation, kind: 'stop' };
        if (e.CoordLocation) return { ...e.CoordLocation, kind: 'address' };
        return null;
      })
      .filter(Boolean);

    list.innerHTML = '';
    if (!entries.length) {
      list.hidden = true;
      return;
    }
    entries.slice(0, 8).forEach(entry => {
      const li = document.createElement('li');
      const isStop = entry.kind === 'stop';
      li.innerHTML = `<span class="suggestion-type">${isStop ? 'Station' : 'Adress'}</span><span>${escapeHtml(entry.name)}</span>`;
      li.addEventListener('click', () => {
        const picked = { label: entry.name, lat: entry.lat, lon: entry.lon, extId: isStop ? entry.extId : null };
        if (which === 'from') journeyFrom = picked; else journeyTo = picked;
        input.value = entry.name;
        list.hidden = true;
      });
      list.appendChild(li);
    });
    list.hidden = false;
  } catch (err) {
    console.error('[journey] address/stop lookup failed', err);
    list.hidden = true;
  }
}

async function searchTrips() {
  const results = document.getElementById('journey-results');
  if (!resrobotApiKey) {
    results.innerHTML = '<p class="journey-status-msg">Ingen ResRobot-nyckel angiven — lägg till en i inställningarna för att söka resor.</p>';
    return;
  }
  if (!journeyFrom || !journeyTo) {
    results.innerHTML = '<p class="journey-status-msg">Välj både en start- och slutpunkt från förslagslistan (eller använd "min plats").</p>';
    return;
  }

  results.innerHTML = '<p class="journey-status-msg">Söker resor…</p>';

  // Prefer the stop extId when we have one (more precise for transit legs);
  // fall back to raw coordinates for addresses/current-location, with
  // walking legs enabled so ResRobot can route on foot to/from transit —
  // this is exactly what originWalk/destWalk are documented for.
  const params = {};
  if (journeyFrom.extId) params.originId = journeyFrom.extId;
  else {
    params.originCoordLat = journeyFrom.lat;
    params.originCoordLong = journeyFrom.lon;
    params.originWalk = '1,0,1500';
  }
  if (journeyTo.extId) params.destId = journeyTo.extId;
  else {
    params.destCoordLat = journeyTo.lat;
    params.destCoordLong = journeyTo.lon;
    params.destWalk = '1,0,1500';
  }

  try {
    const res = await fetch(RESROBOT_TRIP_URL(params, resrobotApiKey));
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status} ${body}`.trim());
    }
    const data = await res.json();
    window.DEBUG_LAST_TRIPS = data; // inspect: window.DEBUG_LAST_TRIPS

    if (data.errorCode) throw new Error(`${data.errorCode}: ${data.errorText || ''}`);

    const trips = data.Trip || [];
    if (!trips.length) {
      results.innerHTML = '<p class="journey-status-msg">Inga resor hittades för den här sträckan.</p>';
      return;
    }
    results.innerHTML = '';
    trips.forEach(trip => results.appendChild(renderTripCard(trip)));
  } catch (err) {
    console.error('[journey] trip search failed', err);
    results.innerHTML = `<p class="journey-status-msg">Kunde inte hämta resor (se konsolen). ${escapeHtml(err.message || '')}</p>`;
  }
}

function renderTripCard(trip) {
  const card = document.createElement('div');
  card.className = 'trip-card';

  const legs = (trip.LegList && trip.LegList.Leg) || [];
  const depTime = legs[0] && legs[0].Origin && legs[0].Origin.time;
  const arrTime = legs[legs.length - 1] && legs[legs.length - 1].Destination && legs[legs.length - 1].Destination.time;
  const durationLabel = formatIsoDuration(trip.duration);

  const summary = document.createElement('div');
  summary.className = 'trip-summary';
  summary.innerHTML = `
    <span class="trip-times">${formatClock(depTime)} → ${formatClock(arrTime)}</span>
    <span class="trip-duration">${durationLabel}</span>
  `;
  card.appendChild(summary);

  const legsRow = document.createElement('div');
  legsRow.className = 'trip-legs';
  legs.forEach((leg, i) => {
    if (i > 0) {
      const arrow = document.createElement('span');
      arrow.className = 'trip-leg-arrow';
      arrow.textContent = '→';
      legsRow.appendChild(arrow);
    }
    legsRow.appendChild(renderLegChip(leg));
  });
  card.appendChild(legsRow);

  return card;
}

function renderLegChip(leg) {
  const chip = document.createElement('span');
  if (leg.type === 'WALK' || leg.type === 'TRSF') {
    chip.className = 'trip-leg trip-leg-walk';
    chip.textContent = leg.dist ? `Gå ${leg.dist} m` : 'Gå';
    return chip;
  }
  const catCode = leg.Product && leg.Product[0] && leg.Product[0].catCode;
  const mode = CAT_CODE_TO_MODE[catCode] || 'pendeltag';
  const lineLabel = (leg.Product && leg.Product[0] && (leg.Product[0].displayNumber || leg.Product[0].line)) || leg.name || '?';

  chip.className = 'trip-leg';
  chip.innerHTML = `<span class="trip-leg-dot dot-${mode}"></span><span>${escapeHtml(lineLabel)}</span>`;
  return chip;
}

function formatClock(hhmmss) {
  if (!hhmmss) return '—';
  return hhmmss.slice(0, 5);
}

// ResRobot durations are ISO 8601 ("PT14H47M", "PT10M") — parse the pieces
// we expect (hours/minutes) rather than pulling in a full ISO 8601 library.
function formatIsoDuration(iso) {
  if (!iso) return '';
  const match = /^PT(?:(\d+)H)?(?:(\d+)M)?/.exec(iso);
  if (!match) return iso;
  const h = match[1] ? `${match[1]}h ` : '';
  const m = match[2] ? `${match[2]}min` : '';
  return (h + m).trim() || iso;
}
