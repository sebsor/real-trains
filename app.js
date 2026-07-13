/* ==========================================================================
   Spårläge — live SL train map (Pendeltåg + Roslagsbanan)

   Data sources:
   - Stations + departures: SL Transport API (transport.integration.sl.se)
     JSON, no key required.
   - Live vehicle GPS: Trafiklab GTFS-RT VehiclePositions feed (protobuf),
     requires a Trafiklab API key (stored in localStorage, client-side —
     fine for a personal hobby project, not meant for public distribution
     since the key is visible in the browser's network tab).

   Known unknowns, flagged rather than silently assumed:
   1. Whether opendata.samtrafiken.se sends CORS headers for browser fetch.
      If it doesn't, VEHICLES_STATUS will show "error" and the console will
      log the exact failure — see fetchVehiclePositions().
   2. The exact filter to isolate Pendeltåg + Roslagsbanan out of SL's full
      vehicle feed (which also carries metro/bus/tram/boat). See
      classifyVehicle() — it's isolated on purpose so it's easy to tune
      once you can see real data in the console (window.DEBUG_LAST_VEHICLES).
   3. The exact shape of /v1/sites response for filtering to rail stations
      only. See isTrainSite() — same debug approach.
   ========================================================================== */

const SL_SITES_URL = 'https://transport.integration.sl.se/v1/sites?expand=true';
const SL_LINES_URL = 'https://transport.integration.sl.se/v1/lines?transport_authority_id=1';
const SL_DEPARTURES_URL = (siteId) => `https://transport.integration.sl.se/v1/sites/${siteId}/departures`;
const GTFS_RT_URL = (key) => `https://opendata.samtrafiken.se/gtfs-rt/sl/VehiclePositions.pb?key=${key}`;

const VEHICLE_POLL_MS = 15000; // GTFS-RT vehicle positions update ~every 2s server-side, but poll politely
const STOCKHOLM_CENTER = [59.334, 18.06];

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
let apiKey = localStorage.getItem('trafiklab_api_key') || '';
let trainLineIds = new Set();   // populated from /v1/lines, used by classifyVehicle()
let stationMarkers = new Map(); // siteId -> Leaflet marker
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
  loadTrainLines();   // used to classify vehicles once positions come in

  if (apiKey) {
    startVehiclePolling();
  } else {
    document.getElementById('setup-overlay').removeAttribute('hidden');
    setStatus('warn', 'Ingen API-nyckel');
  }

  document.getElementById('debug-toggle').addEventListener('change', () => {
    // Force an immediate re-render with the new debug mode
    if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
  });

  registerServiceWorker();
});

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
  input.value = apiKey;

  document.getElementById('setup-save').addEventListener('click', () => {
    const val = input.value.trim();
    if (!val) return;
    apiKey = val;
    localStorage.setItem('trafiklab_api_key', apiKey);
    overlay.setAttribute('hidden', '');
    startVehiclePolling();
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
    const res = await fetch(SL_SITES_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const sites = await res.json();

    window.DEBUG_LAST_SITES = sites; // inspect in console: window.DEBUG_LAST_SITES[0]
    console.log(`[stations] fetched ${sites.length} SL sites total`);

    const trainSites = sites.filter(isTrainSite);
    console.log(`[stations] ${trainSites.length} classified as train sites — ` +
      `if this looks wrong (0 or way too many), inspect window.DEBUG_LAST_SITES ` +
      `and adjust isTrainSite() in app.js`);

    trainSites.forEach(addStationMarker);
  } catch (err) {
    console.error('[stations] failed to load', err);
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

function addStationMarker(site) {
  const latlng = getSiteLatLng(site);
  if (!latlng) {
    console.warn('[stations] no coordinates found for site, skipping', site);
    return;
  }
  const icon = L.divIcon({ className: 'station-marker', iconSize: [11, 11] });
  const marker = L.marker(latlng, { icon, keyboard: false })
    .addTo(map)
    .bindTooltip(site.name || 'Station', { direction: 'top', offset: [0, -6] });

  marker.on('click', () => openBoard(site));
  stationMarkers.set(site.id, marker);
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

    // Departures are grouped by transport mode; keep only train-like groups.
    const departures = extractTrainDepartures(data);

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

// Isolated for the same reason as isTrainSite() — adjust against
// window.DEBUG_LAST_DEPARTURES if the shape differs from what's assumed here.
function extractTrainDepartures(data) {
  // Most likely shape, per SL's documented departures endpoint: an object
  // with a "departures" array, each item carrying a "line" with transport_mode.
  const list = Array.isArray(data) ? data : (data.departures || data.train || []);
  if (!Array.isArray(list)) {
    console.warn('[departures] unexpected response shape', data);
    return [];
  }
  return list
    .filter(dep => {
      const mode = (dep.line && dep.line.transport_mode) || dep.transport_mode || '';
      return mode.toString().toUpperCase().includes('TRAIN') || list === data.train;
    })
    .map(dep => ({
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
// Train line lookup (used to classify vehicles by mode)
// ==========================================================================
async function loadTrainLines() {
  try {
    const res = await fetch(SL_LINES_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    window.DEBUG_LAST_LINES = data;

    // Response groups lines by mode (see SL Transport API docs example).
    // Train-like groups may appear under different keys depending on the
    // live schema — collect anything whose entries say TRAIN.
    const allLines = Array.isArray(data) ? data.flatMap(obj => Object.values(obj).flat()) : [];
    allLines
      .filter(l => (l.transport_mode || '').toUpperCase().includes('TRAIN'))
      .forEach(l => {
        if (l.id != null) trainLineIds.add(String(l.id));
        if (l.gid != null) trainLineIds.add(String(l.gid));
      });

    console.log(`[lines] identified ${trainLineIds.size} train line ids for vehicle filtering`);
  } catch (err) {
    console.error('[lines] failed to load, vehicle classification will fall back to heuristics', err);
  }
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

// Isolated on purpose — see the header comment. Adjust against
// window.DEBUG_LAST_VEHICLES once you can see real route_id / label values.
function classifyVehicle(v) {
  const routeId = v.trip && v.trip.routeId;
  if (routeId && trainLineIds.has(String(routeId))) {
    // Can't reliably distinguish Pendeltåg vs Roslagsbanan by route_id alone
    // without a confirmed mapping — falls back to a label heuristic.
    const label = ((v.vehicle && v.vehicle.label) || '').toLowerCase();
    if (label.includes('ros')) return 'roslagsbanan';
    return 'pendeltag';
  }
  return 'other';
}

function renderVehicles(vehicles) {
  const showAll = document.getElementById('debug-toggle').checked;
  const seen = new Set();

  vehicles.forEach(v => {
    if (!v.position) return;
    const kind = classifyVehicle(v);
    if (!showAll && kind === 'other') return;

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
