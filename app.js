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
const SERVICE_ALERTS_URL = (key) => `https://opendata.samtrafiken.se/gtfs-rt/sl/ServiceAlerts.pb?key=${key}`;
const ALERTS_POLL_MS = 60000; // alerts change far less often than positions — poll gently
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

const VEHICLE_POLL_MS = 8000; // SL's feed itself updates ~every 2s server-side; 8s keeps monthly quota comfortable (~7.5 calls/min, Bronze tier caps at 30k/month) while feeling meaningfully smoother than 15s
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
let vehiclesVisible = false; // master toggle, independent of per-mode filters — off by default to save API quota until opted in

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
  optional Alert alert = 5;
}
message Alert {
  repeated TimeRange active_period = 1;
  repeated EntitySelector informed_entity = 5;
  optional TranslatedString header_text = 10;
  optional TranslatedString description_text = 11;
}
message TimeRange {
  optional uint64 start = 1;
  optional uint64 end = 2;
}
message EntitySelector {
  optional string agency_id = 1;
  optional string route_id = 2;
  optional TripDescriptor trip = 4;
  optional string stop_id = 5;
}
message TranslatedString {
  message Translation {
    required string text = 1;
    optional string language = 2;
  }
  repeated Translation translation = 1;
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
let routeIdToMode = {};         // route_id -> mode, from static GTFS — used to group alerts by vehicle type
let loadTrainTripClassificationPromise = null; // shared, so map mode doesn't re-trigger a second fetch of the same data
let railAreaModeOverride = new Map(); // stop_area id -> 'pendeltag' | 'roslagsbanan', authoritative (from stop_times.txt join)
let stopPointsCache = null;     // cached /v1/stop-points response, shared between station classification and the override join
let gidToAreaId = new Map();    // stop-point gid -> stop_area id, used to join GTFS stop_id against SL's site model
let stationMarkers = new Map(); // siteId -> { marker, mode, site }
let vehicleMarkers = new Map(); // vehicleId -> Leaflet marker
let lastVehiclePosition = new Map(); // vehicleId -> { lat, lon } from the previous poll, used to derive heading when SL doesn't report bearing
const MIN_MOVEMENT_FOR_DERIVED_BEARING_M = 8; // below this, GPS jitter dominates and a derived direction would be unreliable
let FeedMessageType = null;     // protobufjs decoded type, set once on init
let vehiclePollTimer = null;

// ==========================================================================
// Boot
// ==========================================================================
document.addEventListener('DOMContentLoaded', () => {
  checkStorageAvailable();
  wireSetupModal();
  wireJourneyPanel();
  wireStartScreen();
  wireAlertsPanel();
  registerServiceWorker();

  // Shown once, up front, only if at least one key hasn't been set yet —
  // not re-prompted every time a flow is entered. The overlay has no
  // `hidden` attribute in the raw HTML (visible by default), so the
  // "keys exist" case must explicitly hide it too, not just skip showing it.
  const setupOverlay = document.getElementById('setup-overlay');
  if (!apiKey && !staticApiKey && !resrobotApiKey) {
    setupOverlay.removeAttribute('hidden');
  } else {
    setupOverlay.setAttribute('hidden', '');
  }

  // Trafikläge doesn't depend on which flow (map or journey) is chosen —
  // both buttons exist in the DOM from the start, so start polling here
  // rather than duplicating this in both startMapMode() and startJourneyMode().
  if (apiKey) startAlertsPolling();

  // routeIdToMode (used to group alerts by vehicle type) comes from this
  // same static GTFS parse — needed even in journey-only mode, where
  // startMapMode() (which used to be the only place this ran) never fires.
  if (staticApiKey) loadTrainTripClassificationPromise = loadTrainTripClassification();
});

// Confirms whether localStorage actually persists in this browsing
// context — private/incognito mode in several mobile browsers blocks or
// throws on writes, which would otherwise fail completely silently and
// look identical to "the app just doesn't remember my keys".
function checkStorageAvailable() {
  try {
    localStorage.setItem('__storage_test__', '1');
    localStorage.removeItem('__storage_test__');
    console.log('[storage] localStorage is writable — keys should persist across reloads');
  } catch (err) {
    console.error('[storage] localStorage is NOT writable — keys will NOT persist across reloads. ' +
      'This is almost always private/incognito browsing mode blocking storage writes.', err);
  }
}

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
  setLoadingProgress(5, 'Startar kartan…');

  initMap();
  wireBoard();

  loadLegendPrefs(); // must run before loadStations() — activeModes affects which markers get created
  syncLegendCheckboxesToState();
  cleanupStaleLocalStorageKeys();
  setLoadingProgress(10, 'Förbereder…');

  if (apiKey && vehiclesVisible) {
    startVehiclePolling();
  } else if (!apiKey) {
    setStatus('warn', 'Ingen API-nyckel');
  } else {
    setStatus('', 'Fordon avstängda');
  }
  if (!staticApiKey) {
    console.warn('[gtfs-static] no static-data key set — vehicles will show as "other" until one is added');
  } else {
    // Re-render once classification (kicked off at boot, or started here if
    // the key wasn't available yet at boot time) resolves, in case vehicles
    // started polling before it finished. Reuses the same in-flight promise
    // rather than triggering a second fetch of the static GTFS zip.
    if (!loadTrainTripClassificationPromise) loadTrainTripClassificationPromise = loadTrainTripClassification();
    loadTrainTripClassificationPromise.then(() => {
      if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
    });
  }

  document.getElementById('vehicles-toggle').addEventListener('change', (e) => {
    vehiclesVisible = e.target.checked;
    saveLegendPrefs();
    if (vehiclesVisible) {
      if (apiKey) startVehiclePolling(); // resumes fetching, not just rendering
    } else {
      stopVehiclePolling(); // actually stops fetching — this is what saves quota
      renderVehicles([]); // clear any markers already on the map immediately
    }
  });

  document.getElementById('debug-toggle').addEventListener('change', () => {
    // Force an immediate re-render with the new debug mode
    if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
  });

  map.on('zoomend', refreshAllStationVisibility);
  wireModeCheckboxes();
  wireStationSearch();

  await loadStations(); // loading overlay stays up until the initial station markers are placed
  setLoadingProgress(100, 'Klart!');
  setTimeout(() => document.getElementById('loading-overlay').setAttribute('hidden', ''), 250);
}

// Drives the progress bar + train on the loading overlay. Called from the
// actual loading steps below (stop-points fetch, sites fetch, marker
// placement) so the bar reflects real progress rather than a fake timer.
function setLoadingProgress(percent, text) {
  const fill = document.getElementById('loading-fill');
  const train = document.getElementById('loading-train');
  const label = document.getElementById('loading-text');
  const clampedForTrain = Math.min(96, Math.max(4, percent));
  if (fill) fill.style.width = `${percent}%`;
  if (train) train.style.left = `${clampedForTrain}%`;
  if (label && text) label.textContent = text;
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
}

// Generic clear (✕) button for any wrapped text input — shows only when
// the field has content, clears it, refocuses, and dispatches a real
// 'input' event so whatever autocomplete/filter logic is already listening
// reacts naturally (clearing suggestions, resetting journeyFrom/To, etc.)
// rather than needing bespoke clear logic per field.
function wireClearableInput(inputId) {
  const input = document.getElementById(inputId);
  const btn = document.querySelector(`.input-clear-btn[data-target="${inputId}"]`);
  if (!input || !btn) return;

  input.addEventListener('input', () => {
    btn.hidden = input.value.length === 0;
  });
  btn.hidden = input.value.length === 0;

  btn.addEventListener('click', () => {
    input.value = '';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.focus();
    btn.hidden = true;
  });
}

// Narrow version for cases (like the from/to swap) where the value is set
// programmatically and journeyFrom/To are already correct — dispatching a
// full 'input' event there would trigger the autocomplete listener, which
// treats any input event as "the person is typing" and nulls them out.
function syncClearButtonVisibility(inputId) {
  const input = document.getElementById(inputId);
  const btn = document.querySelector(`.input-clear-btn[data-target="${inputId}"]`);
  if (input && btn) btn.hidden = input.value.length === 0;
}

function wireStationSearch() {
  const toggle = document.getElementById('station-search-toggle');
  const panel = document.getElementById('station-search-panel');
  const input = document.getElementById('station-search-input');
  const results = document.getElementById('station-search-results');
  const nearestBtn = document.getElementById('nearest-station-btn');
  wireClearableInput('station-search-input');

  toggle.addEventListener('click', () => {
    const opening = panel.hasAttribute('hidden');
    if (opening) {
      panel.removeAttribute('hidden');
      toggle.classList.add('active');
      input.value = '';
      results.innerHTML = '';
      input.focus();
    } else {
      panel.setAttribute('hidden', '');
      toggle.classList.remove('active');
    }
  });

  document.addEventListener('click', (e) => {
    if (!panel.hasAttribute('hidden') && !panel.contains(e.target) && e.target !== toggle) {
      panel.setAttribute('hidden', '');
      toggle.classList.remove('active');
    }
  });

  input.addEventListener('input', () => {
    const query = input.value.trim().toLowerCase();
    results.innerHTML = '';
    if (query.length < 1) return;

    const matches = [...stationMarkers.values()]
      .filter(({ site }) => (site.name || '').toLowerCase().includes(query))
      .slice(0, 15);

    if (!matches.length) {
      results.innerHTML = '<li id="station-search-empty">Inga stationer hittades</li>';
      return;
    }
    matches.forEach(({ site, mode }) => renderStationResultItem(results, site, mode));
  });

  nearestBtn.addEventListener('click', () => {
    if (!navigator.geolocation) {
      alert('Geolocation stöds inte i den här webbläsaren.');
      return;
    }
    results.innerHTML = '<li id="station-search-empty">Hämtar din plats…</li>';
    input.value = '';
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const { latitude, longitude } = pos.coords;
        const withDistance = [...stationMarkers.values()].map(({ site, mode }) => ({
          site, mode, dist: haversineMeters(latitude, longitude, site.lat, site.lon),
        }));
        withDistance.sort((a, b) => a.dist - b.dist);

        results.innerHTML = '';
        withDistance.slice(0, 10).forEach(({ site, mode, dist }) =>
          renderStationResultItem(results, site, mode, dist));
      },
      (err) => {
        console.error('[stations] geolocation failed', err);
        results.innerHTML = '<li id="station-search-empty">Kunde inte hämta din plats.</li>';
      },
      { enableHighAccuracy: true, timeout: 10000 }
    );
  });
}

function renderStationResultItem(container, site, mode, distanceMeters) {
  const li = document.createElement('li');
  const distLabel = distanceMeters != null
    ? `<span class="station-result-dist">${distanceMeters < 1000 ? `${Math.round(distanceMeters)} m` : `${(distanceMeters / 1000).toFixed(1)} km`}</span>`
    : '';
  li.innerHTML = `<span class="dot dot-${mode}"></span><span>${escapeHtml(site.name || 'Station')}</span>${distLabel}`;
  li.addEventListener('click', () => {
    const latlng = getSiteLatLng(site);
    if (latlng) map.setView(latlng, 15);
    openBoard(site);
    document.getElementById('station-search-panel').setAttribute('hidden', '');
    document.getElementById('station-search-toggle').classList.remove('active');
  });
  container.appendChild(li);
}

// Straight-line distance in meters between two lat/lon points (haversine).
function haversineMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = deg => deg * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// True compass bearing (0-360°, 0=north) from point 1 to point 2 — the
// standard spherical forward-azimuth formula, not a naive flat atan2 on
// raw lat/lon (which would be skewed since a degree of longitude covers
// less physical distance than a degree of latitude, more so the further
// from the equator).
function bearingBetween(lat1, lon1, lat2, lon2) {
  const toRad = deg => deg * Math.PI / 180;
  const phi1 = toRad(lat1), phi2 = toRad(lat2);
  const dLon = toRad(lon2 - lon1);
  const y = Math.sin(dLon) * Math.cos(phi2);
  const x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(dLon);
  return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
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
  vehiclesVisible = false;
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
  // Vehicles are recreated fresh every 15s poll anyway (see renderVehicles),
  // which is the mechanism actually proven to position them correctly —
  // this just closes the up-to-15s gap after a zoom/pan by re-running that
  // same recreation immediately, rather than waiting for the next poll.
  map.on('zoomend moveend', () => {
    if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
  });

  setTimeout(() => map.invalidateSize(), 200);
  window.addEventListener('resize', () => map.invalidateSize());
  window.addEventListener('orientationchange', () => map.invalidateSize());
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

    let persistFailed = false;
    const persist = (key, value) => {
      try {
        localStorage.setItem(key, value);
      } catch (err) {
        console.error(`[setup] failed to save ${key} to localStorage`, err);
        persistFailed = true;
      }
    };

    if (val) {
      apiKey = val;
      persist('trafiklab_api_key', apiKey);
      startVehiclePolling();
    }
    if (staticVal) {
      staticApiKey = staticVal;
      persist('trafiklab_static_api_key', staticApiKey);
      loadTrainTripClassificationPromise = loadTrainTripClassification();
      loadTrainTripClassificationPromise.then(() => {
        if (window.DEBUG_LAST_VEHICLES) renderVehicles(window.DEBUG_LAST_VEHICLES);
      });
    }
    if (resrobotVal) {
      resrobotApiKey = resrobotVal;
      persist('trafiklab_resrobot_api_key', resrobotApiKey);
    }
    overlay.setAttribute('hidden', '');

    if (persistFailed) {
      // Keys still work for this session (the variables are set above) but
      // won't survive a reload — most commonly caused by private/incognito
      // browsing mode blocking or throwing on localStorage writes.
      alert('Nycklarna kunde inte sparas permanent (fungerar bara för denna session). Detta händer oftast i privat/inkognitoläge — prova ett vanligt webbläsarfönster om du vill slippa mata in dem varje gång.');
    }
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
    setLoadingProgress(15, 'Hämtar hållplatser…');
    const areaIdToMode = await loadAreaModeMap();

    setLoadingProgress(45, 'Hämtar stationer…');
    const res = await fetch(SL_SITES_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const sites = await res.json();

    window.DEBUG_LAST_SITES = sites; // inspect in console: window.DEBUG_LAST_SITES[0]
    console.log(`[stations] fetched ${sites.length} SL sites total`);

    setLoadingProgress(65, 'Placerar markörer…');
    let placed = 0;
    sites.forEach(site => {
      const mode = areaIdToMode ? resolveSiteMode(site, areaIdToMode) : (isTrainSite(site) ? 'rail' : null);
      if (!mode) return;
      addStationMarker(site, mode);
      placed++;
    });
    console.log(`[stations] placed ${placed} station markers across all modes`);
    setLoadingProgress(95, 'Nästan klart…');
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
  setLoadingProgress(20, 'Hämtar hållplatser…');
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

  // Avoid both slide-in panels being open at once — on narrow screens this
  // was causing horizontal overflow/scroll rather than a clean overlay.
  const journeyPanel = document.getElementById('journey-panel');
  if (journeyPanel && !journeyPanel.classList.contains('standalone')) {
    journeyPanel.classList.remove('open');
    journeyPanel.setAttribute('aria-hidden', 'true');
    document.getElementById('journey-toggle').classList.remove('active');
  }
}

function closeBoard() {
  const board = document.getElementById('board');
  board.classList.remove('open');
  board.setAttribute('aria-hidden', 'true');
  if (departureCountdownTimer) {
    clearInterval(departureCountdownTimer);
    departureCountdownTimer = null;
  }
}

let departureCountdownTimer = null;
let selectedDepartureRow = null;

async function loadDepartures(site) {
  const empty = document.getElementById('board-empty');
  const table = document.getElementById('board-table');
  const rows = document.getElementById('board-rows');
  empty.hidden = false;
  empty.textContent = 'Hämtar avgångar…';
  table.hidden = true;
  rows.innerHTML = '';
  selectedDepartureRow = null;
  if (departureCountdownTimer) clearInterval(departureCountdownTimer);

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

    // Live countdown while the board is open — re-derives each label from
    // the stored absolute time rather than just decrementing a number, so
    // it stays correct even if the tab was backgrounded for a while.
    departureCountdownTimer = setInterval(updateDepartureCountdowns, 15000);
  } catch (err) {
    console.error('[departures] failed', err);
    empty.textContent = 'Kunde inte hämta avgångar (se konsolen för detaljer).';
  }
}

function updateDepartureCountdowns() {
  document.querySelectorAll('#board-rows .dep-time[data-time]').forEach(el => {
    el.textContent = formatDepartureTime(el.dataset.time);
  });
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
    scheduledTime: dep.scheduled || (dep.departure && dep.departure.scheduled) || null,
    platform: (dep.stop_point && (dep.stop_point.designation || dep.stop_point.name)) || dep.platform || null,
    deviationTexts: Array.isArray(dep.deviations)
      ? dep.deviations.map(d => d.text || d.consequence || d.header).filter(Boolean)
      : [],
    get deviation() { return this.deviationTexts.length > 0; },
  }));
}

function renderDepartureRow(dep) {
  const tr = document.createElement('tr');
  tr.className = 'dep-row';
  const timeLabel = formatDepartureTime(dep.time);
  tr.innerHTML = `
    <td><span class="dep-line">${escapeHtml(dep.line)}</span></td>
    <td>${escapeHtml(dep.destination)}</td>
    <td class="dep-time ${dep.deviation ? 'deviation' : ''}" data-time="${escapeHtml(dep.time || '')}">${timeLabel}</td>
  `;

  const detailTr = document.createElement('tr');
  detailTr.className = 'dep-detail-row';
  detailTr.hidden = true;
  const detailParts = [];
  if (dep.platform) detailParts.push(`<div>Läge/plattform: <strong>${escapeHtml(dep.platform)}</strong></div>`);
  if (dep.scheduledTime && dep.scheduledTime !== dep.time) {
    detailParts.push(`<div>Tidtabell: ${formatDepartureTime(dep.scheduledTime)} (realtid: ${timeLabel})</div>`);
  }
  if (dep.deviationTexts.length) {
    detailParts.push(...dep.deviationTexts.map(t => `<div class="dep-deviation-text">⚠ ${escapeHtml(t)}</div>`));
  }
  if (!detailParts.length) detailParts.push('<div class="dep-detail-empty">Inga fler detaljer</div>');
  detailTr.innerHTML = `<td colspan="3"><div class="dep-detail">${detailParts.join('')}</div></td>`;

  tr.addEventListener('click', () => {
    // Selection highlight — separate from expand/collapse, and persists
    // even if the row is collapsed again (only reset by picking another
    // row or reloading the board), so it's clear which departure you're
    // tracking at a glance.
    if (selectedDepartureRow && selectedDepartureRow !== tr) {
      selectedDepartureRow.classList.remove('selected');
    }
    tr.classList.add('selected');
    selectedDepartureRow = tr;

    detailTr.hidden = !detailTr.hidden;
    tr.classList.toggle('expanded', !detailTr.hidden);
  });

  const frag = document.createDocumentFragment();
  frag.appendChild(tr);
  frag.appendChild(detailTr);
  return frag;
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
  if (!FeedMessageType) {
    const root = protobuf.parse(GTFS_RT_PROTO).root;
    FeedMessageType = root.lookupType('transit_realtime.FeedMessage');
  }

  fetchVehiclePositions();
  if (vehiclePollTimer) clearInterval(vehiclePollTimer);
  vehiclePollTimer = setInterval(fetchVehiclePositions, VEHICLE_POLL_MS);
}

// Actually halts the fetch timer — quota only gets saved by not polling at
// all, not just by hiding already-fetched markers.
function stopVehiclePolling() {
  if (vehiclePollTimer) {
    clearInterval(vehiclePollTimer);
    vehiclePollTimer = null;
  }
  setStatus('', 'Fordon pausade');
}

// ==========================================================================
// Trafikläge (Service Alerts) — separate GTFS-RT feed, same key as
// VehiclePositions. Not filtered by route/mode: SL's alert entities don't
// reliably carry a matching route_id we could join against our own
// classification, so this shows everything currently active rather than
// silently hiding alerts that might actually be relevant.
// ==========================================================================
let alertsPollTimer = null;
let activeAlerts = [];

function startAlertsPolling() {
  if (!FeedMessageType) {
    if (!window.protobuf) {
      console.error('[alerts] protobufjs missing, cannot decode alerts');
      return;
    }
    const root = protobuf.parse(GTFS_RT_PROTO).root;
    FeedMessageType = root.lookupType('transit_realtime.FeedMessage');
  }
  fetchServiceAlerts();
  if (alertsPollTimer) clearInterval(alertsPollTimer);
  alertsPollTimer = setInterval(fetchServiceAlerts, ALERTS_POLL_MS);
}

async function fetchServiceAlerts() {
  try {
    const res = await fetch(SERVICE_ALERTS_URL(apiKey));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const buf = new Uint8Array(await res.arrayBuffer());
    const message = FeedMessageType.decode(buf);
    const obj = FeedMessageType.toObject(message, { defaults: true });

    const nowSec = Date.now() / 1000;
    activeAlerts = (obj.entity || [])
      .map(e => e.alert)
      .filter(Boolean)
      .filter(a => {
        // No active_period entries means "always active" per the GTFS-RT spec.
        if (!a.activePeriod || !a.activePeriod.length) return true;
        return a.activePeriod.some(p =>
          (!p.start || Number(p.start) <= nowSec) && (!p.end || Number(p.end) >= nowSec));
      })
      .map(a => ({
        header: firstTranslation(a.headerText),
        description: firstTranslation(a.descriptionText),
        modes: resolveAlertModes(a.informedEntity),
      }))
      .filter(a => a.header);

    window.DEBUG_LAST_ALERTS = activeAlerts;
    renderAlertsBadge();
  } catch (err) {
    console.error('[alerts] fetch failed', err);
  }
}

function firstTranslation(translatedString) {
  const t = translatedString && translatedString.translation;
  if (!t || !t.length) return '';
  return (t.find(x => x.language === 'sv') || t[0]).text || '';
}

// Resolves which mode(s) an alert affects, via informed_entity.route_id
// joined against routeIdToMode (built during static GTFS parsing). An
// alert can list multiple informed entities (e.g. affects several routes);
// returns the set of distinct modes found. Empty set if nothing resolves —
// those alerts go in an "Övrigt" bucket rather than being silently hidden.
function resolveAlertModes(informedEntity) {
  if (!Array.isArray(informedEntity)) return [];
  const modes = new Set();
  informedEntity.forEach(e => {
    if (e.routeId && routeIdToMode[e.routeId]) modes.add(routeIdToMode[e.routeId]);
  });
  return [...modes];
}

function renderAlertsBadge() {
  const count = activeAlerts.length;
  document.querySelectorAll('.alerts-toggle-btn').forEach(btn => {
    btn.querySelector('.alerts-count').textContent = count > 0 ? count : '';
    btn.classList.toggle('has-alerts', count > 0);
  });
}

function wireAlertsPanel() {
  const toggles = document.querySelectorAll('.alerts-toggle-btn');
  const panel = document.getElementById('alerts-panel');

  toggles.forEach(toggle => {
    toggle.addEventListener('click', () => {
      const opening = panel.hasAttribute('hidden');
      if (opening) {
        renderAlertsList();
        panel.removeAttribute('hidden');
      } else {
        panel.setAttribute('hidden', '');
      }
    });
  });

  document.addEventListener('click', (e) => {
    const clickedToggle = [...toggles].some(t => t.contains(e.target));
    if (!panel.hasAttribute('hidden') && !panel.contains(e.target) && !clickedToggle) {
      panel.setAttribute('hidden', '');
    }
  });
}

function renderAlertsList() {
  const list = document.getElementById('alerts-list');
  list.innerHTML = '';
  if (!activeAlerts.length) {
    list.innerHTML = '<p class="alerts-empty">Inga aktiva trafikstörningar just nu.</p>';
    return;
  }

  // Group by mode — an alert with multiple affected modes appears in each
  // group it's relevant to. Alerts with no resolvable route_id/mode land
  // in "Övrigt" rather than being silently dropped.
  const groups = new Map(); // mode -> alert[]
  activeAlerts.forEach(alert => {
    const modes = alert.modes.length ? alert.modes : ['other'];
    modes.forEach(mode => {
      if (!groups.has(mode)) groups.set(mode, []);
      groups.get(mode).push(alert);
    });
  });

  const order = ['pendeltag', 'roslagsbanan', 'metro', 'tram', 'bus', 'boat', 'other'];
  order.filter(mode => groups.has(mode)).forEach(mode => {
    const alerts = groups.get(mode);
    const group = document.createElement('div');
    group.className = 'alert-group';

    const header = document.createElement('button');
    header.type = 'button';
    header.className = 'alert-group-header';
    const icon = mode === 'other' ? '❓' : (MODE_ICONS[mode] || '');
    const label = mode === 'other' ? 'Övrigt' : (MODE_LABELS_SV[mode] || mode);
    header.innerHTML = `
      <span>${icon} ${escapeHtml(label)}</span>
      <span class="alert-group-count">${alerts.length}</span>
      <span class="alert-group-arrow">▾</span>
    `;

    const body = document.createElement('div');
    body.className = 'alert-group-body';
    body.hidden = true;
    alerts.forEach(alert => {
      const item = document.createElement('div');
      item.className = 'alert-item';
      item.innerHTML = `
        <p class="alert-header">⚠ ${escapeHtml(alert.header)}</p>
        ${alert.description ? `<p class="alert-desc">${escapeHtml(alert.description)}</p>` : ''}
      `;
      body.appendChild(item);
    });

    header.addEventListener('click', () => {
      body.hidden = !body.hidden;
      header.classList.toggle('expanded', !body.hidden);
    });

    group.appendChild(header);
    group.appendChild(body);
    list.appendChild(group);
  });
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
    // No `defaults: true` here (unlike the alerts decode below) — with it,
    // protobufjs fills every absent optional field (bearing, speed, ids)
    // with its type default (0 for numbers), making "SL didn't report a
    // bearing" indistinguishable from "SL reported exactly 0°". Confirmed
    // live: 100% of active Roslagsbanan vehicles showed bearing:0, which
    // turned out to be this coercion hiding "no data" as "north", not 21
    // trains all genuinely facing north. Leaving defaults off lets absent
    // fields come through as real undefined instead.
    const obj = FeedMessageType.toObject(message);

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
    routeIdToMode = cached.routeMode || {};
    console.log(`[gtfs-static] using cached classification (${Object.keys(trainTripMap).length} trips, ${railAreaModeOverride.size} station areas, ${Object.keys(routeIdToMode).length} routes)`);
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
    routeIdToMode = Object.fromEntries(routeClassification);

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

    await writeTrainTripCache(map, railAreaModeOverride, routeIdToMode);
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
    const { fetchedAt, tripMap, areaOverride, routeMode } = cached;
    if (Date.now() - fetchedAt > TRAIN_TRIPS_CACHE_MAX_AGE_MS) return null;
    if (!tripMap || !areaOverride) return null; // stale shape from an older cache version
    return { tripMap, areaOverride, routeMode: routeMode || {} };
  } catch (err) {
    console.warn('[gtfs-static] could not read cached classification', err);
    return null;
  }
}

async function writeTrainTripCache(tripMap, areaOverrideMap, routeMode) {
  try {
    const areaOverride = Object.fromEntries(areaOverrideMap);
    await idbSet(TRAIN_TRIPS_CACHE_KEY, { fetchedAt: Date.now(), tripMap, areaOverride, routeMode });
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
    const lat = v.position.latitude, lon = v.position.longitude;
    const latlng = [lat, lon];

    // Prefer SL's own reported bearing when present. When it's not (the
    // common case for e.g. Roslagsbanan — confirmed live, not a rendering
    // bug), derive heading ourselves from the change in position since the
    // last poll — works for any mode regardless of what SL's feed reports,
    // since it only needs two GPS fixes and real movement between them. A
    // genuinely stationary vehicle correctly gets no direction at all
    // (a stopped vehicle doesn't have a meaningful heading to show) rather
    // than a stale or misleading one.
    let hasBearing = v.position.bearing != null;
    let bearingIsDerived = false;
    let bearing = hasBearing ? v.position.bearing : 0;
    if (!hasBearing) {
      const prev = lastVehiclePosition.get(id);
      if (prev) {
        const movedMeters = haversineMeters(prev.lat, prev.lon, lat, lon);
        if (movedMeters >= MIN_MOVEMENT_FOR_DERIVED_BEARING_M) {
          bearing = bearingBetween(prev.lat, prev.lon, lat, lon);
          hasBearing = true;
          bearingIsDerived = true;
        }
      }
    }
    lastVehiclePosition.set(id, { lat, lon });

    // Recreated fresh every poll rather than repositioned via setLatLng —
    // confirmed live that stations (created once, never setLatLng'd) stay
    // correctly positioned at every zoom level while setLatLng-updated
    // vehicle markers drift. Recreating trades a little extra work every
    // 15s for using the same marker-creation path that's proven reliable.
    const existing = vehicleMarkers.get(id);
    if (existing) map.removeLayer(existing);

    // Rotation uses mask-image with 16 pre-rotated shapes (see CSS) rather
    // than the transform/rotate property family, which didn't visually
    // apply when tested live on a Leaflet-positioned element. Vehicles
    // with no bearing at all (no real data AND not enough movement yet to
    // derive one — e.g. right after a page load, before a second fix
    // arrives) get a plain dot instead of an arrow.
    const dirBucket = Math.round(bearing / 22.5) % 16;
    const shapeClass = hasBearing ? `dir-${dirBucket}` : 'no-bearing';
    const icon = L.divIcon({
      className: `train-marker-wrap train-${kind} ${shapeClass}`,
      iconSize: [16, 16],
      iconAnchor: [8, 8],
    });
    const marker = L.marker(latlng, { icon, keyboard: false }).addTo(map);
    vehicleMarkers.set(id, marker);

    const label = (v.vehicle && v.vehicle.label) || id;
    const speedKmh = v.position.speed != null ? Math.round(v.position.speed * 3.6) : null;
    marker.bindPopup(`
      <p class="popup-title">${escapeHtml(label)}</p>
      <p class="popup-meta">${kind === 'other' ? 'Okänd linjetyp' : kind}${speedKmh != null ? ` · ${speedKmh} km/h` : ''}${hasBearing ? ` · ${bearingIsDerived ? 'riktning (beräknad)' : 'bäring'} ${Math.round(bearing)}°` : ''}</p>
    `);
  });

  // Drop markers for vehicles no longer present in this poll
  for (const [id, marker] of vehicleMarkers) {
    if (!seen.has(id)) {
      map.removeLayer(marker);
      vehicleMarkers.delete(id);
    }
  }
  for (const id of lastVehiclePosition.keys()) {
    if (!seen.has(id)) lastVehiclePosition.delete(id);
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
    if (opening) closeBoard();
  });
  closeBtn.addEventListener('click', () => {
    panel.classList.remove('open');
    panel.setAttribute('aria-hidden', 'true');
    toggle.classList.remove('active');
  });

  wireJourneyAutocomplete('from');
  wireJourneyAutocomplete('to');
  wireClearableInput('journey-from');
  wireClearableInput('journey-to');

  document.getElementById('journey-swap').addEventListener('click', () => {
    [journeyFrom, journeyTo] = [journeyTo, journeyFrom];
    const fromInput = document.getElementById('journey-from');
    const toInput = document.getElementById('journey-to');
    fromInput.value = journeyFrom ? journeyFrom.label : '';
    toInput.value = journeyTo ? journeyTo.label : '';
    syncClearButtonVisibility('journey-from');
    syncClearButtonVisibility('journey-to');
  });

  document.getElementById('journey-use-location').addEventListener('click', useCurrentLocationAsFrom);

  document.getElementById('journey-form').addEventListener('submit', (e) => {
    e.preventDefault();
    searchTrips();
  });

  wireSavedRoutes();
  renderSavedRoutes();

  const timeMode = document.getElementById('journey-time-mode');
  const timeValue = document.getElementById('journey-time-value');
  const dateValue = document.getElementById('journey-date-value');
  timeMode.addEventListener('change', () => {
    const showTime = timeMode.value !== 'now';
    timeValue.hidden = !showTime;
    dateValue.hidden = !showTime;
    if (showTime && !timeValue.value) {
      const now = new Date();
      timeValue.value = now.toTimeString().slice(0, 5);
      dateValue.value = now.toISOString().slice(0, 10);
    }
  });
}

// ==========================================================================
// Saved routes — persisted From/To pairs for one-tap re-search
// ==========================================================================
const SAVED_ROUTES_KEY = 'sparlage_saved_routes';

function loadSavedRoutes() {
  try {
    return JSON.parse(localStorage.getItem(SAVED_ROUTES_KEY)) || [];
  } catch {
    return [];
  }
}

function writeSavedRoutes(routes) {
  try {
    localStorage.setItem(SAVED_ROUTES_KEY, JSON.stringify(routes));
  } catch (err) {
    console.warn('[saved-routes] could not save (localStorage full?)', err);
  }
}

function wireSavedRoutes() {
  document.getElementById('journey-save-route').addEventListener('click', () => {
    if (!journeyFrom || !journeyTo) {
      alert('Välj både en start- och slutpunkt innan du sparar resan.');
      return;
    }
    const routes = loadSavedRoutes();
    const isDuplicate = routes.some(r =>
      r.from.label === journeyFrom.label && r.to.label === journeyTo.label);
    if (isDuplicate) {
      alert('Den här resan är redan sparad.');
      return;
    }
    routes.push({ id: Date.now(), from: journeyFrom, to: journeyTo });
    writeSavedRoutes(routes);
    renderSavedRoutes();

    const btn = document.getElementById('journey-save-route');
    btn.textContent = '★';
    btn.classList.add('saved');
    setTimeout(() => { btn.textContent = '☆'; btn.classList.remove('saved'); }, 1200);
  });
}

function renderSavedRoutes() {
  const section = document.getElementById('saved-routes');
  const list = document.getElementById('saved-routes-list');
  const routes = loadSavedRoutes();

  if (!routes.length) {
    section.setAttribute('hidden', '');
    return;
  }
  section.removeAttribute('hidden');
  list.innerHTML = '';

  routes.forEach(route => {
    const row = document.createElement('div');
    row.className = 'saved-route';
    row.innerHTML = `
      <span class="saved-route-text">${escapeHtml(route.from.label)} → ${escapeHtml(route.to.label)}</span>
      <button type="button" class="saved-route-remove" aria-label="Ta bort">✕</button>
    `;
    row.addEventListener('click', (e) => {
      if (e.target.closest('.saved-route-remove')) return; // handled separately below
      journeyFrom = route.from;
      journeyTo = route.to;
      document.getElementById('journey-from').value = route.from.label;
      document.getElementById('journey-to').value = route.to.label;
      syncClearButtonVisibility('journey-from');
      syncClearButtonVisibility('journey-to');
      searchTrips();
    });
    row.querySelector('.saved-route-remove').addEventListener('click', (e) => {
      e.stopPropagation();
      const updated = loadSavedRoutes().filter(r => r.id !== route.id);
      writeSavedRoutes(updated);
      renderSavedRoutes();
    });
    list.appendChild(row);
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
      syncClearButtonVisibility('journey-from');
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
        syncClearButtonVisibility(input.id);
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
  // passlist=true requests every intermediate stop per leg (confirmed in
  // Trafiklab's own docs), used to draw the trip map route as a sequence
  // of real stops rather than one straight origin-to-destination line.
  const params = { passlist: 'true' };
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

  const timeMode = document.getElementById('journey-time-mode').value;
  if (timeMode !== 'now') {
    const time = document.getElementById('journey-time-value').value;
    const date = document.getElementById('journey-date-value').value;
    if (time) params.time = time;
    if (date) params.date = date;
    if (timeMode === 'arrive') params.searchForArrival = 1;
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
    expandedTripCard = null;

    const header = document.createElement('div');
    header.className = 'journey-route-header';
    header.innerHTML = `${escapeHtml(journeyFrom.label)} → ${escapeHtml(journeyTo.label)}`;
    results.appendChild(header);

    trips.forEach(trip => results.appendChild(renderTripCard(trip)));
  } catch (err) {
    console.error('[journey] trip search failed', err);
    results.innerHTML = `<p class="journey-status-msg">Kunde inte hämta resor (se konsolen). ${escapeHtml(err.message || '')}</p>`;
  }
}

let expandedTripCard = null; // tracks the single currently-open trip card, so opening a new one closes the previous

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
    <span class="trip-expand-arrow">▾</span>
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

  // Tap to expand into full trip detail — a "Resedetaljer" tab (stop-by-stop
  // itinerary, like SL's own app) and a "Karta" tab (route drawn on a small
  // map, colored per mode, created lazily only when actually opened).
  const detail = document.createElement('div');
  detail.className = 'trip-detail';
  detail.hidden = true;
  detail.addEventListener('click', (e) => e.stopPropagation()); // don't let interacting with tabs/map collapse the card

  const tabs = document.createElement('div');
  tabs.className = 'trip-detail-tabs';
  tabs.innerHTML = `
    <button type="button" class="trip-tab active" data-tab="itinerary">Resedetaljer</button>
    <button type="button" class="trip-tab" data-tab="map">Karta</button>
  `;
  detail.appendChild(tabs);

  const itineraryPane = document.createElement('div');
  itineraryPane.className = 'trip-tab-pane';
  itineraryPane.appendChild(renderTripDetail(legs));
  detail.appendChild(itineraryPane);

  const mapPane = document.createElement('div');
  mapPane.className = 'trip-tab-pane';
  mapPane.hidden = true;
  const mapContainer = document.createElement('div');
  mapContainer.className = 'trip-map-container';
  mapPane.appendChild(mapContainer);
  mapPane.appendChild(renderTripMapLegend(legs));
  detail.appendChild(mapPane);

  card.appendChild(detail);

  let tripMap = null;
  tabs.addEventListener('click', (e) => {
    const btn = e.target.closest('.trip-tab');
    if (!btn) return;
    const showMap = btn.dataset.tab === 'map';
    tabs.querySelectorAll('.trip-tab').forEach(b => b.classList.toggle('active', b === btn));
    itineraryPane.hidden = showMap;
    mapPane.hidden = !showMap;
    if (showMap) {
      if (!tripMap) {
        tripMap = renderTripMap(mapContainer, legs);
      } else {
        setTimeout(() => tripMap.invalidateSize(), 30); // container was display:none during creation, size cache needs refreshing
      }
    }
  });

  card.addEventListener('click', () => {
    const wasOpen = !detail.hidden;
    // Accordion: only one trip's detail open at a time, keeps the list readable.
    if (expandedTripCard && expandedTripCard !== card) {
      expandedTripCard.classList.remove('expanded');
      expandedTripCard.querySelector('.trip-detail').hidden = true;
    }
    detail.hidden = wasOpen;
    card.classList.toggle('expanded', !wasOpen);
    expandedTripCard = wasOpen ? null : card;
  });

  return card;
}

// Builds the itinerary as a timeline: each transit leg gets a colored
// vertical bar (mode-colored) connecting its departure and arrival stops,
// with a badge-style line label. Between legs, a distinct transfer row
// shows the wait/change time — matching SL's own app structure, where a
// stop CAN legitimately appear twice in a row (once as arrival, once as
// the next leg's departure) as long as there's a clear transfer marker
// between them, rather than silently merging them into one ambiguous row.
// Note: intermediate/passthrough stops ("Visa X hållplatser" in SL's app)
// aren't shown here — that needs a passlist request ResRobot supports but
// this build doesn't currently ask for.
function renderTripDetail(legs) {
  const container = document.createElement('div');

  legs.forEach((leg, i) => {
    if (leg.type === 'WALK' || leg.type === 'TRSF') {
      container.appendChild(renderWalkLeg(leg));
    } else {
      container.appendChild(renderTransitLeg(leg));
    }

    const nextLeg = legs[i + 1];
    if (nextLeg) {
      const thisEnd = leg.Destination && leg.Destination.time;
      const nextStart = nextLeg.Origin && nextLeg.Origin.time;
      const waitMin = minutesBetween(thisEnd, nextStart);
      if (waitMin != null && waitMin > 0) {
        container.appendChild(renderTransferRow(waitMin));
      }
    }
  });

  return container;
}

function minutesBetween(hhmmss1, hhmmss2) {
  if (!hhmmss1 || !hhmmss2) return null;
  const [h1, m1] = hhmmss1.split(':').map(Number);
  const [h2, m2] = hhmmss2.split(':').map(Number);
  if ([h1, m1, h2, m2].some(Number.isNaN)) return null;
  return (h2 * 60 + m2) - (h1 * 60 + m1);
}

function renderTransitLeg(leg) {
  const catCode = leg.Product && leg.Product[0] && leg.Product[0].catCode;
  const mode = CAT_CODE_TO_MODE[catCode] || 'pendeltag';
  const lineLabel = (leg.Product && leg.Product[0] && (leg.Product[0].displayNumber || leg.Product[0].line)) || leg.name || '?';
  const originTrack = leg.Origin && (leg.Origin.track || leg.Origin.rtTrack);
  const destTrack = leg.Destination && (leg.Destination.track || leg.Destination.rtTrack);

  const wrap = document.createElement('div');
  wrap.className = `itin-leg itin-${mode}`;
  wrap.innerHTML = `
    <div class="itin-stop">
      <span class="itin-time">${formatClock(leg.Origin && leg.Origin.time)}</span>
      <span class="itin-name">${escapeHtml((leg.Origin && leg.Origin.name) || '')}</span>
      ${originTrack ? `<span class="itin-track">Läge ${escapeHtml(originTrack)}</span>` : ''}
    </div>
    <div class="itin-line-info">
      <span class="itin-badge dot-${mode}">${MODE_ICONS[mode] || ''} ${escapeHtml(lineLabel)}</span>
      <span class="itin-line-desc">${MODE_LABELS_SV[mode] || ''} mot ${escapeHtml((leg.Destination && leg.Destination.name) || '')}</span>
    </div>
    <div class="itin-stop">
      <span class="itin-time">${formatClock(leg.Destination && leg.Destination.time)}</span>
      <span class="itin-name">${escapeHtml((leg.Destination && leg.Destination.name) || '')}</span>
      ${destTrack ? `<span class="itin-track">Läge ${escapeHtml(destTrack)}</span>` : ''}
    </div>
  `;
  return wrap;
}

function renderWalkLeg(leg) {
  const wrap = document.createElement('div');
  wrap.className = 'itin-leg itin-walk';
  const distLabel = leg.dist ? `${leg.dist} m` : '';
  const durLabel = leg.duration ? formatIsoDuration(leg.duration) : '';
  wrap.innerHTML = `
    <div class="itin-stop">
      <span class="itin-time">${formatClock(leg.Origin && leg.Origin.time)}</span>
      <span class="itin-name">${escapeHtml((leg.Origin && leg.Origin.name) || '')}</span>
    </div>
    <div class="itin-line-info">
      <span class="itin-walk-icon">🚶</span>
      <span class="itin-line-desc">Promenad ${distLabel} ${durLabel ? `(${durLabel})` : ''}</span>
    </div>
    <div class="itin-stop">
      <span class="itin-time">${formatClock(leg.Destination && leg.Destination.time)}</span>
      <span class="itin-name">${escapeHtml((leg.Destination && leg.Destination.name) || '')}</span>
    </div>
  `;
  return wrap;
}

// Draws the trip's route on a small standalone Leaflet map (separate
// instance from the main app map) — a straight-line connector per leg
// between its stop coordinates, colored to match the mode palette used
// everywhere else. Not the actual rail/road geometry (ResRobot's basic
// trip response doesn't include a shape/polyline, only stop coordinates),
// so curves in the real line won't show — flagged here rather than
// silently presented as more precise than it is.
const MODE_LABELS_SV = {
  pendeltag: 'Pendeltåg', roslagsbanan: 'Roslagsbanan', metro: 'Tunnelbana',
  tram: 'Spårvagn', bus: 'Buss', boat: 'Båt',
};
const MODE_ICONS = {
  pendeltag: '🚆', roslagsbanan: '🚈', metro: '🚇', tram: '🚊', bus: '🚌', boat: '⛴️',
};

// Only lists the modes actually present in this specific trip, rather than
// a full 6-mode legend every time — more relevant for a 2-3 leg journey.
function renderTripMapLegend(legs) {
  const modesUsed = new Set();
  let hasWalk = false;
  legs.forEach(leg => {
    if (leg.type === 'WALK' || leg.type === 'TRSF') { hasWalk = true; return; }
    const catCode = leg.Product && leg.Product[0] && leg.Product[0].catCode;
    modesUsed.add(CAT_CODE_TO_MODE[catCode] || 'pendeltag');
  });

  const legend = document.createElement('div');
  legend.className = 'trip-map-legend';
  let html = '';
  modesUsed.forEach(mode => {
    html += `<span class="journey-legend-item"><span class="dot dot-${mode}"></span>${MODE_LABELS_SV[mode] || mode}</span>`;
  });
  if (hasWalk) html += `<span class="journey-legend-item"><span class="dot dot-walk"></span>Gång</span>`;
  legend.innerHTML = html;
  return legend;
}

function renderTripMap(container, legs) {
  const map = L.map(container, { zoomControl: true, attributionControl: false })
    .setView(STOCKHOLM_CENTER, 12);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO',
    subdomains: 'abcd',
    maxZoom: 19,
  }).addTo(map);

  const bounds = [];
  let missingCoords = false;

  legs.forEach(leg => {
    const o = leg.Origin, d = leg.Destination;
    if (!o || !d || o.lat == null || o.lon == null || d.lat == null || d.lon == null) {
      missingCoords = true;
      return;
    }

    // Prefer the full stop sequence (from passlist=true) over a single
    // straight origin-to-destination line — Stop is the typical HAFAS
    // field name for intermediate stops; check a couple of plausible
    // shapes defensively since this wasn't directly confirmed in the spec.
    const passedStops = (leg.Stops && leg.Stops.Stop) || leg.Stop || null;
    let points;
    if (Array.isArray(passedStops) && passedStops.length) {
      points = passedStops
        .filter(s => s.lat != null && s.lon != null)
        .map(s => [s.lat, s.lon]);
    }
    if (!points || points.length < 2) {
      points = [[o.lat, o.lon], [d.lat, d.lon]];
    }

    const isWalk = leg.type === 'WALK' || leg.type === 'TRSF';
    const catCode = leg.Product && leg.Product[0] && leg.Product[0].catCode;
    const mode = isWalk ? null : (CAT_CODE_TO_MODE[catCode] || 'pendeltag');
    const color = mode
      ? getComputedStyle(document.documentElement).getPropertyValue(`--${mode}`).trim()
      : '#7d8590';

    L.polyline(points, {
      color, weight: isWalk ? 3 : 5, opacity: 0.9,
      dashArray: isWalk ? '4,7' : null,
    }).addTo(map);

    // Endpoint markers only (not every intermediate stop) — keeps the map
    // readable rather than dotting every single passthrough station.
    [[o.lat, o.lon], [d.lat, d.lon]].forEach(ll => {
      L.circleMarker(ll, { radius: 5, color: '#0d1117', weight: 2, fillColor: color, fillOpacity: 1 }).addTo(map);
    });
    points.forEach(p => bounds.push(p));
  });

  if (bounds.length) {
    map.fitBounds(bounds, { padding: [24, 24] });
  }
  if (missingCoords) {
    console.warn('[journey] one or more legs had no coordinates from ResRobot — route line may be incomplete. Inspect window.DEBUG_LAST_TRIPS.');
  }
  console.log('[journey] rendered trip map — if the route looks like straight lines instead of following stops, check whether legs have a Stops.Stop array:', legs[0]);
  if (!bounds.length) {
    container.innerHTML = '<p class="trip-map-empty">Ingen kartdata tillgänglig för den här resan.</p>';
  }

  return map;
}

function renderTransferRow(waitMin) {
  const row = document.createElement('div');
  row.className = 'itin-transfer';
  row.textContent = `${waitMin} min · Byte`;
  return row;
}

function renderLegChip(leg) {
  const chip = document.createElement('span');
  if (leg.type === 'WALK' || leg.type === 'TRSF') {
    chip.className = 'trip-leg trip-leg-walk';
    chip.textContent = leg.dist ? `🚶 Gå ${leg.dist} m` : '🚶 Gå';
    return chip;
  }
  const catCode = leg.Product && leg.Product[0] && leg.Product[0].catCode;
  const mode = CAT_CODE_TO_MODE[catCode] || 'pendeltag';
  const lineLabel = (leg.Product && leg.Product[0] && (leg.Product[0].displayNumber || leg.Product[0].line)) || leg.name || '?';

  chip.className = 'trip-leg';
  chip.innerHTML = `<span class="trip-leg-icon">${MODE_ICONS[mode] || ''}</span><span class="trip-leg-dot dot-${mode}"></span><span>${escapeHtml(lineLabel)}</span>`;
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
