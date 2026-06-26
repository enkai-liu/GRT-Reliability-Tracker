const state = {
  data: null,
  live: null,
  selectedKey: null,
  selectedStopKey: null,
  mode: "all",
  search: "",
  sort: "on_time_asc",
  map: {
    width: 0,
    height: 0,
    scale: 1,
    panX: 0,
    panY: 0,
    dragging: false,
    dragStartX: 0,
    dragStartY: 0,
    dragPanX: 0,
    dragPanY: 0,
    dragMoved: false,
  },
};

const els = {
  dateRange: document.querySelector("#dateRange"),
  systemOnTime: document.querySelector("#systemOnTime"),
  systemMedian: document.querySelector("#systemMedian"),
  systemP90: document.querySelector("#systemP90"),
  systemRoutes: document.querySelector("#systemRoutes"),
  routeSearch: document.querySelector("#routeSearch"),
  sortRoutes: document.querySelector("#sortRoutes"),
  routeList: document.querySelector("#routeList"),
  routeCount: document.querySelector("#routeCount"),
  networkMap: document.querySelector("#networkMap"),
  zoomOut: document.querySelector("#zoomOut"),
  zoomReset: document.querySelector("#zoomReset"),
  zoomIn: document.querySelector("#zoomIn"),
  selectedMode: document.querySelector("#selectedMode"),
  selectedTitle: document.querySelector("#selectedTitle"),
  routeBadge: document.querySelector("#routeBadge"),
  routeMode: document.querySelector("#routeMode"),
  routeTitle: document.querySelector("#routeTitle"),
  routeOnTime: document.querySelector("#routeOnTime"),
  routeLate: document.querySelector("#routeLate"),
  routeMedian: document.querySelector("#routeMedian"),
  routeP90: document.querySelector("#routeP90"),
  hourChart: document.querySelector("#hourChart"),
  stopTable: document.querySelector("#stopTable"),
  stopCount: document.querySelector("#stopCount"),
  liveSection: document.querySelector("#liveSection"),
  liveUpdated: document.querySelector("#liveUpdated"),
  liveSummary: document.querySelector("#liveSummary"),
  liveArrivals: document.querySelector("#liveArrivals"),
  transferSection: document.querySelector("#transferSection"),
  transferSubtitle: document.querySelector("#transferSubtitle"),
  transferTable: document.querySelector("#transferTable"),
};

const ns = "http://www.w3.org/2000/svg";

function pct(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function seconds(value) {
  return `${Math.round(Number(value || 0))}s`;
}

function number(value) {
  return Number(value || 0).toLocaleString();
}

function reliabilityColor(onTimeRate) {
  if (onTimeRate >= 0.95) return "#38c78d";
  if (onTimeRate >= 0.9) return "#4fa3dd";
  if (onTimeRate >= 0.85) return "#eda63a";
  return "#ef5d6c";
}

function routeLabel(route) {
  return route.route_short_name || route.route_id;
}

function stopKey(stop) {
  return `${stop.direction_id}:${stop.stop_id}`;
}

function selectedRoute() {
  return state.data.routes.find((item) => item.key === state.selectedKey) || null;
}

function selectedStop() {
  if (!state.selectedKey || !state.selectedStopKey) return null;
  return (state.data.routeStops[state.selectedKey] || []).find((stop) => stopKey(stop) === state.selectedStopKey) || null;
}

function sortRouteNumber(a, b) {
  const an = Number(routeLabel(a));
  const bn = Number(routeLabel(b));
  if (Number.isFinite(an) && Number.isFinite(bn)) return an - bn;
  return routeLabel(a).localeCompare(routeLabel(b), undefined, { numeric: true });
}

function filteredRoutes() {
  const query = state.search.trim().toLowerCase();
  const routes = state.data.routes.filter((route) => {
    if (state.mode !== "all" && route.transit_mode !== state.mode) return false;
    if (!query) return true;
    const haystack = [
      route.route_short_name,
      route.route_id,
      route.transit_mode,
      pct(route.on_time_rate),
      seconds(route.p90_delay_seconds),
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });

  routes.sort((a, b) => {
    if (state.sort === "on_time_desc") return b.on_time_rate - a.on_time_rate;
    if (state.sort === "p90_desc") return b.p90_delay_seconds - a.p90_delay_seconds;
    if (state.sort === "observations_desc") return b.observations - a.observations;
    if (state.sort === "route_asc") return sortRouteNumber(a, b);
    return a.on_time_rate - b.on_time_rate;
  });

  return routes;
}

function projectPoint(lat, lon, width, height) {
  const pad = 44;
  const { minLat, maxLat, minLon, maxLon } = state.data.bounds;
  const baseX = pad + ((lon - minLon) / (maxLon - minLon)) * (width - pad * 2);
  const baseY = pad + ((maxLat - lat) / (maxLat - minLat)) * (height - pad * 2);
  const centerX = width / 2;
  const centerY = height / 2;
  const x = centerX + (baseX - centerX) * state.map.scale + state.map.panX;
  const y = centerY + (baseY - centerY) * state.map.scale + state.map.panY;
  return [x, y];
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function setZoom(nextScale, anchorX = state.map.width / 2, anchorY = state.map.height / 2) {
  const oldScale = state.map.scale;
  const newScale = clamp(nextScale, 0.65, 7);
  if (newScale === oldScale) return;

  const centerX = state.map.width / 2;
  const centerY = state.map.height / 2;
  state.map.panX = anchorX - centerX - ((anchorX - centerX - state.map.panX) / oldScale) * newScale;
  state.map.panY = anchorY - centerY - ((anchorY - centerY - state.map.panY) / oldScale) * newScale;
  state.map.scale = newScale;
  drawMap();
}

function resetZoom() {
  state.map.scale = 1;
  state.map.panX = 0;
  state.map.panY = 0;
  drawMap();
}

function pathForPoints(points, width, height) {
  return points
    .map(([lat, lon], index) => {
      const [x, y] = projectPoint(lat, lon, width, height);
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function drawMap() {
  if (state.selectedKey) {
    drawLineDiagram();
    return;
  }

  const svg = els.networkMap;
  const rect = svg.getBoundingClientRect();
  const width = Math.max(600, rect.width);
  const height = Math.max(600, rect.height);
  state.map.width = width;
  state.map.height = height;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.textContent = "";

  const background = document.createElementNS(ns, "g");
  background.setAttribute("class", "routes-background");
  const foreground = document.createElementNS(ns, "g");
  foreground.setAttribute("class", "routes-foreground");
  const stopsLayer = document.createElementNS(ns, "g");
  stopsLayer.setAttribute("class", "stops-layer");
  svg.append(background, foreground, stopsLayer);

  for (const route of state.data.routes) {
    const shapes = state.data.routeShapes[route.key] || [];
    const targetLayer = route.key === state.selectedKey ? foreground : background;
    for (const shape of shapes) {
      const path = document.createElementNS(ns, "path");
      path.setAttribute("class", routeLineClass(route.key));
      path.setAttribute("d", pathForPoints(shape.points, width, height));
      path.setAttribute("stroke", reliabilityColor(route.on_time_rate));
      const baseWidth = route.transit_mode === "lrt" ? 6.5 : route.key === state.selectedKey ? 4.8 : 2.6;
      path.setAttribute("stroke-width", (baseWidth / Math.sqrt(state.map.scale)).toFixed(2));
      path.dataset.routeKey = route.key;
      path.addEventListener("click", () => {
        if (!state.map.dragMoved) selectRoute(route.key);
      });
      targetLayer.append(path);
    }
  }

  const selectedStops = state.data.routeStops[state.selectedKey] || [];
  for (const stop of selectedStops) {
    const [x, y] = projectPoint(stop.stop_lat, stop.stop_lon, width, height);
    const dot = document.createElementNS(ns, "circle");
    dot.setAttribute("class", "stop-dot visible");
    dot.setAttribute("cx", x.toFixed(1));
    dot.setAttribute("cy", y.toFixed(1));
    const radius = stop.on_time_rate < 0.8 ? 4.6 : 3.4;
    dot.setAttribute("r", (radius / Math.sqrt(state.map.scale)).toFixed(2));
    dot.setAttribute("stroke", reliabilityColor(stop.on_time_rate));
    stopsLayer.append(dot);
  }
}

function longestShape(routeKey) {
  const shapes = state.data.routeShapes[routeKey] || [];
  return shapes.reduce((longest, shape) => {
    if (!longest || shape.points.length > longest.points.length) return shape;
    return longest;
  }, null);
}

function distanceSquared(aLat, aLon, bLat, bLon) {
  const latScale = 111_000;
  const lonScale = Math.cos((aLat * Math.PI) / 180) * 111_000;
  const dy = (aLat - bLat) * latScale;
  const dx = (aLon - bLon) * lonScale;
  return dx * dx + dy * dy;
}

function shapeProgressLookup(routeKey) {
  const shape = longestShape(routeKey);
  if (!shape || shape.points.length < 2) return new Map();

  const cumulative = [0];
  for (let index = 1; index < shape.points.length; index += 1) {
    const [prevLat, prevLon] = shape.points[index - 1];
    const [lat, lon] = shape.points[index];
    cumulative.push(cumulative[index - 1] + Math.sqrt(distanceSquared(prevLat, prevLon, lat, lon)));
  }

  const stops = state.data.routeStops[routeKey] || [];
  const progress = new Map();
  for (const stop of stops) {
    let bestDistance = Number.POSITIVE_INFINITY;
    let bestProgress = 0;
    for (let index = 0; index < shape.points.length; index += 1) {
      const [lat, lon] = shape.points[index];
      const distance = distanceSquared(stop.stop_lat, stop.stop_lon, lat, lon);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestProgress = cumulative[index];
      }
    }
    progress.set(stopKey(stop), bestProgress);
  }
  return progress;
}

function stopsInLineOrder(routeKey) {
  const progress = shapeProgressLookup(routeKey);
  return [...(state.data.routeStops[routeKey] || [])].sort((a, b) => {
    if (a.direction_id !== b.direction_id) return a.direction_id - b.direction_id;
    const ap = progress.get(stopKey(a)) ?? 0;
    const bp = progress.get(stopKey(b)) ?? 0;
    return ap - bp || a.stop_name.localeCompare(b.stop_name, undefined, { numeric: true });
  });
}

function drawLineDiagram() {
  const svg = els.networkMap;
  const rect = svg.getBoundingClientRect();
  const width = Math.max(600, rect.width);
  const height = Math.max(420, rect.height);
  const route = selectedRoute();
  const stops = stopsInLineOrder(state.selectedKey);
  const byDirection = new Map();

  state.map.width = width;
  state.map.height = height;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.textContent = "";

  if (!route || !stops.length) return;

  for (const stop of stops) {
    const direction = stop.direction_id ?? 0;
    if (!byDirection.has(direction)) byDirection.set(direction, []);
    byDirection.get(direction).push(stop);
  }

  const directions = [...byDirection.keys()].sort((a, b) => a - b);
  const padX = 72;
  const top = Math.max(110, height / 2 - directions.length * 58);
  const rowGap = directions.length > 1 ? 116 : 0;
  const lineColor = reliabilityColor(route.on_time_rate);

  directions.forEach((direction, rowIndex) => {
    const rowStops = byDirection.get(direction);
    const y = top + rowIndex * rowGap;
    const startX = padX;
    const endX = width - padX;
    const label = document.createElementNS(ns, "text");
    label.setAttribute("class", "line-direction-label");
    label.setAttribute("x", "28");
    label.setAttribute("y", (y + 5).toFixed(1));
    label.textContent = `Dir ${direction}`;
    svg.append(label);

    const rail = document.createElementNS(ns, "line");
    rail.setAttribute("class", "line-diagram-rail");
    rail.setAttribute("x1", startX);
    rail.setAttribute("x2", endX);
    rail.setAttribute("y1", y);
    rail.setAttribute("y2", y);
    rail.setAttribute("stroke", lineColor);
    svg.append(rail);

    rowStops.forEach((stop, index) => {
      const denominator = Math.max(1, rowStops.length - 1);
      const x = startX + ((endX - startX) * index) / denominator;
      const isSelected = stopKey(stop) === state.selectedStopKey;

      const hit = document.createElementNS(ns, "circle");
      hit.setAttribute("class", "station-hit-area");
      hit.setAttribute("cx", x.toFixed(1));
      hit.setAttribute("cy", y.toFixed(1));
      hit.setAttribute("r", "17");
      hit.dataset.stopKey = stopKey(stop);
      hit.addEventListener("click", () => selectStop(stopKey(stop)));
      svg.append(hit);

      const dot = document.createElementNS(ns, "circle");
      dot.setAttribute("class", `station-dot${isSelected ? " selected" : ""}`);
      dot.setAttribute("cx", x.toFixed(1));
      dot.setAttribute("cy", y.toFixed(1));
      dot.setAttribute("r", isSelected ? "8" : "6");
      dot.setAttribute("fill", reliabilityColor(stop.on_time_rate));
      dot.dataset.stopKey = stopKey(stop);
      dot.addEventListener("click", () => selectStop(stopKey(stop)));
      const title = document.createElementNS(ns, "title");
      title.textContent = `${stop.stop_name} · ${pct(stop.on_time_rate)} on-time · ${seconds(stop.p90_delay_seconds)} P90`;
      dot.append(title);
      svg.append(dot);

      const showLabel = isSelected || index === 0 || index === rowStops.length - 1 || rowStops.length <= 8;
      if (showLabel) {
        const text = document.createElementNS(ns, "text");
        text.setAttribute("class", `station-label${isSelected ? " selected" : ""}`);
        text.setAttribute("x", x.toFixed(1));
        text.setAttribute("y", (y + 28 + (index % 2) * 16).toFixed(1));
        text.textContent = stop.stop_name;
        svg.append(text);
      }
    });
  });
}

function routeLineClass(key) {
  const classes = ["route-line"];
  if (state.selectedKey) {
    classes.push(key === state.selectedKey ? "selected" : "dimmed");
  }
  return classes.join(" ");
}

function renderSystemMetrics() {
  const rows = state.data.systemByDate;
  const observations = rows.reduce((sum, row) => sum + row.observations, 0);
  const onTime = rows.reduce((sum, row) => sum + row.on_time_rate * row.observations, 0) / observations;
  const median = rows[Math.floor(rows.length / 2)]?.median_delay_seconds || 0;
  const p90 = rows.reduce((sum, row) => sum + row.p90_delay_seconds * row.observations, 0) / observations;

  els.dateRange.textContent = `${state.data.overall.startDate} to ${state.data.overall.endDate}`;
  els.systemOnTime.textContent = pct(onTime);
  els.systemMedian.textContent = seconds(median);
  els.systemP90.textContent = seconds(p90);
  els.systemRoutes.textContent = state.data.routes.length;
}

function renderRouteList() {
  const routes = filteredRoutes();
  els.routeCount.textContent = routes.length;
  els.routeList.textContent = "";

  for (const route of routes) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `route-item${route.key === state.selectedKey ? " active" : ""}`;
    button.addEventListener("click", () => selectRoute(route.key));

    const color = reliabilityColor(route.on_time_rate);
    button.innerHTML = `
      <div class="route-pill" style="background:${color}">${routeLabel(route)}</div>
      <div class="route-name">
        <strong>${route.transit_mode.toUpperCase()} ${routeLabel(route)}</strong>
        <span>${number(route.observations)} observations</span>
      </div>
      <div class="route-score">
        <strong>${pct(route.on_time_rate)}</strong>
        <span>${seconds(route.p90_delay_seconds)}</span>
      </div>
    `;
    els.routeList.append(button);
  }
}

function renderDetail() {
  const route = selectedRoute();
  const stop = selectedStop();
  renderLive();
  renderTransfers();

  if (!route) {
    els.selectedMode.textContent = "System";
    els.selectedTitle.textContent = "All lines";
    els.routeBadge.textContent = "--";
    els.routeBadge.style.background = "var(--panel-raised)";
    els.routeBadge.style.color = "var(--muted)";
    els.routeMode.textContent = "Select a line";
    els.routeTitle.textContent = "System overview";
    els.routeOnTime.textContent = els.systemOnTime.textContent;
    els.routeLate.textContent = "--";
    els.routeMedian.textContent = els.systemMedian.textContent;
    els.routeP90.textContent = els.systemP90.textContent;
    renderSystemHourChart();
    renderStopTable([]);
    return;
  }

  const metricSource = stop || route;
  const color = reliabilityColor(metricSource.on_time_rate);
  els.selectedMode.textContent = route.transit_mode.toUpperCase();
  els.selectedTitle.textContent = stop ? stop.stop_name : `Route ${routeLabel(route)}`;
  els.routeBadge.textContent = routeLabel(route);
  els.routeBadge.style.background = color;
  els.routeBadge.style.color = "";
  els.routeMode.textContent = stop
    ? `${route.transit_mode.toUpperCase()} route ${routeLabel(route)} · direction ${stop.direction_id}`
    : `${route.transit_mode.toUpperCase()} line`;
  els.routeTitle.textContent = stop ? stop.stop_name : `Route ${routeLabel(route)}`;
  els.routeOnTime.textContent = pct(metricSource.on_time_rate);
  els.routeLate.textContent = pct(metricSource.late_rate);
  els.routeMedian.textContent = seconds(metricSource.median_delay_seconds);
  els.routeP90.textContent = seconds(metricSource.p90_delay_seconds);
  renderHourChart(state.data.routeByHour[route.key] || []);
  renderStopTable(stopsInLineOrder(route.key));
}

function renderSystemHourChart() {
  const byHour = new Map();
  for (const rows of Object.values(state.data.routeByHour)) {
    for (const row of rows) {
      const item = byHour.get(row.hour_of_day) || { observations: 0, weighted: 0 };
      item.observations += row.observations;
      item.weighted += row.on_time_rate * row.observations;
      byHour.set(row.hour_of_day, item);
    }
  }
  const rows = Array.from({ length: 24 }, (_, hour) => {
    const item = byHour.get(hour);
    return {
      hour_of_day: hour,
      on_time_rate: item ? item.weighted / item.observations : 0,
    };
  });
  renderHourChart(rows);
}

function renderHourChart(rows) {
  const byHour = new Map(rows.map((row) => [row.hour_of_day, row]));
  els.hourChart.textContent = "";
  for (let hour = 0; hour < 24; hour += 1) {
    const item = byHour.get(hour);
    const rate = item ? item.on_time_rate : 0;
    const bar = document.createElement("div");
    bar.className = "hour-bar";
    bar.style.height = `${Math.max(3, rate * 126)}px`;
    bar.style.background = reliabilityColor(rate);
    bar.title = `${hour}:00 · ${pct(rate)} on-time`;
    if (hour % 4 === 0) {
      const label = document.createElement("span");
      label.textContent = hour;
      bar.append(label);
    }
    els.hourChart.append(bar);
  }
}

function renderStopTable(stops) {
  els.stopCount.textContent = `${stops.length} stops`;
  els.stopTable.textContent = "";

  if (!stops.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Select a route to inspect stop-level reliability.";
    els.stopTable.append(empty);
    return;
  }

  for (const stop of stops) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `stop-row${stopKey(stop) === state.selectedStopKey ? " active" : ""}`;
    row.addEventListener("click", () => selectStop(stopKey(stop)));
    row.innerHTML = `
      <div>
        <strong>${stop.stop_name}</strong>
        <span>Direction ${stop.direction_id} · ${number(stop.observations)} obs</span>
      </div>
      <div class="stop-metric" style="color:${reliabilityColor(stop.on_time_rate)}">${pct(stop.on_time_rate)}</div>
      <div class="stop-metric">${seconds(stop.p90_delay_seconds)}</div>
    `;
    els.stopTable.append(row);
  }
}

function transferColor(successRate) {
  if (successRate >= 0.9) return "var(--good)";
  if (successRate >= 0.8) return "var(--ok)";
  if (successRate >= 0.7) return "var(--watch)";
  return "var(--poor)";
}

function routeLabelByKey(key) {
  const route = state.data.routes.find((item) => item.key === key);
  return route ? routeLabel(route) : key.split(":")[1];
}

function renderTransfers() {
  const transfers = state.data.transfers;
  if (!transfers || (!transfers.worst.length && !Object.keys(transfers.byRoute).length)) {
    els.transferSection.hidden = true;
    return;
  }
  els.transferSection.hidden = false;

  const route = selectedRoute();
  const rows = route
    ? [...(transfers.byRoute[route.key] || [])].sort((a, b) => a.success_rate - b.success_rate)
    : transfers.worst;
  els.transferSubtitle.textContent = route
    ? `From route ${routeLabel(route)}`
    : "Riskiest connections";

  els.transferTable.textContent = "";
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "No frequently used connections observed from this route.";
    els.transferTable.append(empty);
    return;
  }

  for (const item of rows.slice(0, 10)) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = "transfer-row";
    row.addEventListener("click", () => selectRoute(item.toKey));
    const fromLabel = routeLabelByKey(item.fromKey);
    const toLabel = routeLabelByKey(item.toKey);
    const waitMinutes = Math.round(item.median_scheduled_wait_seconds / 60);
    row.innerHTML = `
      <div>
        <strong>${route ? "" : `${fromLabel} `}→ ${toLabel} · ${item.transfer_stop_name}</strong>
        <span>${number(item.attempts)} attempts · ${waitMinutes} min scheduled wait</span>
      </div>
      <div class="stop-metric" style="color:${transferColor(item.success_rate)}">${pct(item.success_rate)}</div>
    `;
    els.transferTable.append(row);
  }
}

function delayMinutes(secondsValue) {
  const minutes = Number(secondsValue || 0) / 60;
  const rounded = Math.round(minutes * 10) / 10;
  if (rounded === 0) return "on time";
  return `${rounded > 0 ? "+" : ""}${rounded.toFixed(1)} min`;
}

function delayRange(lowerSeconds, upperSeconds) {
  const lower = Math.round(Number(lowerSeconds || 0) / 60);
  const upper = Math.round(Number(upperSeconds || 0) / 60);
  if (lower === upper) return null;
  const signed = (minutes) => `${minutes > 0 ? "+" : ""}${minutes}`;
  return `${signed(lower)} to ${signed(upper)} min`;
}

function delayColor(secondsValue) {
  if (secondsValue <= 120) return "var(--good)";
  if (secondsValue <= 300) return "var(--watch)";
  return "var(--poor)";
}

async function fetchLive() {
  try {
    const response = await fetch(`data/live-predictions.json?t=${Date.now()}`, { cache: "no-store" });
    state.live = response.ok ? await response.json() : null;
  } catch {
    state.live = null;
  }
  renderLive();
}

function liveAgeMinutes() {
  return (Date.now() - Date.parse(state.live.generatedAtUtc)) / 60000;
}

function liveSummaryItem(label, value, color) {
  const item = document.createElement("article");
  const name = document.createElement("span");
  name.textContent = label;
  const metric = document.createElement("strong");
  metric.textContent = value;
  if (color) metric.style.color = color;
  item.append(name, metric);
  return item;
}

function renderLiveArrivals(arrivals, emptyMessage) {
  els.liveArrivals.textContent = "";
  if (!arrivals.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = emptyMessage;
    els.liveArrivals.append(empty);
    return;
  }
  for (const arrival of arrivals) {
    const range = delayRange(arrival.predicted_delay_lower_seconds, arrival.predicted_delay_upper_seconds);
    const hasRange = range !== null
      && arrival.predicted_delay_lower_seconds !== undefined
      && arrival.predicted_delay_upper_seconds !== undefined;
    const delayText = hasRange ? range : delayMinutes(arrival.predicted_delay_seconds);
    const delayTitle = hasRange ? ` title="most likely ${delayMinutes(arrival.predicted_delay_seconds)}"` : "";
    const row = document.createElement("div");
    row.className = "live-arrival";
    row.innerHTML = `
      <div>
        <strong>${arrival.transit_mode.toUpperCase()} ${arrival.route_short_name} · ${arrival.stop_name || arrival.stop_id}</strong>
        <span>arrives in ${Math.round(arrival.eta_minutes)} min · feed says ${delayMinutes(arrival.feed_delay_seconds)}</span>
      </div>
      <div class="live-delay" style="color:${delayColor(arrival.predicted_delay_seconds)}"${delayTitle}>${delayText}</div>
    `;
    els.liveArrivals.append(row);
  }
}

function renderLive() {
  const live = state.live;
  if (!live || !live.totals || !live.totals.stopArrivals) {
    els.liveSection.hidden = true;
    return;
  }

  els.liveSection.hidden = false;
  const age = liveAgeMinutes();
  const updated = new Date(live.generatedAtUtc).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  els.liveUpdated.textContent = age > 10 ? `stale · ${updated}` : `updated ${updated}`;
  els.liveUpdated.classList.toggle("stale", age > 10);
  els.liveSummary.textContent = "";

  const route = selectedRoute();
  const liveRoute = route ? live.routes.find((item) => item.key === route.key) : null;

  if (route) {
    if (!liveRoute) {
      els.liveSummary.append(liveSummaryItem("Status", "not running"));
      renderLiveArrivals([], "No live vehicles on this route right now.");
      return;
    }
    els.liveSummary.append(
      liveSummaryItem("Avg delay", delayMinutes(liveRoute.mean_predicted_delay_seconds), delayColor(liveRoute.mean_predicted_delay_seconds)),
      liveSummaryItem("Worst", delayMinutes(liveRoute.max_predicted_delay_seconds), delayColor(liveRoute.max_predicted_delay_seconds)),
      liveSummaryItem("Vehicles", number(liveRoute.trips)),
      liveSummaryItem("Late stops", number(liveRoute.late_arrivals), liveRoute.late_arrivals ? "var(--poor)" : null),
    );
    renderLiveArrivals(
      live.worstArrivals.filter((arrival) => arrival.routeKey === route.key).slice(0, 6),
      "No upcoming arrivals from this route among the system's most delayed.",
    );
    return;
  }

  const lateArrivals = live.routes.reduce((sum, item) => sum + item.late_arrivals, 0);
  els.liveSummary.append(
    liveSummaryItem("Vehicles", number(live.totals.trips)),
    liveSummaryItem("Routes", number(live.totals.routes)),
    liveSummaryItem("Arrivals", number(live.totals.stopArrivals)),
    liveSummaryItem("Late", number(lateArrivals), lateArrivals ? "var(--poor)" : null),
  );
  renderLiveArrivals(live.worstArrivals.slice(0, 6), "No delayed arrivals right now.");
}

function selectRoute(key) {
  const nextKey = state.selectedKey === key ? null : key;
  state.selectedKey = nextKey;
  state.selectedStopKey = null;
  resetZoom();
  renderRouteList();
  renderDetail();
  drawMap();
}

function selectStop(key) {
  state.selectedStopKey = state.selectedStopKey === key ? null : key;
  renderDetail();
  drawMap();
}

function mapPointFromEvent(event) {
  const rect = els.networkMap.getBoundingClientRect();
  return {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  };
}

function bindMapControls() {
  els.zoomOut.addEventListener("click", () => setZoom(state.map.scale / 1.35));
  els.zoomIn.addEventListener("click", () => setZoom(state.map.scale * 1.35));
  els.zoomReset.addEventListener("click", () => resetZoom());

  els.networkMap.addEventListener("wheel", (event) => {
    event.preventDefault();
    const point = mapPointFromEvent(event);
    const factor = event.deltaY < 0 ? 1.16 : 1 / 1.16;
    setZoom(state.map.scale * factor, point.x, point.y);
  }, { passive: false });

  els.networkMap.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) return;
    state.map.dragging = true;
    state.map.dragStartX = event.clientX;
    state.map.dragStartY = event.clientY;
    state.map.dragPanX = state.map.panX;
    state.map.dragPanY = state.map.panY;
    state.map.dragMoved = false;
    els.networkMap.classList.add("dragging");
    els.networkMap.setPointerCapture(event.pointerId);
  });

  els.networkMap.addEventListener("pointermove", (event) => {
    if (!state.map.dragging) return;
    if (Math.abs(event.clientX - state.map.dragStartX) > 3 || Math.abs(event.clientY - state.map.dragStartY) > 3) {
      state.map.dragMoved = true;
    }
    state.map.panX = state.map.dragPanX + event.clientX - state.map.dragStartX;
    state.map.panY = state.map.dragPanY + event.clientY - state.map.dragStartY;
    drawMap();
  });

  els.networkMap.addEventListener("pointerup", (event) => {
    state.map.dragging = false;
    els.networkMap.classList.remove("dragging");
    if (els.networkMap.hasPointerCapture(event.pointerId)) {
      els.networkMap.releasePointerCapture(event.pointerId);
    }
    window.setTimeout(() => {
      state.map.dragMoved = false;
    }, 0);
  });

  els.networkMap.addEventListener("pointercancel", () => {
    state.map.dragging = false;
    els.networkMap.classList.remove("dragging");
  });
}

function bindControls() {
  els.routeSearch.addEventListener("input", (event) => {
    state.search = event.target.value;
    renderRouteList();
  });

  els.sortRoutes.addEventListener("change", (event) => {
    state.sort = event.target.value;
    renderRouteList();
  });

  for (const button of document.querySelectorAll(".segmented button")) {
    button.addEventListener("click", () => {
      document.querySelector(".segmented button.active")?.classList.remove("active");
      button.classList.add("active");
      state.mode = button.dataset.mode;
      renderRouteList();
    });
  }

  bindMapControls();
  window.addEventListener("resize", () => drawMap());
}

async function init() {
  const response = await fetch("data/dashboard-data.json");
  state.data = await response.json();
  bindControls();
  renderSystemMetrics();
  renderRouteList();
  renderDetail();
  drawMap();
  fetchLive();
  window.setInterval(fetchLive, 60_000);
}

init().catch((error) => {
  document.body.innerHTML = `<pre class="empty-state">Unable to load dashboard data: ${error.message}</pre>`;
});
