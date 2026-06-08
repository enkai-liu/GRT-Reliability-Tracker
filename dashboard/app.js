const state = {
  data: null,
  selectedKey: null,
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
  if (onTimeRate >= 0.95) return "#15845f";
  if (onTimeRate >= 0.9) return "#2e86ab";
  if (onTimeRate >= 0.85) return "#d88a1d";
  return "#c43d4b";
}

function routeLabel(route) {
  return route.route_short_name || route.route_id;
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
  const route = state.data.routes.find((item) => item.key === state.selectedKey) || null;

  if (!route) {
    els.selectedMode.textContent = "System";
    els.selectedTitle.textContent = "All lines";
    els.routeBadge.textContent = "--";
    els.routeBadge.style.background = "#1c2528";
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

  const color = reliabilityColor(route.on_time_rate);
  els.selectedMode.textContent = route.transit_mode.toUpperCase();
  els.selectedTitle.textContent = `Route ${routeLabel(route)}`;
  els.routeBadge.textContent = routeLabel(route);
  els.routeBadge.style.background = color;
  els.routeMode.textContent = `${route.transit_mode.toUpperCase()} line`;
  els.routeTitle.textContent = `Route ${routeLabel(route)}`;
  els.routeOnTime.textContent = pct(route.on_time_rate);
  els.routeLate.textContent = pct(route.late_rate);
  els.routeMedian.textContent = seconds(route.median_delay_seconds);
  els.routeP90.textContent = seconds(route.p90_delay_seconds);
  renderHourChart(state.data.routeByHour[route.key] || []);
  renderStopTable(state.data.routeStops[route.key] || []);
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
  const ranked = stops
    .filter((stop) => stop.observations >= 50)
    .sort((a, b) => a.on_time_rate - b.on_time_rate || b.p90_delay_seconds - a.p90_delay_seconds)
    .slice(0, 12);

  els.stopCount.textContent = `${stops.length} stops`;
  els.stopTable.textContent = "";

  if (!ranked.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Select a route to inspect stop-level reliability.";
    els.stopTable.append(empty);
    return;
  }

  for (const stop of ranked) {
    const row = document.createElement("div");
    row.className = "stop-row";
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

function selectRoute(key) {
  state.selectedKey = state.selectedKey === key ? null : key;
  renderRouteList();
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
}

init().catch((error) => {
  document.body.innerHTML = `<pre class="empty-state">Unable to load dashboard data: ${error.message}</pre>`;
});
