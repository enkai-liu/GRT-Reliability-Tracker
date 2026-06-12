/* GRT trip planner core — pure RAPTOR router with reliability scoring.
 *
 * No DOM, fetch, or browser APIs: `prepare(timetable)` builds indexes,
 * `plan(prepared, query)` returns ranked itineraries. Runs unchanged in the
 * browser (window.GRTPlanner) or Node (module.exports), so the same module
 * can back a Cloud Run endpoint later.
 *
 * Reliability model:
 *  - each leg carries the route's historical delay quantiles for its boarding
 *    hour (median + p90, from route_by_hour);
 *  - transfers use the observed make-rate for that route pair at that stop
 *    when we have one, otherwise an estimate from the connection's slack vs
 *    the incoming route's delay distribution;
 *  - itineraries are ranked by p90 arrival plus a missed-connection penalty.
 */
(function (global, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else global.GRTPlanner = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const TRANSFER_SLACK_S = 45;       // same-stop reboard buffer inside RAPTOR
  const DEFAULT_MAX_WALK_M = 600;    // origin/destination access radius
  const DEFAULT_MAX_ROUNDS = 4;      // up to 3 transfers
  const MISSED_PENALTY_S = 900;      // ranking penalty weight for risky transfers
  const LIVE_HORIZON_S = 45 * 60;    // apply live delays to boardings this soon

  function haversineM(lat1, lon1, lat2, lon2) {
    const r = 6371000;
    const p1 = (lat1 * Math.PI) / 180;
    const p2 = (lat2 * Math.PI) / 180;
    const dp = p2 - p1;
    const dl = ((lon2 - lon1) * Math.PI) / 180;
    const a = Math.sin(dp / 2) ** 2 + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) ** 2;
    return 2 * r * Math.asin(Math.sqrt(a));
  }

  function prepare(tt) {
    const n = tt.stops.ids.length;
    const stopPatterns = Array.from({ length: n }, () => []);
    tt.patterns.forEach((p, pi) => {
      p.stops.forEach((s, pos) => stopPatterns[s].push([pi, pos]));
    });
    const foot = Array.from({ length: n }, () => []);
    for (const [a, b, s] of tt.footpaths) {
      foot[a].push([b, s]);
      foot[b].push([a, s]);
    }
    return { tt, n, stopPatterns, foot, walkSpeed: tt.walkSpeedMS || 1.25 };
  }

  function nearestStops(prep, lat, lon, maxM, limit) {
    const { tt } = prep;
    const out = [];
    for (let i = 0; i < prep.n; i += 1) {
      const d = haversineM(lat, lon, tt.stops.lats[i], tt.stops.lons[i]);
      if (d <= maxM) out.push([i, d]);
    }
    out.sort((a, b) => a[1] - b[1]);
    return out.slice(0, limit || 6);
  }

  /* ---------------- RAPTOR core ---------------- */

  function raptor(prep, sources, targets, depart, maxRounds) {
    const { tt, n, stopPatterns, foot } = prep;
    const best = new Float64Array(n).fill(Infinity);
    const tau = [new Float64Array(n).fill(Infinity)];
    const parent = [new Array(n).fill(null)];

    let marked = new Set();
    for (const [s, walkS] of sources) {
      const t = depart + walkS;
      if (t < tau[0][s]) {
        tau[0][s] = t;
        parent[0][s] = { type: "origin", walkS };
        best[s] = t;
        marked.add(s);
      }
    }

    const rounds = [];
    for (let k = 1; k <= maxRounds && marked.size; k += 1) {
      const tk = Float64Array.from(tau[k - 1]);
      const pk = new Array(n).fill(null);
      const improved = new Set();

      const queue = new Map();
      for (const s of marked) {
        for (const [pi, pos] of stopPatterns[s]) {
          const cur = queue.get(pi);
          if (cur === undefined || pos < cur) queue.set(pi, pos);
        }
      }

      for (const [pi, startPos] of queue) {
        const p = tt.patterns[pi];
        const stops = p.stops;
        const trips = p.trips;
        let tripIdx = -1;
        let boardPos = -1;

        for (let pos = startPos; pos < stops.length; pos += 1) {
          const s = stops[pos];

          if (tripIdx >= 0) {
            const arr = trips[tripIdx].times[pos];
            if (arr < tk[s] && arr < best[s]) {
              tk[s] = arr;
              best[s] = arr;
              pk[s] = { type: "transit", pi, tripIdx, boardPos, alightPos: pos };
              improved.add(s);
            }
          }

          const readyAt = tau[k - 1][s];
          if (readyAt < Infinity) {
            const ready = readyAt + (k > 1 ? TRANSFER_SLACK_S : 0);
            let lo = 0;
            let hi = trips.length - 1;
            let found = -1;
            while (lo <= hi) {
              const mid = (lo + hi) >> 1;
              if (trips[mid].times[pos] >= ready) {
                found = mid;
                hi = mid - 1;
              } else {
                lo = mid + 1;
              }
            }
            if (found >= 0 && (tripIdx === -1 || found < tripIdx)) {
              tripIdx = found;
              boardPos = pos;
            }
          }
        }
      }

      const afterFoot = new Set(improved);
      for (const s of improved) {
        for (const [t2, walkS] of foot[s]) {
          const t = tk[s] + walkS;
          if (t < tk[t2] && t < best[t2]) {
            tk[t2] = t;
            best[t2] = t;
            pk[t2] = { type: "walk", from: s, seconds: walkS };
            afterFoot.add(t2);
          }
        }
      }

      tau.push(tk);
      parent.push(pk);
      marked = afterFoot;

      let bestArrive = Infinity;
      let bestStop = -1;
      for (const [s, walkS] of targets) {
        const t = tk[s] + walkS;
        if (t < bestArrive) {
          bestArrive = t;
          bestStop = s;
        }
      }
      rounds.push({ round: k, arrive: bestArrive, stop: bestStop });
    }

    return { tau, parent, rounds };
  }

  function reconstruct(run, round, stop) {
    const steps = [];
    let cur = stop;
    let k = round;
    for (let guard = 0; guard < 64; guard += 1) {
      const rec = run.parent[k][cur];
      if (!rec) {
        if (k === 0) return null;
        k -= 1;
        continue;
      }
      if (rec.type === "origin") {
        steps.push({ type: "origin", stop: cur, walkS: rec.walkS });
        steps.reverse();
        return steps;
      }
      if (rec.type === "walk") {
        steps.push({ type: "walk", from: rec.from, to: cur, seconds: rec.seconds });
        cur = rec.from;
        continue;
      }
      steps.push({ type: "transit", pi: rec.pi, tripIdx: rec.tripIdx, boardPos: rec.boardPos, alightPos: rec.alightPos });
      cur = null; // recomputed below
      const pat = run.patternsRef[rec.pi];
      cur = pat.stops[rec.boardPos];
      k -= 1;
    }
    return null;
  }

  /* ---------------- reliability ---------------- */

  function routeQuantiles(tt, routeKey, seconds) {
    const byHour = tt.reliability.routeHour[routeKey];
    if (!byHour) return [60, 240];
    const hour = String(Math.floor((seconds % 86400) / 3600));
    if (byHour[hour]) return byHour[hour];
    const all = Object.values(byHour);
    if (!all.length) return [60, 240];
    const median = all.reduce((sum, q) => sum + q[0], 0) / all.length;
    const p90 = all.reduce((sum, q) => sum + q[1], 0) / all.length;
    return [Math.round(median), Math.round(p90)];
  }

  function estimateMakeRate(slackS, median) {
    if (slackS <= 0) return 0.05;
    const m = Math.max(median, 30);
    const p = 1 - Math.pow(0.5, slackS / m);
    return Math.min(0.98, Math.max(0.05, p));
  }

  /* ---------------- itinerary assembly ---------------- */

  function buildItinerary(prep, run, round, targetEntry, query, live) {
    const { tt } = prep;
    run.patternsRef = tt.patterns;
    const steps = reconstruct(run, round, targetEntry.stop);
    if (!steps || !steps.some((s) => s.type === "transit")) return null;

    const liveRoutes = {};
    if (live && live.routes) {
      for (const r of live.routes) liveRoutes[r.key] = r.mean_predicted_delay_seconds;
    }

    const legs = [];
    const transfers = [];
    let prevTransit = null;

    for (const step of steps) {
      if (step.type === "origin") {
        if (step.walkS > 0) {
          legs.push({ type: "walk", kind: "access", seconds: step.walkS, toStop: step.stop });
        }
        continue;
      }
      if (step.type === "walk") {
        legs.push({ type: "walk", kind: "between", seconds: step.seconds, fromStop: step.from, toStop: step.to });
        continue;
      }

      const pat = tt.patterns[step.pi];
      const trip = pat.trips[step.tripIdx];
      const boardStop = pat.stops[step.boardPos];
      const alightStop = pat.stops[step.alightPos];
      const boardS = trip.times[step.boardPos];
      const alightS = trip.times[step.alightPos];
      const [median, p90] = routeQuantiles(tt, pat.routeKey, boardS);

      let liveDelta = null;
      if (
        query.nowSeconds !== undefined &&
        liveRoutes[pat.routeKey] !== undefined &&
        boardS - query.nowSeconds < LIVE_HORIZON_S
      ) {
        liveDelta = Math.round(liveRoutes[pat.routeKey]);
      }

      const leg = {
        type: "transit",
        routeKey: pat.routeKey,
        headsign: pat.headsign,
        tripId: trip.id,
        boardStop,
        alightStop,
        boardSeconds: boardS,
        alightSeconds: alightS,
        stopCount: step.alightPos - step.boardPos,
        viaStops: pat.stops.slice(step.boardPos, step.alightPos + 1),
        delayMedianS: median,
        delayP90S: p90,
        liveDelta,
      };

      if (prevTransit) {
        const slack = boardS - prevTransit.alightSeconds;
        const observedKey = `${prevTransit.routeKey}|${pat.routeKey}|${tt.stops.ids[prevTransit.alightStop]}`;
        const observed = tt.reliability.transferRates[observedKey];
        const incomingDelay = prevTransit.liveDelta !== null && prevTransit.liveDelta > prevTransit.delayMedianS
          ? prevTransit.liveDelta
          : prevTransit.delayMedianS;
        const makeRate = observed !== undefined ? observed : estimateMakeRate(slack, incomingDelay);
        transfers.push({
          atStop: boardStop,
          fromRouteKey: prevTransit.routeKey,
          toRouteKey: pat.routeKey,
          slackSeconds: slack,
          makeRate,
          observed: observed !== undefined,
        });
      }

      legs.push(leg);
      prevTransit = leg;
    }

    const transitLegs = legs.filter((l) => l.type === "transit");
    const first = transitLegs[0];
    const last = transitLegs[transitLegs.length - 1];
    const egressWalkS = targetEntry.walkS;
    if (egressWalkS > 0) {
      legs.push({ type: "walk", kind: "egress", seconds: egressWalkS, fromStop: targetEntry.stop });
    }

    const scheduledArrive = last.alightSeconds + egressWalkS;
    const lastLive = last.liveDelta !== null && last.liveDelta > last.delayMedianS ? last.liveDelta : last.delayMedianS;
    const expectedArrive = scheduledArrive + lastLive;
    const p90Arrive = scheduledArrive + last.delayP90S;
    const success = transfers.reduce((p, t) => p * t.makeRate, 1);

    return {
      departSeconds: query.departSeconds,
      leaveSeconds: first.boardSeconds - (legs[0].kind === "access" ? legs[0].seconds : 0),
      firstBoardSeconds: first.boardSeconds,
      scheduledArriveSeconds: scheduledArrive,
      expectedArriveSeconds: Math.round(expectedArrive),
      p90ArriveSeconds: Math.round(p90Arrive),
      transferCount: transfers.length,
      success: Math.round(success * 1000) / 1000,
      legs,
      transfers,
      score: p90Arrive + (1 - success) * MISSED_PENALTY_S,
    };
  }

  function itineraryKey(itin) {
    return itin.legs
      .filter((l) => l.type === "transit")
      .map((l) => `${l.routeKey}@${l.boardSeconds}`)
      .join(">");
  }

  /* ---------------- public API ---------------- */

  function plan(prep, query) {
    const maxWalk = query.maxWalkM || DEFAULT_MAX_WALK_M;
    const maxRounds = query.maxRounds || DEFAULT_MAX_ROUNDS;
    const walkSecs = (m) => Math.round(m / prep.walkSpeed);

    const sources = nearestStops(prep, query.from.lat, query.from.lon, maxWalk, 6)
      .map(([s, d]) => [s, walkSecs(d)]);
    const targetList = nearestStops(prep, query.to.lat, query.to.lon, maxWalk, 6);
    if (!sources.length) return { error: "no-origin-stops", itineraries: [] };
    if (!targetList.length) return { error: "no-destination-stops", itineraries: [] };
    const targets = targetList.map(([s, d]) => [s, walkSecs(d)]);
    const targetWalk = new Map(targets);

    const itineraries = [];
    const seen = new Set();

    const collect = (departSeconds) => {
      const run = raptor(prep, sources, targets, departSeconds, maxRounds);
      let prevArrive = Infinity;
      for (const r of run.rounds) {
        if (r.arrive >= prevArrive - 30 || r.arrive === Infinity) continue;
        prevArrive = r.arrive;
        const itin = buildItinerary(
          prep, run, r.round,
          { stop: r.stop, walkS: targetWalk.get(r.stop) || 0 },
          { ...query, departSeconds },
          query.live,
        );
        if (!itin) continue;
        const key = itineraryKey(itin);
        if (seen.has(key)) continue;
        seen.add(key);
        itineraries.push(itin);
      }
      return itineraries.length ? itineraries[0].firstBoardSeconds : null;
    };

    const firstBoard = collect(query.departSeconds);
    if (firstBoard !== null) {
      collect(firstBoard + 60); // the "next departure" alternatives
    }

    itineraries.sort((a, b) => a.score - b.score);
    return {
      error: null,
      sources: sources.map(([s, w]) => ({ stop: s, walkSeconds: w })),
      targets: targets.map(([s, w]) => ({ stop: s, walkSeconds: w })),
      itineraries: itineraries.slice(0, 4),
    };
  }

  return { prepare, plan, nearestStops, haversineM };
});
