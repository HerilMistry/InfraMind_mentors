

import os
import json
import time
import urllib.parse
import urllib.request
from statistics import median

from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState

# ----------------------------------------------------------------- config ---
PROM_URL = os.getenv("PROMETHEUS_URL")  # e.g. http://prometheus:9090 ; unset = use state only
PROM_QUERIES = {
    "memory_pressure": "1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)",
    "cpu": '100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[2m])))',
    "disk_used_ratio": "1 - (node_filesystem_avail_bytes / node_filesystem_size_bytes)",
}
CEILINGS = {"memory_pressure": 1.0, "cpu": 100.0, "disk_used_ratio": 1.0}
STALE_AFTER_S = 180                                  # metric series with no new sample
HEARTBEAT_MAX_AGE_S = {"pg_backup": 26 * 3600}       # "should have happened by now" jobs
DEPLOY_LOOKBACK_S = 45 * 60

# Dynamic spike detection (MAD-based).
# Values are in the same units as the corresponding metrics:
# cpu = percentage points; memory_pressure/disk_used_ratio = fractions.
MAD_MIN_HISTORY = 3
MAD_Z_THRESHOLD = 3.5
MAD_MIN_DELTA = {
    "cpu": 15.0,               # at least +15 percentage points above baseline
    "memory_pressure": 0.05,   ""
Monitoring subgraph - same 6 nodes, same edges, real logic inside.

Design stance (this is your defence):
  Prometheus alert rules answer: "did ONE metric cross ONE threshold?"
  The monitoring agent answers:  "given ALL signals + recent changes + history,
  is something wrong, how sure am I, how long do we have, and what is the evidence?"
It sits ON TOP of Prometheus (and logs, deploy events, heartbeats, per-host data).
It does not replace Alertmanager; it handles what rules structurally can't:
  1. Silent failures  (nothing red, but something is dead/stale/diverged)
  2. Slow burns       (below threshold today, certain failure in 1h)
  3. Correlation      (deploy + one odd host + log burst = one story, not 3 pages)
  4. Evidence trail   (every verdict carries the reasons, for the postmortem)

No change to CognitiveState needed: all new data lives inside raw_signals
(a plain dict), under raw_signals["analysis"].
""# at least +5 percentage points
    "disk_used_ratio": 0.05,   # at least +5 percentage points
}

# evidence weights combined with noisy-OR: independent weak signals corroborate
WEIGHTS = {
    "Memory Spike Signature": 0.50,
    "CPU Saturation": 0.40,
    "Cpu Spike Anomaly": 0.45,
    "Memory Pressure Spike Anomaly": 0.45,
    "Disk Used Ratio Spike Anomaly": 0.40,
    "Stale Telemetry": 0.60,
    "Missed Heartbeat": 0.60,
    "Error Log Burst": 0.50,
    "Gray Failure": 0.60,
    "Fleet Divergence": 0.55,
    "Change-Correlated Anomaly": 0.30,
    "TTF<30m": 0.80,
    "TTF<120m": 0.40,
}


# ---------------------------------------------------------------- helpers ---
def _analysis(raw):
    a = dict(raw.get("analysis", {}))
    a["evidence"] = list(a.get("evidence", []))
    return a


def _note(a, node, msg):
    a["evidence"].append(f"[{node}] {msg}")


def _prom_range(query, minutes=60, step="60s"):
    end = time.time()
    params = urllib.parse.urlencode(
        {"query": query, "start": end - minutes * 60, "end": end, "step": step}
    )
    with urllib.request.urlopen(f"{PROM_URL}/api/v1/query_range?{params}", timeout=5) as r:
        data = json.load(r)
    res = data["data"]["result"]
    return [(float(t), float(v)) for t, v in res[0]["values"]] if res else []


def _slope_r2(series):
    ""Least-squares slope (units per minute) and r^2 for [(ts, value)].""
    if len(series) < 5:
        return 0.0, 0.0
    t0 = series[0][0]
    xs = [(t - t0) / 60.0 for t, _ in series]
    ys = [v for _, v in series]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return 0.0, 0.0
    return sxy / sxx, (sxy * sxy) / (sxx * syy)


def _mad_spike(current, historical_values, min_delta):
    ""
    Detect an unusually high current value against prior observations.

    Uses the median and Median Absolute Deviation (MAD), which are robust
    to outliers. The current observation must not be included in its own
    baseline.
    Returns (is_spike, baseline_median, upper_bound, modified_z_score).
    ""
    values = [float(v) for v in historical_values if v is not None]
    if len(values) < MAD_MIN_HISTORY:
        return False, None, None, None

    center = median(values)
    mad = median([abs(v - center) for v in values])

    # MAD=0 is common for stable metrics (e.g. 4, 4, 4).
    # The absolute minimum delta provides a usable fallback in that case.
    robust_sigma = 1.4826 * mad
    statistical_delta = MAD_Z_THRESHOLD * robust_sigma
    required_delta = max(statistical_delta, min_delta)
    upper_bound = center + required_delta

    is_spike = float(current) > upper_bound
    modified_z = (
        (0.6745 * (float(current) - center) / mad)
        if mad > 0 else None
    )
    return is_spike, center, upper_bound, modified_z


# ------------------------------------------------------------------ nodes ---
def metric_observer_node(state: CognitiveState):
    ""Collect from every source we have, normalise, and detect MISSING data.""
    print("[Monitoring] Metric Observer collecting data...")
    raw = dict(state.get("raw_signals", {}) or {})
    metrics = dict(raw.get("metrics", {}))
    history = dict(raw.get("history", {}))
    now = raw.get("now", time.time())
    a = _analysis(raw)

    if PROM_URL:  # optional live scrape; logs/events/heartbeats arrive via raw_signals
        for name, q in PROM_QUERIES.items():
            try:
                series = _prom_range(q)
                if series:
                    history[name] = series
                    metrics[name] = series[-1][1]
            except Exception as e:  # a failing scrape is itself a signal
                _note(a, "observer", f"Prometheus query for {name} failed: {e}")

    stale = [k for k, s in history.items() if s and now - s[-1][0] > STALE_AFTER_S]
    late = [
        j for j, max_age in HEARTBEAT_MAX_AGE_S.items()
        if j in raw.get("heartbeats", {}) and now - raw["heartbeats"][j] > max_age
    ]
    a["stale_sources"], a["late_heartbeats"] = stale, late
    _note(a, "observer", f"sources: metrics={list(metrics)} stale={stale} late_jobs={late}")

    # keep everything the caller passed (logs, events, by_host...) - original dropped it
    return {"raw_signals": {**raw, "metrics": metrics, "history": history,
                            "now": now, "status": "normalized", "analysis": a}}


def pattern_detector_node(state: CognitiveState):
    raw = state.get("raw_signals", {}) or {}
    metrics, now = raw.get("metrics", {}), raw.get("now", time.time())
    a = _analysis(raw)
    patterns = []

    # --- original two rules (kept; they are the cheap baseline) ---
    if metrics.get("memory_pressure", 0) > 0.85:
        patterns.append("Memory Spike Signature")
    if metrics.get("cpu", 0) > 90:
        patterns.append("CPU Saturation")

    # --- dynamic spike detection: compare the latest value to its own history ---
    # Exclude the newest history sample because it is normally the current value.
    for name, min_delta in MAD_MIN_DELTA.items():
        current = metrics.get(name)
        series = raw.get("history", {}).get(name, [])
        if current is None or len(series) < MAD_MIN_HISTORY + 1:
            continue

        historical_values = [value for _, value in series[:-1]]
        is_spike, baseline, upper_bound, modified_z = _mad_spike(
            current, historical_values, min_delta
        )

        if is_spike:
            pattern_name = f"{name.replace('_', ' ').title()} Spike Anomaly"
            patterns.append(pattern_name)
            z_text = (
                f", modified_z={modified_z:.2f}"
                if modified_z is not None else ", MAD=0; used minimum-delta fallback"
            )
            _note(
                a,
                "pattern",
                f"{name} spike: current={float(current):.4f}, "
                f"baseline_median={baseline:.4f}, upper_bound={upper_bound:.4f}"
                f"{z_text}",
            )

    # --- silent-failure rules: Prometheus can't alert on what isn't there ---
    if a.get("stale_sources"):
        patterns.append("Stale Telemetry")
        _note(a, "pattern", f"no fresh samples from {a['stale_sources']}")
    if a.get("late_heartbeats"):
        patterns.append("Missed Heartbeat")
        _note(a, "pattern", f"scheduled jobs overdue: {a['late_heartbeats']}")

    # --- log burst vs own baseline (needs logs, not metrics) ---
    errs = [l["ts"] for l in raw.get("logs", []) if l.get("level") == "ERROR"]
    recent = sum(1 for t in errs if now - t <= 300)
    baseline = sum(1 for t in errs if 300 < now - t <= 2100) / 6.0  # per 5 min
    if recent >= 10 and recent >= 3 * max(baseline, 1):
        patterns.append("Error Log Burst")
        _note(a, "pattern", f"{recent} ERROR lines in 5m vs baseline {baseline:.1f}")

    # --- gray failure: infra looks healthy, users are failing ---
    if (metrics.get("synthetic_success_rate", 1.0) < 0.95
            and metrics.get("cpu", 0) < 70 and metrics.get("memory_pressure", 0) < 0.8):
        patterns.append("Gray Failure")
        _note(a, "pattern", f"synthetic probe success={metrics['synthetic_success_rate']:.2f} "
                            f"while cpu/mem are green")

    # --- fleet divergence: the average hides the one bad host ---
    by_host = raw.get("by_host", {})
    rates = {h: v.get("error_rate", 0.0) for h, v in by_host.items()}
    if len(rates) >= 3:
        med = median(rates.values())
        bad = [h for h, r in rates.items() if r > max(0.05, 5 * med)]
        if bad:
            patterns.append("Fleet Divergence")
            _note(a, "pattern", f"hosts {bad} error rate {[round(rates[h],2) for h in bad]} "
                                f"vs fleet median {med:.3f}")

    # --- change correlation: did something ship right before this? ---
    deploys = [e for e in raw.get("events", [])
               if e.get("type") == "deploy" and 0 <= now - e["ts"] <= DEPLOY_LOOKBACK_S]
    if patterns and deploys:
        d = max(deploys, key=lambda e: e["ts"])
        patterns.append("Change-Correlated Anomaly")
        _note(a, "pattern", f"deploy {d.get('service')}@{d.get('version')} "
                            f"{int((now - d['ts'])/60)}m ago precedes anomaly")

    print(f"[Monitoring] Pattern Detector found: {patterns}")
    return {"active_patterns": patterns, "raw_signals": {**raw, "analysis": a}}


def trend_analyzer_node(state: CognitiveState):
    print("[Monitoring] Trend Analyzer computing momentum...")
    raw = state.get("raw_signals", {}) or {}
    a = _analysis(raw)
    trends = {}
    for name, series in raw.get("history", {}).items():
        slope, r2 = _slope_r2(series)
        trends[name] = {"slope_per_min": slope, "r2": r2}
        if name in CEILINGS and slope > 0 and r2 > 0.7:
            _note(a, "trend", f"{name} rising {slope:.4f}/min (r2={r2:.2f}) - steady, not a blip")
    a["trends"] = trends
    return {"raw_signals": {**raw, "analysis": a}}


def forecast_agent_node(state: CognitiveState):
    raw = state.get("raw_signals", {}) or {}
    metrics, a = raw.get("metrics", {}), _analysis(raw)
    patterns = state.get("active_patterns", [])
    ttf = None

    # real forecast: time until each resource hits its ceiling at current slope
    for name, t in a.get("trends", {}).items():
        if name in CEILINGS and t["slope_per_min"] > 0 and t["r2"] > 0.7 and name in metrics:
            eta = (CEILINGS[name] - metrics[name]) / t["slope_per_min"]
            _note(a, "forecast", f"{name} hits ceiling in ~{eta:.0f}m")
            ttf = eta if ttf is None else min(ttf, eta)

    # no history to extrapolate from -> keep the original conservative default
    if ttf is None and "Memory Spike Signature" in patterns:
        ttf = 15.0
        _note(a, "forecast", "no history; conservative default TTF=15m")

    print(f"[Monitoring] Forecast Agent predicts TTF: {ttf}m")
    return {"ttf_estimate": ttf, "raw_signals": {**raw, "analysis": a}}


def risk_assessment_node(state: CognitiveState):
    raw = state.get("raw_signals", {}) or {}
    a = _analysis(raw)
    ttf = state.get("ttf_estimate")
    signals = list(state.get("active_patterns", []))
    if ttf is not None:
        signals.append("TTF<30m" if ttf < 30 else "TTF<120m" if ttf < 120 else "")

    # a forecast breach shortly after a deploy is also change-correlated
    now = raw.get("now", time.time())
    if ttf is not None and ttf < 120 and "Change-Correlated Anomaly" not in signals:
        d = [e for e in raw.get("events", []) if e.get("type") == "deploy"
             and 0 <= now - e["ts"] <= DEPLOY_LOOKBACK_S]
        if d:
            signals.append("Change-Correlated Anomaly")
            _note(a, "risk", f"forecast breach follows deploy {d[-1].get('service')}@{d[-1].get('version')}")

    # noisy-OR: independent corroborating signals compound; one weak signal does not page
    p_ok = 1.0
    for s in signals:
        p_ok *= 1 - WEIGHTS.get(s, 0.0)
    risk_score = max(0.1, round(1 - p_ok, 2))
    _note(a, "risk", f"signals={[s for s in signals if s]} -> risk={risk_score}")
    print(f"[Monitoring] Risk Assessment Agent calculated risk: {risk_score}")
    return {"risk_score": risk_score, "raw_signals": {**raw, "analysis": a}}


def monitoring_consensus_node(state: CognitiveState):
    raw = state.get("raw_signals", {}) or {}
    a = _analysis(raw)
    risk = state.get("risk_score", 0.0)
    ttf = state.get("ttf_estimate")
    incident_declared = risk > 0.8
    escalation_flag = 0.5 < risk <= 0.8
    escalation_reason = None
    if escalation_flag:
        escalation_reason = "Ambiguous risk score, human verification required."

    a["incident_report"] = {
        "verdict": "incident" if incident_declared else "needs_human" if escalation_flag else "ok",
        "risk": risk, "ttf_min": ttf, "patterns": state.get("active_patterns", []),
        "evidence": a["evidence"],   # the trace an SRE reads during the postmortem
    }
    print(f"[Monitoring] Consensus Reached - Incident Declared: {incident_declared}")
    return {
        "incident_declared": incident_declared,
        "escalation_flag": escalation_flag,
        "escalation_reason": escalation_reason,
        "raw_signals": {**raw, "analysis": a},
    }


# ------------------------------------------------------------------ graph ---
def create_monitoring_subgraph():
    workflow = StateGraph(CognitiveState)
    workflow.add_node("metric_observer", metric_observer_node)
    workflow.add_node("pattern_detector", pattern_detector_node)
    workflow.add_node("trend_analyzer", trend_analyzer_node)
    workflow.add_node("forecast_agent", forecast_agent_node)
    workflow.add_node("risk_assessment", risk_assessment_node)
    workflow.add_node("consensus", monitoring_consensus_node)
    workflow.set_entry_point("metric_observer")
    workflow.add_edge("metric_observer", "pattern_detector")
    workflow.add_edge("pattern_detector", "trend_analyzer")
    workflow.add_edge("trend_analyzer", "forecast_agent")
    workflow.add_edge("forecast_agent", "risk_assessment")
    workflow.add_edge("risk_assessment", "consensus")
    workflow.add_edge("consensus", END)
    return workflow.compile()


# ------------------------------------------------- demo scenarios (fixtures) ---
def _scenarios():
    now = time.time()
    leak = [(now - (60 - i) * 60, 0.55 + 0.0038 * i) for i in range(61)]
    errs = [{"ts": now - s, "level": "ERROR"} for s in range(10, 280, 15)]
    return {
        "1. Slow memory leak after deploy (all thresholds green)": {
            "now": now, "metrics": {"memory_pressure": leak[-1][1], "cpu": 41},
            "history": {"memory_pressure": leak},
            "events": [{"ts": now - 40 * 60, "type": "deploy", "service": "checkout", "version": "v142"}],
        },
        "2. One stale host in the fleet, averages look fine (Knight Capital shape)": {
            "now": now, "metrics": {"cpu": 38, "memory_pressure": 0.52, "synthetic_success_rate": 0.93},
            "by_host": {f"h{i}": {"error_rate": 0.002} for i in range(1, 8)} | {"h8": {"error_rate": 0.60}},
            "logs": errs, "events": [{"ts": now - 20 * 60, "type": "deploy", "service": "router", "version": "v9"}],
        },
        "3. Backup job silently dead for 3 days (GitLab 2017 shape)": {
            "now": now, "metrics": {"cpu": 22, "memory_pressure": 0.4},
            "heartbeats": {"pg_backup": now - 72 * 3600},
        },
    }


if __name__ == "__main__":
    app = create_monitoring_subgraph()
    for title, sig in _scenarios().items():
        print(f"\n===== {title}")
        out = app.invoke({"raw_signals": sig})
        rep = out["raw_signals"]["analysis"]["incident_report"]
        print(json.dumps({k: v for k, v in rep.items() if k != "evidence"}, indent=2))
        print("\n".join(rep["evidence"]))