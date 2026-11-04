"""
RCA subgraph - same 7 nodes, same edges, real logic inside.

What an SRE does after an incident is raised, and which node does it:
  observation          "what exactly are we investigating? which service, which window?"
  evidence_collector   pull pod status, k8s events, container logs (incl. --previous), Loki
  log_investigator     cluster the noise, find the first failing pod ("patient zero"), top frame
  causal_graph         order events in time, keep only edges that make physical sense
  hypothesis_generator list plausible causes (leak, traffic, bad deploy, dependency, node, config)
  hypothesis_verifier  test each cause against logs AND metrics; for/against evidence
  consensus            pick winner, name the pod, hand the optimiser structured actions

Output for the optimiser: state["root_cause_declared"] (one readable line) and
state["raw_signals"]["rca_report"] (pods, evidence, ruled-out causes, actions).
CognitiveState is unchanged; everything new lives in raw_signals["rca"] / ["rca_report"].
Optional LLM (Groq): set GROQ_API_KEY. It only (a) triages log lines no regex recognised and
(b) breaks near-ties between the top two hypotheses. It never invents a new cause.
"""
import os
import re
import json
import math
import time
import bisect
import subprocess
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from statistics import median

from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState, LogEvent, Hypothesis

RCA_WINDOW_S = 60 * 60
RCA_LIVE = os.getenv("RCA_LIVE") == "1"          # pull from a real cluster via kubectl
LOKI_URL = os.getenv("LOKI_URL")                  # optional
PRIOR = 0.25


# ---------------------------------------------------------------- helpers ---
def _g(o, k, d=None):
    return o.get(k, d) if isinstance(o, dict) else getattr(o, k, d)


def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _epoch(iso):
    return datetime.fromisoformat(iso.replace("Z", "")).replace(tzinfo=timezone.utc).timestamp()


def _hhmm(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M")


def _sig(x):
    return 1 / (1 + math.exp(-x))


def _logit(p):
    return math.log(p / (1 - p))


def _slope_r2(series):
    """per-minute slope and r^2"""
    if len(series) < 5:
        return 0.0, 0.0
    t0 = series[0][0]
    xs = [(t - t0) / 60.0 for t, _ in series]
    ys = [v for _, v in series]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    if sxx == 0 or syy == 0:
        return 0.0, 0.0
    return sxy / sxx, sxy * sxy / (sxx * syy)


def _pearson(pairs):
    n = len(pairs)
    if n < 10:
        return None
    mx = sum(a for a, _ in pairs) / n
    my = sum(b for _, b in pairs) / n
    sxx = sum((a - mx) ** 2 for a, _ in pairs)
    syy = sum((b - my) ** 2 for _, b in pairs)
    if sxx == 0 or syy == 0:
        return None
    return sum((a - mx) * (b - my) for a, b in pairs) / math.sqrt(sxx * syy)


def _align(a, b, tol=600):
    bt = [t for t, _ in b]
    out = []
    for t, v in a:
        if not b:
            break
        i = bisect.bisect_left(bt, t)
        j = min((j for j in (i - 1, i) if 0 <= j < len(b)), key=lambda j: abs(bt[j] - t))
        if abs(bt[j] - t) <= tol:
            out.append((v, b[j][1]))
    return out


def _segments(series, drop=0.25):
    """split a series at sudden drops (a restart resets memory)"""
    segs, cur = [], []
    for p in series:
        if cur and p[1] < cur[-1][1] * (1 - drop):
            segs.append(cur)
            cur = []
        cur.append(p)
    if cur:
        segs.append(cur)
    return segs


def _onset(series, factor=None, floor=None):
    """first time (after the baseline half) the series exceeds baseline*factor or an absolute floor"""
    if len(series) < 10:
        return None
    half = len(series) // 2
    base = median(v for _, v in series[:half])
    for t, v in series[half:]:
        if (factor and v > base * factor) or (floor is not None and v >= floor):
            return t
    return None


def _llm_json(system, user):
    key = os.getenv("GROQ_API_KEY")
    if not key:
        return None
    body = {
        "model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": system + " Respond with a single JSON object."},
                     {"role": "user", "content": user}],
    }
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions", data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json",
                 "User-Agent": "rca-agent/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(json.load(r)["choices"][0]["message"]["content"])
    except Exception:
        return None


# --------------------------------------------- live collection (optional) ---
def _kubectl(args):
    out = subprocess.run(["kubectl", *args, "-o", "json"], capture_output=True, text=True,
                         timeout=20, check=True).stdout
    return json.loads(out)


def _collect_live(ns, now):
    pods, events, logs = [], [], {}
    for p in _kubectl(["get", "pods", "-n", ns])["items"]:
        cs = (p["status"].get("containerStatuses") or [{}])[0]
        term = (cs.get("lastState") or {}).get("terminated")
        meta = p["metadata"]
        pods.append({
            "name": meta["name"], "app": meta.get("labels", {}).get("app", meta["name"]),
            "node": p["spec"].get("nodeName"), "ready": bool(cs.get("ready")),
            "restarts": cs.get("restartCount", 0),
            "version": meta.get("labels", {}).get("version", (cs.get("image", ":?").split(":")[-1])),
            "limits": p["spec"]["containers"][0].get("resources", {}).get("limits", {}),
            "last_terminated": {"reason": term["reason"], "exitCode": term.get("exitCode"),
                                "ts": _epoch(term["finishedAt"])} if term else None})
    for e in _kubectl(["get", "events", "-n", ns])["items"]:
        ts = e.get("lastTimestamp") or e.get("eventTime")
        if ts:
            events.append({"ts": _epoch(ts), "reason": e["reason"], "type": e.get("type"),
                           "object": f'{e["involvedObject"]["kind"].lower()}/{e["involvedObject"]["name"]}',
                           "message": e.get("message", "")})
    for p in pods:
        for prev in ([True, False] if p["restarts"] else [False]):
            args = ["logs", p["name"], "-n", ns, "--tail=300", "--timestamps"] + (["--previous"] if prev else [])
            r = subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=20)
            for ln in r.stdout.splitlines():
                tok, _, rest = ln.partition(" ")
                try:
                    logs.setdefault(p["name"], []).append({"ts": _epoch(tok[:19]), "line": rest})
                except ValueError:
                    pass
    if LOKI_URL:
        q = urllib.parse.urlencode({"query": f'{{namespace="{ns}"}} |~ "(?i)error|oom|exception|refused|timeout"',
                                    "start": int((now - RCA_WINDOW_S) * 1e9), "end": int(now * 1e9), "limit": 1000})
        try:
            with urllib.request.urlopen(f"{LOKI_URL}/loki/api/v1/query_range?{q}", timeout=10) as r:
                for st in json.load(r)["data"]["result"]:
                    pod = st["stream"].get("pod", "unknown")
                    for t, line in st["values"]:
                        logs.setdefault(pod, []).append({"ts": int(t) / 1e9, "line": line})
        except Exception:
            pass
    return {"namespace": ns, "pods": pods, "events": events, "logs": logs}


# ------------------------------------------------------------ log signatures ---
SIGS = [
    ("oom", re.compile(r"OOMKill|Out of memory|oom-kill|Killed process|MemoryError|OutOfMemoryError|Cannot allocate memory", re.I)),
    ("evict", re.compile(r"Evicted|DiskPressure|MemoryPressure|NodeNotReady|node was low on resource", re.I)),
    ("config", re.compile(r"CreateContainerConfigError|(secret|configmap) \S+ not found|missing required env|invalid configuration|KeyError: '[A-Z_]+'", re.I)),
    ("imagepull", re.compile(r"ErrImagePull|ImagePullBackOff", re.I)),
    ("dep_conn", re.compile(r"connection refused|ECONNREFUSED|no route to host|Name or service not known|could not connect|connecting to \S+:\d+", re.I)),
    ("timeout", re.compile(r"timed? ?out|deadline exceeded", re.I)),
    ("probe", re.compile(r"(Readiness|Liveness|Startup) probe failed", re.I)),
    ("exception", re.compile(r"Traceback|Exception\b|panic:|FATAL|\bError\b|\bERROR\b")),
]
KIND = {"oom": "OOMKilled", "evict": "Evicted", "config": "Config Error", "imagepull": "Config Error",
        "dep_conn": "Dependency Error", "timeout": "Timeout", "probe": "Probe Failure",
        "exception": "Exception Burst"}
LEVEL_ERR = re.compile(r"\b(ERROR|FATAL|CRITICAL)\b|Traceback|^\s+File \"|panic:")
LEVEL_WARN = re.compile(r"\bWARN(ING)?\b")
TARGET = re.compile(r"(?:connecting to|dial tcp|connect to|to)\s+([A-Za-z0-9_.\-]+):(\d{2,5})")
FRAME = re.compile(r'File "([^"]+)", line (\d+), in (\S+)')
EXC = re.compile(r"^([\w.]+(?:Error|Exception))\b")

# who can cause what (physical plausibility) - an edge also needs correct time order
ONTOLOGY = {
    "Deploy": ["Memory Spike", "Exception Burst", "Config Error"],
    "Traffic Increase": ["Memory Spike", "CPU Saturation", "Timeout"],
    "Memory Spike": ["OOMKilled", "Evicted"],
    "CPU Saturation": ["Timeout", "Probe Failure"],
    "Node Pressure": ["Evicted"],
    "OOMKilled": ["Pod Restart"],
    "Evicted": ["Pod Restart"],
    "Config Error": ["Pod Restart"],
    "Pod Restart": ["Dependency Error", "Service Disruption"],
    "Dependency Error": ["Probe Failure", "Service Disruption"],
    "Probe Failure": ["Service Disruption"],
    "Exception Burst": ["Service Disruption"],
    "Timeout": ["Service Disruption"],
}


def _level(line):
    return "ERROR" if LEVEL_ERR.search(line) else "WARN" if LEVEL_WARN.search(line) else "INFO"


def _template(text):
    return re.sub(r"0x[0-9a-f]+|[0-9a-f]{8,}|\d+", "N", text)[:160]


def _parse_ev(ev):
    m = re.match(r"^pod=(\S+) \| (.*)$", _g(ev, "message", ""), re.S)
    if not m:
        return None
    return {"ts": _epoch(_g(ev, "timestamp")), "level": _g(ev, "level"), "pod": m.group(1), "text": m.group(2)}


# ------------------------------------------------------------------ nodes ---
def observation_node(state: CognitiveState):
    """Scope the investigation from what monitoring handed over."""
    print("[RCA] Observation Agent scoping incident...")
    raw = dict(state.get("raw_signals", {}) or {})
    now = raw.get("now") or time.time()
    k8s = raw.get("k8s", {})
    scope = {"service": raw.get("service"), "namespace": raw.get("namespace") or k8s.get("namespace", "default"),
             "incident_ts": now, "window_start": now - RCA_WINDOW_S, "window_end": now,
             "symptoms": list(state.get("active_patterns", [])), "monitoring_risk": state.get("risk_score"),
             "monitoring_ttf_min": state.get("ttf_estimate")}
    carried = (raw.get("analysis") or {}).get("evidence", [])
    rca = {"scope": scope, "trail": [f"[observation] scope={scope['service']}@{scope['namespace']} "
                                     f"window=60m symptoms={scope['symptoms']}"] + carried}
    return {"raw_signals": {**raw, "now": now, "rca": rca}}


def evidence_collector_node(state: CognitiveState):
    """Pods + k8s events + logs -> LogEvents (the evidence pool)."""
    print("[RCA] Evidence Collector querying Loki/Kubernetes...")
    raw = dict(state.get("raw_signals", {}) or {})
    rca = dict(raw["rca"])
    sc = rca["scope"]
    k8s = raw.get("k8s")
    if k8s is None and RCA_LIVE:
        try:
            k8s = _collect_live(sc["namespace"], sc["incident_ts"])
        except Exception as e:
            rca["trail"].append(f"[collector] live k8s collection failed: {e}")
    k8s = k8s or {"pods": [], "events": [], "logs": {}}

    ev = []
    # a restarted pod's last words (logs just before it died) matter even if the crash predates the window
    death = {p["name"]: p["last_terminated"]["ts"] for p in k8s.get("pods", []) if p.get("last_terminated")}
    for pod, lines in k8s.get("logs", {}).items():
        for l in sorted(lines, key=lambda x: x["ts"])[-300:]:
            in_win = sc["window_start"] <= l["ts"] <= sc["window_end"]
            pre_death = pod in death and death[pod] - 300 <= l["ts"] <= death[pod] + 5
            if in_win or pre_death:
                ev.append(LogEvent(timestamp=_iso(l["ts"]), level=_level(l["line"]),
                                   message=f"pod={pod} | {l['line']}"))
    for e in k8s.get("events", []):
        if sc["window_start"] <= e["ts"] <= sc["window_end"]:
            lvl = "ERROR" if e["reason"] in ("OOMKilling", "Evicted", "Failed") else "WARN" if e.get("type") == "Warning" else "INFO"
            ev.append(LogEvent(timestamp=_iso(e["ts"]), level=lvl,
                               message=f"pod={e['object']} | [k8s-event] {e['reason']}: {e['message']}"))
    for p in k8s.get("pods", []):  # pod state is evidence even if the kill was before the window
        lt = p.get("last_terminated")
        if lt:
            ev.append(LogEvent(timestamp=_iso(lt["ts"]), level="ERROR",
                               message=f"pod={p['name']} | [k8s-status] last terminated reason={lt['reason']} "
                                       f"exit={lt.get('exitCode')} restarts={p['restarts']} "
                                       f"limit={p.get('limits', {}).get('memory', '?')} version={p.get('version')}"))
    ev.sort(key=lambda e: _g(e, "timestamp"))
    rca["pods"], rca["k8s_events"] = k8s.get("pods", []), k8s.get("events", [])
    rca["trail"].append(f"[collector] {len(k8s.get('pods', []))} pods, {len(k8s.get('events', []))} k8s events, "
                        f"{len(ev)} evidence lines")
    return {"evidence_pool": ev, "raw_signals": {**raw, "rca": rca}}


def log_investigator_node(state: CognitiveState):
    """Cluster, classify, find patient zero, extract the top stack frame."""
    print("[RCA] Log Investigator analyzing stack traces...")
    raw = dict(state.get("raw_signals", {}) or {})
    rca = dict(raw["rca"])
    entries = [e for e in (_parse_ev(x) for x in state.get("evidence_pool", [])) if e]
    by_pod = defaultdict(lambda: {"errors": 0, "first_error_ts": None, "categories": Counter(),
                                  "templates": Counter(), "frame": None, "exception": None})
    dep_targets, unclassified, hits = Counter(), Counter(), []
    last_frame = {}
    for e in entries:
        text, pod = e["text"], e["pod"]
        fm = FRAME.search(text)
        if fm:
            last_frame[pod] = f"{fm.group(1)}:{fm.group(2)} in {fm.group(3)}"
        cat = next((c for c, rx in SIGS if rx.search(text)), None)
        if cat is None:
            if e["level"] == "ERROR":
                unclassified[_template(text)] += 1
            continue
        b = by_pod[pod]
        b["categories"][cat] += 1
        b["templates"][_template(text)] += 1
        if e["level"] in ("ERROR", "WARN") or cat in ("oom", "evict"):
            b["errors"] += 1
            if b["first_error_ts"] is None or e["ts"] < b["first_error_ts"]:
                b["first_error_ts"] = e["ts"]
        hits.append((e["ts"], KIND[cat], pod))
        if cat == "dep_conn":
            tm = TARGET.search(text)
            if tm:
                dep_targets[f"{tm.group(1)}:{tm.group(2)}"] += 1
        xm = EXC.match(text.split(":")[0].strip() + ":") if cat in ("exception", "oom") else None
        if xm and pod in last_frame:
            by_pod[pod]["exception"], by_pod[pod]["frame"] = xm.group(1), last_frame[pod]

    # pod restarts are timeline events too
    for p in rca.get("pods", []):
        if p["restarts"] and p.get("last_terminated"):
            hits.append((p["last_terminated"]["ts"], "Pod Restart", p["name"]))

    first = {}
    for ts, kind, pod in sorted(hits):
        if kind not in first:
            first[kind] = {"ts": ts, "pod": pod, "count": 0}
        first[kind]["count"] += 1
    first = {k: v for k, v in first.items() if k != "Exception Burst" or v["count"] >= 3}

    errs = [(b["first_error_ts"], p) for p, b in by_pod.items() if b["first_error_ts"] is not None]
    pz = min(errs) if errs else None
    triage = None
    if unclassified:
        triage = _llm_json("You triage Kubernetes container error log templates. For each template return "
                           "a category from: memory, dependency, config, code_bug, network, unknown.",
                           json.dumps({"templates": [t for t, _ in unclassified.most_common(5)]}))
    rca["findings"] = {
        "by_pod": {p: {**b, "categories": dict(b["categories"]), "templates": b["templates"].most_common(3)}
                   for p, b in by_pod.items()},
        "dep_targets": dict(dep_targets), "log_events": first,
        "patient_zero": {"pod": pz[1], "ts": pz[0]} if pz else None,
        "unclassified": unclassified.most_common(5), "llm_triage": triage}
    if pz:
        rca["trail"].append(f"[investigator] patient zero = {pz[1]} at {_hhmm(pz[0])}")
    for p, b in by_pod.items():
        if b["frame"]:
            rca["trail"].append(f"[investigator] {p}: {b['exception']} at {b['frame']}")
    return {"raw_signals": {**raw, "rca": rca}}


def causal_graph_builder_node(state: CognitiveState):
    """Merge log events with metric onsets; link only plausible, correctly-ordered pairs."""
    print("[RCA] Causal Graph Builder connecting temporal events...")
    raw = dict(state.get("raw_signals", {}) or {})
    rca = dict(raw["rca"])
    hist, now = raw.get("history", {}), raw["now"]
    tl = {k: {"ts": v["ts"], "pod": v["pod"], "detail": f"{v['count']}x"} for k, v in rca["findings"]["log_events"].items()}

    rps, mem = hist.get("rps", []), hist.get("memory_pressure", [])
    t_on = _onset(rps, factor=1.5)
    if t_on:
        half = median(v for _, v in rps[:len(rps) // 2])
        peak = max(v for t, v in rps if t >= t_on)
        tl["Traffic Increase"] = {"ts": t_on, "pod": None, "detail": f"{peak / half:.1f}x baseline"}
    recent_mem = [p for p in mem if p[0] >= now - 6 * 3600]
    m_on = next((t for t, v in recent_mem if v >= 0.85), None)
    if m_on:
        tl["Memory Spike"] = {"ts": m_on, "pod": None, "detail": "memory >= 85%"}
    cpu_on = next((t for t, v in hist.get("cpu", []) if v > 90), None)
    if cpu_on:
        tl["CPU Saturation"] = {"ts": cpu_on, "pod": None, "detail": "cpu > 90%"}
    deploys = [e for e in raw.get("events", []) if e.get("type") == "deploy" and 0 <= now - e["ts"] <= 72 * 3600]
    if deploys:
        d = max(deploys, key=lambda e: e["ts"])
        tl["Deploy"] = {"ts": d["ts"], "pod": None, "detail": f"{d.get('service')}@{d.get('version')}"}
    evicts = [e for e in rca["k8s_events"] if "NodeNotReady" in e["reason"] or "Pressure" in e["reason"]]
    if evicts:
        tl["Node Pressure"] = {"ts": evicts[0]["ts"], "pod": evicts[0]["object"], "detail": evicts[0]["reason"]}
    if tl:
        tl["Service Disruption"] = {"ts": now, "pod": None, "detail": "incident raised"}

    graph = {}
    for cause, effects in ONTOLOGY.items():
        if cause in tl:
            ok = [e for e in effects if e in tl and tl[cause]["ts"] <= tl[e]["ts"] + 60]
            if ok:
                graph[cause] = ok
    effects_all = {e for v in graph.values() for e in v}
    rca["timeline"] = tl
    rca["causal_roots"] = sorted((k for k in graph if k not in effects_all), key=lambda k: tl[k]["ts"])
    chain = sorted(tl, key=lambda k: tl[k]["ts"])
    rca["trail"].append("[causal] order: " + " -> ".join(f"{k}@{_hhmm(tl[k]['ts'])}" for k in chain))
    return {"causal_graph": graph, "raw_signals": {**raw, "rca": rca}}


# ----------------------------------------------- hypotheses & verification ---
def _svc(c):
    return c["scope"]["service"]


def _oom_pods(c):
    out = [p for p in c["pods"] if (p.get("last_terminated") or {}).get("reason") == "OOMKilled"]
    return sorted(out, key=lambda p: p["last_terminated"]["ts"])


def _deploys(c):
    return [e for e in c["raw"].get("events", []) if e.get("type") == "deploy"]


def _v_leak(c):
    chk, mem, rps = [], c["hist"].get("memory_pressure", []), c["hist"].get("rps", [])
    oom = _oom_pods(c)
    blamed = [p["name"] for p in oom if p["app"] == _svc(c)]
    if not oom and "Memory Spike" not in c["tl"]:
        chk.append((-2.0, "no OOMKilled or memory pressure in the window"))
    elif oom and _svc(c) and not blamed:
        chk.append((-1.0, f"OOM happened in {oom[0]['app']}, not in the alerting service {_svc(c)}"))
    segs = [s for s in _segments(mem) if len(s) >= 6]
    grow = [s for s in segs if (lambda sr: sr[0] > 0 and sr[1] > 0.8)(_slope_r2(s))]
    span_h = (mem[-1][0] - mem[0][0]) / 3600 if len(mem) > 1 else 0
    if len(grow) >= 2:
        sl = [_slope_r2(s)[0] * 60 for s in grow]
        chk.append((1.5, f"sawtooth: memory climbs ~{sum(sl) / len(sl):.2f}/h in {len(grow)} cycles, then OOM resets it"))
    elif len(segs) == 1 and grow and span_h >= 3 and _slope_r2(grow[0])[1] > 0.9:
        chk.append((1.0, f"memory rising steadily for {span_h:.0f}h (r2={_slope_r2(grow[0])[1]:.2f})"))
    else:
        chk.append((-0.5, "no sustained time-based memory growth"))
    r = _pearson(_align(mem, rps))
    if r is not None:
        if abs(r) < 0.3 and grow:
            chk.append((1.0, f"memory is independent of request rate (r={r:.2f})"))
        elif r > 0.8:
            chk.append((-1.5, f"memory tracks request rate (r={r:.2f}) - load, not leak"))
    for d in _deploys(c):
        before = [p for p in mem if p[0] < d["ts"]]
        after_grow = [s for s in grow if s[0][0] >= d["ts"] - 600]
        if len(before) >= 5 and abs(_slope_r2(before)[0] * 60) < 0.01 and after_grow:
            chk.append((1.0, f"flat memory before deploy {d.get('version')}, sawtooth after it "
                             f"({(c['now'] - d['ts']) / 3600:.0f}h ago)"))
            break
    fr = [(p, c["findings"]["by_pod"].get(p, {})) for p in blamed]
    frame = next((f"{b['exception']} at {b['frame']}" for _, b in fr if b.get("frame")), None)
    if frame:
        chk.append((0.5, f"allocation site in stack trace: {frame}"))
    lim = oom[0].get("limits", {}).get("memory", "?") if oom else "?"
    head = f"{blamed[0] if blamed else '?'} OOMKilled repeatedly (limit {lim})" + (f"; {frame}" if frame else "")
    return chk, blamed, head


def _v_traffic(c):
    chk, tl, mem = [], c["tl"], c["hist"].get("memory_pressure", [])
    oom = _oom_pods(c)
    blamed = [p["name"] for p in oom if p["app"] == _svc(c)]
    if not oom and "Memory Spike" not in tl:
        chk.append((-2.0, "no OOMKilled or memory pressure in the window"))
    elif oom and _svc(c) and not blamed:
        chk.append((-1.0, f"OOM happened in {oom[0]['app']}, not in the alerting service"))
    tr = tl.get("Traffic Increase")
    if not tr:
        chk.append((-2.0, "request rate did not increase"))
        return chk, blamed, "no traffic anomaly"
    anchor = (tl.get("Memory Spike") or tl.get("OOMKilled") or {}).get("ts")
    if anchor and tr["ts"] <= anchor + 60:
        chk.append((1.5, f"request rate rose {tr['detail']} at {_hhmm(tr['ts'])}, "
                         f"{(anchor - tr['ts']) / 60:.0f}m BEFORE memory pressure/OOM"))
    r = _pearson(_align(mem, c["hist"].get("rps", [])))
    if r is not None and r > 0.8:
        chk.append((1.5, f"memory follows request rate (r={r:.2f})"))
    pre = [p for p in mem if p[0] < tr["ts"]]
    if len(pre) >= 20 and abs(_slope_r2(pre)[0] * 60) < 0.01:
        chk.append((0.5, f"memory was flat for {(pre[-1][0] - pre[0][0]) / 3600:.1f}h before the surge"))
    if len([s for s in _segments(mem) if len(s) > 5]) <= 1:
        chk.append((0.5, "no earlier restart cycles (not a slow leak)"))
    hpa = next((e for e in c["events"] if "maximum replica" in e["message"] or "maxReplicas" in e["message"]), None)
    if hpa:
        chk.append((0.5, "autoscaler pinned at max replicas, could not absorb the load"))
    lim = oom[0].get("limits", {}).get("memory", "?") if oom else "?"
    head = (f"{blamed[0] if blamed else '?'} OOMKilled first (limit {lim}); request rate {tr['detail']} "
            f"from {_hhmm(tr['ts'])}" + ("; HPA at max" if hpa else ""))
    return chk, blamed, head


def _v_deploy(c):
    chk, pz = [], c["findings"]["patient_zero"]
    deps = _deploys(c)
    near = [d for d in deps if pz and 0 <= pz["ts"] - d["ts"] <= 3600]
    if not near:
        return [(-1.0, "no deploy in the 60m before the first error")], [], "no related deploy"
    d = max(near, key=lambda e: e["ts"])
    chk.append((1.5, f"deploy {d.get('service')}@{d.get('version')} landed {(pz['ts'] - d['ts']) / 60:.0f}m before first error"))
    erroring = {p for p, b in c["findings"]["by_pod"].items() if b["errors"]}
    new = [p["name"] for p in c["pods"] if p.get("version") == d.get("version")]
    old_ok = [p["name"] for p in c["pods"] if p.get("version") != d.get("version") and p["name"] not in erroring and p["app"] == _svc(c)]
    if new and erroring & set(new) and old_ok:
        chk.append((1.0, f"errors only on pods running {d.get('version')}; older pods are clean"))
    elif new and erroring - set(new):
        chk.append((-1.0, "older-version pods fail too"))
    exc = next(((p, b) for p, b in c["findings"]["by_pod"].items() if b.get("frame")), None)
    if exc:
        chk.append((0.5, f"new exception {exc[1]['exception']} at {exc[1]['frame']}"))
    blamed = sorted(erroring & set(new))
    return chk, blamed, f"deploy {d.get('version')} introduced failures on {len(blamed)} pod(s)"


def _v_dependency(c):
    chk, tg = [], c["findings"]["dep_targets"]
    if not tg:
        return [(-2.0, "no connection errors toward any dependency")], [], "none"
    target, n = max(tg.items(), key=lambda kv: kv[1])
    host = target.split(":")[0].split(".")[0]
    chk.append((2.0, f"{n} connection errors toward {target}"))
    dep = next((p for p in c["pods"] if p["app"] == host or p["name"].startswith(host)), None)
    blamed = []
    if dep:
        lt = dep.get("last_terminated") or {}
        bad = (not dep["ready"]) or dep["restarts"] > 0
        if bad:
            chk.append((1.0, f"{dep['name']} is {'NotReady' if not dep['ready'] else 'restarting'}: "
                             f"{dep['restarts']} restarts, last={lt.get('reason', '?')} (limit {dep.get('limits', {}).get('memory', '?')})"))
            blamed = [dep["name"]]
        first_caller = c["findings"]["patient_zero"]
        if lt and first_caller and lt["ts"] <= first_caller["ts"] + 60:
            chk.append((0.5, f"{dep['name']} died {_hhmm(lt['ts'])}, callers started failing {_hhmm(first_caller['ts'])}"))
    else:
        chk.append((0.5, f"{target} is not a pod in this namespace (external dependency)"))
        blamed = [target]
    m = c["metrics"]
    if m.get("cpu", 0) < 70 and m.get("memory_pressure", 0) < 0.8:
        chk.append((0.5, f"callers' own CPU/memory are healthy ({m.get('cpu')}%, {m.get('memory_pressure')}) - they are victims"))
    head = f"dependency {blamed[0] if blamed else target} down; {_svc(c)} pods fail with connection errors"
    return chk, blamed, head


def _v_node(c):
    ev = [e for e in c["events"] if e["reason"] in ("Evicted", "NodeNotReady") or "Pressure" in e["reason"]]
    cat = [p for p, b in c["findings"]["by_pod"].items() if b["categories"].get("evict")]
    if not ev and not cat:
        return [(-2.0, "no eviction / node pressure events")], [], "none"
    nodes = sorted({p.get("node") for p in c["pods"] if p["name"] in cat or p["name"] in {e["object"].split("/")[-1] for e in ev}} - {None})
    return [(2.5, f"{len(ev) or len(cat)} eviction/pressure events on node(s) {nodes}")], nodes, f"node pressure on {nodes}"


def _v_config(c):
    cat = [p for p, b in c["findings"]["by_pod"].items() if b["categories"].get("config")]
    if not cat:
        return [(-2.0, "no config/secret/image errors")], [], "none"
    chk = [(2.5, f"config errors on {cat}")]
    pz = c["findings"]["patient_zero"]
    if any(pz and 0 <= pz["ts"] - d["ts"] <= 3600 for d in _deploys(c)):
        chk.append((0.5, "started right after a deploy"))
    return chk, cat, f"misconfiguration on {cat}"


CATALOG = {
    "H1": ("Memory leak in {svc}", _v_leak, ["Gradual memory increase", "Sawtooth after restarts", "Memory independent of traffic"]),
    "H2": ("Traffic Spike causing OOM", _v_traffic, ["Sudden ingress request spike", "Memory tracks request rate", "HPA at max replicas"]),
    "H3": ("Bad deploy / code regression in {svc}", _v_deploy, ["Errors start minutes after deploy", "Only new-version pods fail", "New exception type"]),
    "H4": ("Downstream dependency failure", _v_dependency, ["Connection refused/timeouts to one host", "Callers' CPU/memory healthy", "Dependency pod not Ready"]),
    "H5": ("Node pressure / eviction", _v_node, ["Evicted events", "Multiple pods on one node fail"]),
    "H6": ("Configuration / secret / image error", _v_config, ["CreateContainerConfigError", "Missing secret or env"]),
}


def hypothesis_generator_node(state: CognitiveState):
    print("[RCA] Hypothesis Generator proposing causes...")
    raw = dict(state.get("raw_signals", {}) or {})
    rca = dict(raw["rca"])
    tl, svc = rca["timeline"], rca["scope"]["service"] or "service"
    recent_deploy = "Deploy" in tl and raw["now"] - tl["Deploy"]["ts"] <= 3 * 3600
    trig = {"H1": "OOMKilled" in tl or "Memory Spike" in tl, "H2": "OOMKilled" in tl or "Memory Spike" in tl,
            "H3": recent_deploy or "Exception Burst" in tl, "H4": "Dependency Error" in tl,
            "H5": "Evicted" in tl or "Node Pressure" in tl, "H6": "Config Error" in tl}
    hyps = [Hypothesis(id=h, description=CATALOG[h][0].format(svc=svc), confidence=PRIOR,
                       expected_symptoms=CATALOG[h][2]) for h, on in trig.items() if on]
    rca["trail"].append(f"[hypotheses] generated {[h.id for h in hyps]} (others had no trigger evidence)")
    return {"active_hypotheses": hyps, "raw_signals": {**raw, "rca": rca}}


def hypothesis_verifier_node(state: CognitiveState):
    print("[RCA] Hypothesis Verifier cross-checking evidence...")
    raw = dict(state.get("raw_signals", {}) or {})
    rca = dict(raw["rca"])
    c = {"raw": raw, "scope": rca["scope"], "pods": rca["pods"], "events": rca["k8s_events"],
         "findings": rca["findings"], "tl": rca["timeline"], "hist": raw.get("history", {}),
         "metrics": raw.get("metrics", {}), "now": raw["now"]}
    hyps, ver = state.get("active_hypotheses", []), {}
    for h in hyps:
        checks, blamed, head = CATALOG[h.id][1](c)
        h.confidence = round(_sig(_logit(PRIOR) + sum(d for d, _ in checks)), 3)
        ver[h.id] = {"checks": checks, "blamed": blamed, "headline": head}
        print(f"[RCA]   {h.id} {h.description}: {h.confidence}")
    rca["verification"] = ver
    return {"active_hypotheses": hyps, "raw_signals": {**raw, "rca": rca}}


# -------------------------------------------------------------- consensus ---
def _pod_card(c_pods, name):
    p = next((p for p in c_pods if p["name"] == name), None)
    if not p:
        return {"name": name}
    lt = p.get("last_terminated") or {}
    return {"name": name, "app": p["app"], "node": p.get("node"), "version": p.get("version"),
            "ready": p["ready"], "restarts": p["restarts"], "last_terminated": lt.get("reason"),
            "memory_limit": p.get("limits", {}).get("memory")}


def _actions(hid, svc, blamed, pods, tl):
    app = next((p["app"] for p in pods if p["name"] in blamed), svc)
    lim = next((p.get("limits", {}).get("memory") for p in pods if p["name"] in blamed), None)
    return {
        "H1": [{"action": "rollback_deployment", "target": app, "rationale": "leak aligned with latest deploy" if "Deploy" in tl else "stop recurrence"},
               {"action": "rolling_restart", "target": app, "rationale": "mitigate until fixed"},
               {"action": "raise_memory_limit_temporarily", "target": app, "params": {"current": lim}}],
        "H2": [{"action": "scale_out", "target": app, "rationale": "absorb load"},
               {"action": "raise_hpa_max_replicas", "target": app},
               {"action": "add_rate_limit_or_shed_load", "target": app},
               {"action": "review_memory_limit", "target": app, "params": {"current": lim}, "rationale": "secondary"}],
        "H3": [{"action": "rollback_deployment", "target": app, "rationale": "regression after deploy"}],
        "H4": [{"action": "restore_dependency", "target": blamed[0] if blamed else "?", "rationale": "root of the outage"},
               {"action": "enable_circuit_breaker", "target": svc, "rationale": "callers should degrade, not fail"},
               {"action": "investigate_dependency_resource_limits", "target": blamed[0] if blamed else "?", "params": {"current": lim}}],
        "H5": [{"action": "cordon_and_drain_node", "target": blamed[0] if blamed else "?"}, {"action": "add_node_capacity"}],
        "H6": [{"action": "fix_config_or_secret", "target": blamed[0] if blamed else svc}, {"action": "rollback_config"}],
    }.get(hid, [])


def rca_consensus_node(state: CognitiveState):
    hyps = state.get("active_hypotheses", [])
    raw = dict(state.get("raw_signals", {}) or {})
    rca = dict(raw.get("rca", {}))
    if not hyps:
        return {"root_cause_declared": None, "escalation_flag": True,
                "escalation_reason": "No hypothesis triggered by the collected evidence.",
                "raw_signals": {**raw, "rca_report": {"trail": rca.get("trail", [])}}}
    ranked = sorted(hyps, key=lambda x: x.confidence, reverse=True)
    top = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None
    margin = top.confidence - (second.confidence if second else 0.0)
    ver, llm_note = rca["verification"], None
    if second and margin < 0.15:
        ans = _llm_json("You are an SRE adjudicating between two root-cause hypotheses using ONLY the evidence given. "
                        "Return {\"winner\": \"<id>\", \"reason\": \"...\"}.",
                        json.dumps({h.id: {"desc": h.description, "evidence": [t for _, t in ver[h.id]["checks"]]}
                                    for h in (top, second)}))
        if ans and ans.get("winner") in (top.id, second.id):
            llm_note = ans
            if ans["winner"] == second.id:
                top, second = second, top
    h = ver[top.id]
    root = f"{top.description} | {h['headline']}"
    print(f"[RCA] Consensus Reached - Root Cause: {root} (Confidence: {top.confidence})")
    escalate = top.confidence < 0.7 or margin < 0.15
    reason = None
    if escalate:
        reason = ("Low confidence in root cause diagnosis." if top.confidence < 0.7
                  else f"Top two hypotheses within {margin:.2f}.") + (f" LLM suggested {llm_note['winner']}." if llm_note else "")
    report = {
        "root_cause": root, "confidence": top.confidence, "margin_to_runner_up": round(margin, 3),
        "blamed": [_pod_card(rca["pods"], n) for n in h["blamed"]],
        "patient_zero": rca["findings"]["patient_zero"],
        "evidence_for": [t for d, t in h["checks"] if d > 0],
        "evidence_against": [t for d, t in h["checks"] if d < 0],
        "ruled_out": [{"id": x.id, "desc": x.description, "confidence": x.confidence,
                       "why_not": [t for d, t in ver[x.id]["checks"] if d < 0][:2]} for x in ranked if x.id != top.id],
        "causal_chain": rca["causal_graph"] if "causal_graph" in rca else state.get("causal_graph"),
        "timeline": {k: _iso(v["ts"]) for k, v in sorted(rca["timeline"].items(), key=lambda kv: kv[1]["ts"])},
        "recommended_actions": _actions(top.id, rca["scope"]["service"], h["blamed"], rca["pods"], rca["timeline"]),
        "llm_assisted": llm_note, "trail": rca["trail"]}
    return {"root_cause_declared": root, "escalation_flag": escalate, "escalation_reason": reason,
            "raw_signals": {**raw, "rca_report": report}}


# ------------------------------------------------------------------ graph ---
def create_rca_subgraph():
    workflow = StateGraph(CognitiveState)
    workflow.add_node("observation", observation_node)
    workflow.add_node("evidence_collector", evidence_collector_node)
    workflow.add_node("log_investigator", log_investigator_node)
    workflow.add_node("causal_graph", causal_graph_builder_node)
    workflow.add_node("hypothesis_generator", hypothesis_generator_node)
    workflow.add_node("hypothesis_verifier", hypothesis_verifier_node)
    workflow.add_node("consensus", rca_consensus_node)
    workflow.set_entry_point("observation")
    workflow.add_edge("observation", "evidence_collector")
    workflow.add_edge("evidence_collector", "log_investigator")
    workflow.add_edge("log_investigator", "causal_graph")
    workflow.add_edge("causal_graph", "hypothesis_generator")
    workflow.add_edge("hypothesis_generator", "hypothesis_verifier")
    workflow.add_edge("hypothesis_verifier", "consensus")
    workflow.add_edge("consensus", END)
    return workflow.compile()


# ------------------------------------------------ demo incidents (fixtures) ---
T0 = datetime(2026, 6, 13, 10, 5, tzinfo=timezone.utc).timestamp()


def _pod(name, app, ver, restarts=0, term=None, ready=True, limit="2Gi", node="node-1"):
    return {"name": name, "app": app, "node": node, "ready": ready, "restarts": restarts, "version": ver,
            "limits": {"memory": limit}, "last_terminated": term}


def _fx_traffic():
    now, mem, rps = T0, [], []
    for i in range(181):
        m = 180 - i
        base = 100 + 3 * math.sin(i / 7)
        r = base * (1 + 2.2 * min(1, max(0, (40 - m) / 10))) if m <= 40 else base
        t = now - m * 60
        rps.append((t, r))
        mem.append((t, min(0.99, 0.30 + 0.0021 * r)))
    pods = [_pod("inference-service-7d9f-a1", "inference-service", "v5", 2, {"reason": "OOMKilled", "exitCode": 137, "ts": now - 30 * 60}),
            _pod("inference-service-7d9f-b2", "inference-service", "v5", 1, {"reason": "OOMKilled", "exitCode": 137, "ts": now - 27 * 60}),
            _pod("inference-service-7d9f-c3", "inference-service", "v5")]
    logs = {"inference-service-7d9f-a1": [{"ts": now - 33 * 60, "line": "WARN memory usage 94% of limit"},
                                          {"ts": now - 31 * 60, "line": "WARN memory usage 98% of limit"}],
            "inference-service-7d9f-b2": [{"ts": now - 29 * 60, "line": "WARN memory usage 97% of limit"}]}
    ev = [{"ts": now - 38 * 60, "reason": "ScalingLimited", "type": "Warning", "object": "horizontalpodautoscaler/inference-service",
           "message": "the desired replica count is more than the maximum replica count"}]
    return {"service": "inference-service", "now": now, "metrics": {"memory_pressure": 0.97, "cpu": 62},
            "history": {"memory_pressure": mem, "rps": rps}, "events": [{"ts": now - 48 * 3600, "type": "deploy", "service": "inference-service", "version": "v5"}],
            "k8s": {"namespace": "prod", "pods": pods, "events": ev, "logs": logs}}


def _fx_leak():
    now, mem, rps = T0, [], []
    dep_age_h, rate = 28.23, 0.14
    for i in range(181):
        t = now - (180 - i) * 600
        h = (t - (now - dep_age_h * 3600)) / 3600
        mem.append((t, 0.30 + (0.14 * (h % ((0.97 - 0.30) / rate)) if h >= 0 else 0.0)))
        rps.append((t, 100 + 25 * math.sin(2 * math.pi * t / 86400)))
    pods = [_pod("inference-service-5c4d-k9", "inference-service", "v2.3.1", 5, {"reason": "OOMKilled", "exitCode": 137, "ts": now - 4.3 * 3600})]
    logs = {"inference-service-5c4d-k9": [
        {"ts": now - 20 * 60, "line": "WARN memory usage 90% of limit"},
        {"ts": now - 4.3 * 3600 - 2, "line": "ERROR unhandled exception while handling request"},
        {"ts": now - 4.3 * 3600 - 1, "line": 'File "/app/cache.py", line 88, in put'},
        {"ts": now - 4.3 * 3600, "line": "MemoryError: Out of memory"}]}
    return {"service": "inference-service", "now": now, "metrics": {"memory_pressure": mem[-1][1], "cpu": 40},
            "history": {"memory_pressure": mem, "rps": rps},
            "events": [{"ts": now - dep_age_h * 3600, "type": "deploy", "service": "inference-service", "version": "v2.3.1"}],
            "k8s": {"namespace": "prod", "pods": pods, "events": [], "logs": logs}}


def _fx_dependency():
    now, mem, rps = T0, [], []
    for i in range(61):
        t = now - (60 - i) * 60
        mem.append((t, 0.42 + 0.004 * math.sin(i)))
        rps.append((t, 120 + 4 * math.sin(i / 3)))
    pods = [_pod(f"checkout-6b8f-x{i}", "checkout", "v77", 0, None, ready=False, limit="1Gi") for i in range(1, 4)]
    pods.append(_pod("redis-master-0", "redis-master", "7.2", 3, {"reason": "OOMKilled", "exitCode": 137, "ts": now - 14 * 60},
                     ready=False, limit="512Mi", node="node-2"))
    logs = {p["name"]: [{"ts": now - 13 * 60 + 20 * k, "line": "ERROR redis.exceptions.ConnectionError: Error 111 connecting to redis-master:6379. Connection refused."}
                        for k in range(38)] + [{"ts": now - 60, "line": "INFO GET /healthz 503"}] for p in pods[:3]}
    logs["redis-master-0"] = [{"ts": now - 15 * 60, "line": "# WARNING Can't save in background: fork: Cannot allocate memory"}]
    ev = [{"ts": now - 12 * 60, "reason": "Unhealthy", "type": "Warning", "object": f"pod/{p['name']}",
           "message": "Readiness probe failed: HTTP probe failed with statuscode: 503"} for p in pods[:3]]
    ev.append({"ts": now - 10 * 60, "reason": "BackOff", "type": "Warning", "object": "pod/redis-master-0", "message": "Back-off restarting failed container"})
    return {"service": "checkout", "now": now, "metrics": {"cpu": 35, "memory_pressure": 0.42, "synthetic_success_rate": 0.55},
            "history": {"memory_pressure": mem, "rps": rps}, "events": [],
            "k8s": {"namespace": "prod", "pods": pods, "events": ev, "logs": logs}}


if __name__ == "__main__":
    app = create_rca_subgraph()
    cases = {"1. Traffic surge OOMs the pods (looks like a leak, isn't)": (_fx_traffic, ["Memory Spike Signature"]),
             "2. Real memory leak since a deploy (28h sawtooth)": (_fx_leak, ["Memory Spike Signature"]),
             "3. Alert on checkout, culprit is redis-master-0": (_fx_dependency, ["Gray Failure"])}
    for title, (fx, pats) in cases.items():
        print(f"\n{'=' * 8} {title}")
        out = app.invoke({"raw_signals": fx(), "active_patterns": pats, "incident_declared": True, "risk_score": 0.9})
        r = out["raw_signals"]["rca_report"]
        print("\nROOT CAUSE :", r["root_cause"])
        print("CONFIDENCE :", r["confidence"], "| escalate:", out.get("escalation_flag"), out.get("escalation_reason") or "")
        print("BLAMED     :", [(b["name"], b.get("last_terminated"), b.get("restarts")) for b in r["blamed"]])
        print("FOR        :", *r["evidence_for"], sep="\n   + ")
        print("RULED OUT  :", *[f"{x['id']} {x['desc']} ({x['confidence']}): {x['why_not']}" for x in r["ruled_out"]], sep="\n   - ")
        print("ACTIONS    :", [(a["action"], a.get("target")) for a in r["recommended_actions"]])
        print("TIMELINE   :", r["timeline"])