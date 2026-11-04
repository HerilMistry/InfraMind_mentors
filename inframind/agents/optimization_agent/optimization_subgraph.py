"""
Optimization subgraph - same 7 nodes, same edges, real logic inside.

RCA says WHAT broke and what NOT to do. Optimization decides WHICH SAFE FIX to run, in what order,
and how we will know it worked. Kubernetes' own remedies (restart / reschedule / scale) fix a pod,
not a cause - so the optimiser deliberately builds the "Kubernetes default" plan too, and vetoes it
when the RCA proved it is wrong. That veto, with the reason, is the differentiator.

  strategy_generator   turn rca_report.recommended_actions into candidate plans (+ the k8s-default baseline),
                       ordered mitigate -> fix -> prevent
  cost_analysis        action cost + replica cost + downtime cost (downtime depends on fixing the CAUSE)
  safety_analysis      enforce RCA do_not_do (veto), noisy-OR action risk, low-confidence + last-pod penalties
  performance_impact   P(relief) from diagnosis confidence, time-to-relief, does it beat monitoring's TTF?
  resource_planner     real capacity checks; repairs plans (HPA pinned -> raise max first; shrink to fit memory)
  ranking              utility = 100*safety*P(relief) - cost - penalties (recurrence, races TTF)
  consensus            pick winner, list rejected plans + why, verification/abort criteria, escalate if unsure

Reads : raw_signals["rca_report"], ["rca"], optional ["capacity"], state["ttf_estimate"]
Writes: raw_signals["opt"] (working data), raw_signals["opt_report"] (for the executor / human).
The plan is a DRY RUN: it never executes anything.
"""
import math

from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState, ExecutionPlan, AtomicAction

DOWNTIME_COST_PER_MIN = 2.0     # cost units per minute the incident stays open
UNRESOLVED_DOWNTIME_MIN = 45.0  # assumed incident length if the plan does not touch the root cause
REPLICA_COST = 3.0              # cost units per extra replica (1h)
UNCERTAIN_CONF = 0.7            # below this, irreversible actions are penalised and a human approves
SAFETY_FLOOR = 0.8              # same threshold as the original consensus node
TIE_MARGIN = 5.0                # utility gap below which the choice is a coin flip -> escalate
RECURRENCE_PENALTY = 15.0
TTF_PENALTY = 25.0


# ------------------------------------------------------------ action catalog ---
def _a(type_, cost, risk, ttr, phase, rev=True, disr=False):
    """phase: 0 mitigate, 1 fix, 2 prevent/follow-up. ttr = minutes until it takes effect (0 = no direct relief)."""
    return {"type": type_, "cost": cost, "risk": risk, "ttr": ttr, "phase": phase,
            "reversible": rev, "disruptive": disr}


ACTIONS = {
    "rollback_deployment": _a("ROLLBACK", 8, 0.20, 5, 1),
    "rolling_restart": _a("RESTART", 5, 0.15, 3, 0, disr=True),
    "restart_pods": _a("RESTART", 4, 0.35, 2, 0, disr=True),           # all-at-once: what a naive responder does
    "raise_memory_limit_temporarily": _a("PATCH_LIMITS", 3, 0.15, 3, 0),
    "scale_out": _a("SCALE_UP", 2, 0.05, 3, 0),
    "raise_hpa_max_replicas": _a("PATCH_HPA", 1, 0.05, 1, 0),
    "add_rate_limit_or_shed_load": _a("RATE_LIMIT", 2, 0.20, 1, 0),    # drops some user requests
    "review_memory_limit": _a("REVIEW", 0, 0.0, 0, 2),
    "restore_dependency": _a("RESTORE_DEPENDENCY", 10, 0.25, 6, 1),
    "enable_circuit_breaker": _a("CIRCUIT_BREAKER", 2, 0.10, 1, 0),
    "investigate_dependency_resource_limits": _a("INVESTIGATE", 0, 0.0, 0, 2),
    "cordon_and_drain_node": _a("DRAIN_NODE", 12, 0.30, 8, 1, disr=True),
    "add_node_capacity": _a("ADD_NODE", 30, 0.05, 6, 0),
    "fix_config_or_secret": _a("PATCH_CONFIG", 3, 0.20, 3, 1),
    "rollback_config": _a("ROLLBACK", 4, 0.15, 3, 1),
    "raise_or_remove_cpu_limit": _a("PATCH_LIMITS", 3, 0.15, 3, 1),
    "right_size_cpu_requests": _a("PATCH_LIMITS", 2, 0.10, 3, 2),
    "free_disk_or_expand_volume": _a("EXPAND_VOLUME", 6, 0.20, 4, 0, rev=False),
    "rotate_or_cap_logs": _a("PATCH_CONFIG", 2, 0.10, 2, 1),
    "find_top_disk_writer": _a("INVESTIGATE", 0, 0.0, 0, 2),
    "renew_certificate": _a("RENEW_CERT", 4, 0.10, 3, 1),
    "fix_rotation_job_or_cert_manager": _a("PATCH_CONFIG", 3, 0.10, 5, 2),
    "add_expiry_alert": _a("ALERT_RULE", 1, 0.0, 0, 2),
    "add_retry_backoff_jitter_and_budget": _a("PATCH_CONFIG", 4, 0.15, 8, 1),
}
DEFAULT = _a("MANUAL", 3, 0.30, 10, 1)

# which actions actually remove / neutralise each diagnosed cause
RESOLVES = {
    "H1": {"rollback_deployment"},
    "H2": {"scale_out", "raise_hpa_max_replicas", "add_rate_limit_or_shed_load"},
    "H3": {"rollback_deployment"},
    "H4": {"restore_dependency", "enable_circuit_breaker"},
    "H5": {"cordon_and_drain_node", "add_node_capacity"},
    "H6": {"fix_config_or_secret", "rollback_config"},
    "H7": {"raise_or_remove_cpu_limit"},
    "H8": {"free_disk_or_expand_volume"},
    "H9": {"renew_certificate"},
    "H10": {"enable_circuit_breaker", "add_retry_backoff_jitter_and_budget"},
}
KEYWORDS = [("retry storm", "H10"), ("memory leak", "H1"), ("traffic spike", "H2"), ("bad deploy", "H3"),
            ("dependency", "H4"), ("node pressure", "H5"), ("configuration", "H6"),
            ("cpu throttling", "H7"), ("disk", "H8"), ("certificate", "H9")]

_RESTARTS = {"rolling_restart", "restart_pods"}
# RCA do_not_do token -> (actions it blocks, scope)   scope: any | victims (target is a victim service) | sole (plan has nothing else)
BLOCK_RULES = {
    "restart_only": (_RESTARTS, "sole"), "restart_pod": (_RESTARTS, "any"), "restart_pods": (_RESTARTS, "any"),
    "restart_callers": (_RESTARTS, "victims"), "restart_pods_on_same_node": (_RESTARTS, "any"),
    "scale_out": ({"scale_out"}, "any"), "scale_out_callers": ({"scale_out"}, "victims"),
    "rollback_deployment": ({"rollback_deployment"}, "any"),
    "raise_memory_limit_only": ({"raise_memory_limit_temporarily"}, "sole"),
    "add_nodes": ({"add_node_capacity"}, "any"),
}
VERIFY = {  # (success looks like, abort/escalate if)
    "H1": ("memory sawtooth stops; no OOMKilled for 2 full leak cycles", "memory climbs again after rollback -> leak predates the deploy, escalate"),
    "H2": ("memory_pressure < 0.7 and no new OOMKilled for 15m", "OOM recurs or request rate keeps rising after scale-out -> shed load / escalate"),
    "H3": ("errors stop on new pods; exception type disappears", "errors persist on rolled-back version -> not the deploy"),
    "H4": ("dependency Ready; callers' connection errors -> 0; callers Ready", "dependency crashes again -> raise its limits / escalate"),
    "H5": ("pods rescheduled off the node; no new Evicted events", "pressure appears on the next node too -> capacity problem"),
    "H6": ("pods start without config errors", "same error after fix -> wrong key or secret"),
    "H7": ("throttled periods < 5% and latency back to baseline", "still throttled after raising the limit -> check node-level contention"),
    "H8": ("disk < 80% and no 'no space left' errors", "disk refills quickly -> find the writer before closing"),
    "H9": ("x509 errors stop; synthetic success rate recovers", "errors persist -> chain/intermediate issue or clock skew"),
    "H10": ("downstream call rate back to ~1x user rate; pool errors stop", "calls re-amplify when breaker closes -> keep it open, fix dependency first"),
}


# ---------------------------------------------------------------- helpers ---
def _meta(name):
    return ACTIONS.get(name, DEFAULT)


def _ctx(state):
    raw = dict(state.get("raw_signals", {}) or {})
    return raw, raw.get("rca_report") or {}, raw.get("rca") or {}


def _hid(rep):
    if rep.get("hypothesis_id"):
        return rep["hypothesis_id"]
    head = (rep.get("root_cause") or "").split(" | ")[0].lower()
    return next((h for kw, h in KEYWORDS if kw in head), None)


def _mem_mi(s):
    if not s:
        return None
    for suf, mul in (("Gi", 1024), ("Mi", 1), ("G", 1000), ("M", 1)):
        if str(s).endswith(suf):
            return float(str(s)[:-len(suf)]) * mul
    return None


def _build_actions(acts):
    return [AtomicAction(action_type=_meta(a["action"])["type"], target=a["target"], parameters=dict(a["params"]))
            for a in acts]


def _eta(names, hid):
    """(does the plan touch the diagnosed cause?, minutes until relief)"""
    cov = [_meta(n)["ttr"] for n in names if n in RESOLVES.get(hid, ())]
    if cov:
        return True, float(min(cov))
    anyt = [_meta(n)["ttr"] for n in names if _meta(n)["ttr"] > 0]
    return False, float(min(anyt)) if anyt else 0.0


def _vetoes(acts, avoid, victims):
    out = []
    for tok in avoid:
        head, _, why = tok.partition(" ")
        rule = BLOCK_RULES.get(head)
        if not rule:
            continue
        blocked, scope = rule
        if scope == "sole":
            core = [a for a in acts if _meta(a["action"])["phase"] < 2]
            hit = core[:1] if core and all(a["action"] in blocked for a in core) else []
        else:
            hit = [a for a in acts if a["action"] in blocked and (scope != "victims" or a["target"] in victims)]
        if hit:
            out.append({"action": hit[0]["action"], "target": hit[0]["target"], "rule": head, "why": why.strip("() ")})
    return out


def _steps(acts):
    return [f"{a['action']}->{a['target']}" for a in acts]


# ------------------------------------------------------------------ nodes ---
def strategy_generator_node(state: CognitiveState):
    """Candidate plans from the RCA's recommended actions, plus the plan Kubernetes would run by itself."""
    print("[Optimization] Strategy Generator building candidate plans...")
    raw, rep, rca = _ctx(state)
    pods = rca.get("pods", [])
    svc = (rca.get("scope") or {}).get("service")
    opt = {"plans": {}, "trail": []}
    acts = []
    for r in rep.get("recommended_actions", []):
        name, target = r["action"], r.get("target") or svc or "?"
        prm = dict(r.get("params") or {})
        prm["action"] = name
        if r.get("rationale"):
            prm["rationale"] = r["rationale"]
        if name == "scale_out":
            cur = len([p for p in pods if p["app"] == target]) or 3
            prm.update(current=cur, replicas=cur + max(2, math.ceil(cur * 0.5)))
        acts.append({"action": name, "target": target, "params": prm})
    acts.sort(key=lambda a: _meta(a["action"])["phase"])        # mitigate -> fix -> prevent (stable)

    cands, seen = [], set()

    def add(sid, plan_acts, origin):
        key = tuple((a["action"], a["target"]) for a in plan_acts)
        if plan_acts and key not in seen:
            seen.add(key)
            cands.append((sid, plan_acts, origin))

    add("strat-full", acts, "all RCA actions, mitigate -> fix -> prevent")
    add("strat-core", [a for a in acts if _meta(a["action"])["phase"] < 2], "mitigate + fix now, prevention later")
    add("strat-first", acts[:1], "single highest-priority RCA action (fast path)")

    # what Kubernetes would do on its own: restart if pods already restarted, else scale out
    base_app = svc or (acts[0]["target"] if acts else None)
    if base_app:
        restarts = sum(p["restarts"] for p in pods if p["app"] == base_app)
        if restarts:
            b = {"action": "restart_pods", "target": base_app, "params": {"action": "restart_pods"}}
        else:
            cur = len([p for p in pods if p["app"] == base_app]) or 3
            b = {"action": "scale_out", "target": base_app,
                 "params": {"action": "scale_out", "current": cur, "replicas": cur + max(2, math.ceil(cur * 0.5))}}
        add("strat-k8s-default", [b], "what Kubernetes/HPA would do by itself (baseline)")

    strategies = []
    for sid, plan_acts, origin in cands:
        opt["plans"][sid] = {"acts": plan_acts, "origin": origin}
        strategies.append(ExecutionPlan(strategy_id=sid, actions=_build_actions(plan_acts),
                                        safety_score=0.0, cost_estimate=0.0))
    opt["trail"].append(f"[generator] {len(strategies)} candidate plans for {_hid(rep) or 'unknown cause'}: "
                        f"{[s.strategy_id for s in strategies]}")
    if not strategies:
        opt["trail"].append("[generator] no rca_report / recommended_actions - nothing to plan")
    return {"proposed_strategies": strategies, "raw_signals": {**raw, "opt": opt}}


def cost_analysis_node(state: CognitiveState):
    """Infra cost + the cost of the incident staying open (depends on whether the CAUSE is fixed)."""
    print("[Optimization] Cost Analysis evaluating resource spend...")
    raw, rep, _ = _ctx(state)
    opt, hid = dict(raw["opt"]), _hid(rep)
    opt["cost"] = {}
    strategies = state.get("proposed_strategies", [])
    for s in strategies:
        acts = opt["plans"][s.strategy_id]["acts"]
        infra = sum(_meta(a["action"])["cost"] for a in acts)
        infra += sum(REPLICA_COST * (a["params"].get("replicas", 0) - a["params"].get("current", 0))
                     for a in acts if a["action"] == "scale_out")
        covers, ttr = _eta([a["action"] for a in acts], hid)
        down = DOWNTIME_COST_PER_MIN * (ttr if covers else UNRESOLVED_DOWNTIME_MIN)
        s.cost_estimate = round(infra + down, 1)
        opt["cost"][s.strategy_id] = {"infra": round(infra, 1), "downtime": round(down, 1)}
    return {"proposed_strategies": strategies, "raw_signals": {**raw, "opt": opt}}


def safety_analysis_node(state: CognitiveState):
    """Enforce the RCA's do_not_do list, then score residual risk."""
    print("[Optimization] Safety Analysis checking constraints...")
    raw, rep, rca = _ctx(state)
    opt = dict(raw["opt"])
    opt["safety"] = {}
    conf = rep.get("confidence", 0.0)
    avoid = rep.get("do_not_do", [])
    victims = set((rep.get("blast_radius") or {}).get("victims", []))
    pods = rca.get("pods", [])
    strategies = state.get("proposed_strategies", [])
    for s in strategies:
        acts = opt["plans"][s.strategy_id]["acts"]
        notes, ok = [], 1.0
        for a in acts:                                      # noisy-OR, same family as monitoring's risk
            ok *= 1 - _meta(a["action"])["risk"]
        if conf < UNCERTAIN_CONF and any(not _meta(a["action"])["reversible"] for a in acts):
            ok *= 0.85
            notes.append(f"irreversible action while diagnosis confidence is only {conf:.2f}")
        for a in acts:
            if _meta(a["action"])["disruptive"]:
                mine = [p for p in pods if p["app"] == a["target"]]
                if mine and len([p for p in mine if p["ready"]]) <= 1:
                    ok *= 0.8
                    notes.append(f"{a['action']} on {a['target']} would take down the last ready pod")
        vet = _vetoes(acts, avoid, victims)
        s.safety_score = 0.0 if vet else round(ok, 3)
        opt["safety"][s.strategy_id] = {"vetoes": vet, "notes": notes, "raw_score": round(ok, 3)}
        if vet:
            opt["trail"].append(f"[safety] {s.strategy_id} VETOED by RCA: "
                                f"{vet[0]['action']} on {vet[0]['target']} ({vet[0]['why']})")
    return {"proposed_strategies": strategies, "raw_signals": {**raw, "opt": opt}}


def performance_impact_node(state: CognitiveState):
    """Simulate: will it work, how soon, will it come back, does it beat the monitoring TTF?"""
    print("[Optimization] Performance Impact simulating TTF resolution...")
    raw, rep, _ = _ctx(state)
    opt, hid = dict(raw["opt"]), _hid(rep)
    conf, ttf = rep.get("confidence", 0.0), state.get("ttf_estimate")
    opt["perf"] = {}
    for sid, p in opt["plans"].items():
        covers, eta = _eta([a["action"] for a in p["acts"]], hid)
        relief = 0.1 + 0.8 * conf if covers else 0.15      # fixing the wrong thing only helps by luck
        races = ttf is not None and eta > ttf
        opt["perf"][sid] = {"covers_root": covers, "eta_min": eta, "relief_prob": round(relief, 2),
                            "recurrence": "low" if covers else "high", "races_ttf": races}
        if races:
            opt["trail"].append(f"[perf] {sid} needs ~{eta:.0f}m but monitoring predicts breach in {ttf:.0f}m")
    return {"raw_signals": {**raw, "opt": opt}}


def resource_planner_node(state: CognitiveState):
    """Check the plan against real cluster limits, and REPAIR it when it would silently fail."""
    print("[Optimization] Resource Planner verifying cluster capacity...")
    raw, rep, rca = _ctx(state)
    opt = dict(raw["opt"])
    opt["resource"] = {}
    cap, pods = raw.get("capacity") or {}, rca.get("pods", [])
    pinned = any("maximum replica" in e.get("message", "") or "maxReplicas" in e.get("message", "")
                 for e in rca.get("k8s_events", []))
    strategies = state.get("proposed_strategies", [])
    for s in strategies:
        p = opt["plans"][s.strategy_id]
        acts, notes, feasible, why = p["acts"], [], True, None
        for a in list(acts):
            if a["action"] != "scale_out":
                continue
            prm = a["params"]
            per_pod = next((_mem_mi(q.get("limits", {}).get("memory")) for q in pods
                            if q["app"] == a["target"] and q.get("limits", {}).get("memory")), None) or 512
            extra, free = prm["replicas"] - prm["current"], cap.get("free_mem_mi")
            if free is not None and extra * per_pod > free:
                fit = int(free // per_pod)
                if fit >= 1:
                    prm["replicas"] = prm["current"] + fit
                    notes.append(f"cluster has {free:.0f}Mi free: shrank scale-out to {prm['replicas']} replicas "
                                 f"({per_pod:.0f}Mi each)")
                else:
                    feasible, why = False, "no free cluster memory for even one more replica; add_node_capacity first"
            if feasible and pinned and not any(x["action"] == "raise_hpa_max_replicas" for x in acts):
                acts.insert(acts.index(a), {"action": "raise_hpa_max_replicas", "target": a["target"],
                                            "params": {"action": "raise_hpa_max_replicas", "max": prm["replicas"]}})
                s.cost_estimate = round(s.cost_estimate + _meta("raise_hpa_max_replicas")["cost"], 1)
                notes.append("AMENDED: HPA is pinned at max replicas and would undo a manual scale-out; "
                             "inserted raise_hpa_max_replicas first")
        s.actions = _build_actions(acts)
        opt["resource"][s.strategy_id] = {"feasible": feasible, "why": why, "notes": notes}
    return {"proposed_strategies": strategies, "raw_signals": {**raw, "opt": opt}}


def optimization_ranking_node(state: CognitiveState):
    """utility = 100*safety*P(relief) - cost - penalties. Vetoed / infeasible plans are excluded WITH reasons."""
    print("[Optimization] Ranking Agent balancing Cost vs Safety...")
    raw, _, _ = _ctx(state)
    opt = dict(raw["opt"])
    opt["utility"], opt["rejected"] = {}, []
    viable = []
    for s in state.get("proposed_strategies", []):
        sid = s.strategy_id
        sf, rs, pf = opt["safety"][sid], opt["resource"][sid], opt["perf"][sid]
        steps = _steps(opt["plans"][sid]["acts"])
        if sf["vetoes"]:
            opt["rejected"].append({"id": sid, "steps": steps, "why": "; ".join(
                f"{v['action']}->{v['target']} blocked by RCA rule '{v['rule']}': {v['why']}" for v in sf["vetoes"])})
            continue
        if not rs["feasible"]:
            opt["rejected"].append({"id": sid, "steps": steps, "why": rs["why"]})
            continue
        u = 100 * s.safety_score * pf["relief_prob"] - s.cost_estimate
        u -= RECURRENCE_PENALTY if pf["recurrence"] == "high" else 0
        u -= TTF_PENALTY if pf["races_ttf"] else 0
        opt["utility"][sid] = round(u, 1)
        viable.append(s)
    ranked = sorted(viable, key=lambda s: opt["utility"][s.strategy_id], reverse=True)
    opt["trail"].append("[ranking] " + " > ".join(f"{s.strategy_id}({opt['utility'][s.strategy_id]})" for s in ranked))
    return {"ranked_strategies": ranked, "raw_signals": {**raw, "opt": opt}}


def optimization_consensus_node(state: CognitiveState):
    raw, rep, _ = _ctx(state)
    opt = dict(raw.get("opt", {}))
    ranked = state.get("ranked_strategies", [])
    if not ranked:
        why = "; ".join(f"{r['id']}: {r['why']}" for r in opt.get("rejected", [])) or "no rca_report / actions to plan from"
        print("[Optimization] Consensus: no viable plan -> escalate")
        return {"selected_plan": None, "escalation_flag": True,
                "escalation_reason": f"No safe plan available. {why}",
                "raw_signals": {**raw, "opt_report": {"selected": None, "rejected": opt.get("rejected", []),
                                                      "trail": opt.get("trail", [])}}}
    top = ranked[0]
    sid = top.strategy_id
    print(f"[Optimization] Consensus Reached - Selected Strategy: {sid} (Safety: {top.safety_score})")
    pf, sf, conf = opt["perf"][sid], opt["safety"][sid], rep.get("confidence", 0.0)
    margin = opt["utility"][sid] - opt["utility"][ranked[1].strategy_id] if len(ranked) > 1 else None

    reasons = []
    if top.safety_score < SAFETY_FLOOR:
        reasons.append(f"Top strategy {sid} falls below safety threshold ({top.safety_score}).")
    if not pf["covers_root"]:
        reasons.append("Top plan only mitigates; no safe action addresses the diagnosed cause.")
    if conf < UNCERTAIN_CONF:
        reasons.append(f"Diagnosis confidence is only {conf:.2f}.")
    if pf["races_ttf"]:
        reasons.append("Plan may land after the predicted breach.")
    if margin is not None and margin < TIE_MARGIN:
        reasons.append(f"Top two plans within {margin:.1f} utility.")
    escalate = bool(reasons)

    chosen = opt["plans"][sid]["acts"]
    taken = {(a["action"], a["target"]) for a in chosen}
    follow_ups = [f"{a['action']}->{a['target']}" for a in opt["plans"].get("strat-full", {}).get("acts", [])
                  if (a["action"], a["target"]) not in taken]
    ok_when, abort_when = VERIFY.get(_hid(rep), ("service metrics recover", "no improvement in 10m -> escalate"))
    report = {
        "selected": sid, "why_selected": opt["plans"][sid]["origin"],
        "steps": [{"action": a["action"], "target": a["target"], "phase": ["mitigate", "fix", "prevent"][_meta(a["action"])["phase"]],
                   "params": {k: v for k, v in a["params"].items() if k not in ("action", "rationale")},
                   "rationale": a["params"].get("rationale")} for a in chosen],
        "safety": top.safety_score, "cost": top.cost_estimate, "cost_breakdown": opt["cost"][sid],
        "p_relief": pf["relief_prob"], "eta_min": pf["eta_min"], "recurrence_risk": pf["recurrence"],
        "diagnosis_confidence": conf, "safety_notes": sf["notes"],
        "amendments": opt["resource"][sid]["notes"],
        "rejected": opt["rejected"] + [{"id": s.strategy_id, "steps": _steps(opt["plans"][s.strategy_id]["acts"]),
                                        "why": f"lower utility ({opt['utility'][s.strategy_id]} vs {opt['utility'][sid]})"}
                                       for s in ranked[1:]],
        "then_consider": follow_ups,
        "success_criteria": ok_when, "abort_if": abort_when,
        "k8s_gap": rep.get("k8s_gap"), "do_not_do": rep.get("do_not_do", []),
        "approval_required": escalate or any(_meta(a["action"])["disruptive"] for a in chosen),
        "dry_run": True, "trail": opt.get("trail", [])}
    return {"selected_plan": top, "escalation_flag": escalate,
            "escalation_reason": " ".join(reasons) or None,
            "raw_signals": {**raw, "opt_report": report}}


# ------------------------------------------------------------------ graph ---
def create_optimization_subgraph():
    workflow = StateGraph(CognitiveState)
    workflow.add_node("strategy_generator", strategy_generator_node)
    workflow.add_node("cost_analysis", cost_analysis_node)
    workflow.add_node("safety_analysis", safety_analysis_node)
    workflow.add_node("performance_impact", performance_impact_node)
    workflow.add_node("resource_planner", resource_planner_node)
    workflow.add_node("ranking", optimization_ranking_node)
    workflow.add_node("consensus", optimization_consensus_node)
    workflow.set_entry_point("strategy_generator")
    workflow.add_edge("strategy_generator", "cost_analysis")
    workflow.add_edge("cost_analysis", "safety_analysis")
    workflow.add_edge("safety_analysis", "performance_impact")
    workflow.add_edge("performance_impact", "resource_planner")
    workflow.add_edge("resource_planner", "ranking")
    workflow.add_edge("ranking", "consensus")
    workflow.add_edge("consensus", END)
    return workflow.compile()


# ------------------------------------------------ demo incidents (fixtures) ---
def _pods(app, n, restarts=0, ready=True, mem="2Gi"):
    return [{"name": f"{app}-{i}", "app": app, "ready": ready, "restarts": restarts, "limits": {"memory": mem}}
            for i in range(n)]


def _fx_traffic():
    acts = [{"action": "scale_out", "target": "inference-service", "rationale": "absorb load"},
            {"action": "raise_hpa_max_replicas", "target": "inference-service"},
            {"action": "add_rate_limit_or_shed_load", "target": "inference-service"},
            {"action": "review_memory_limit", "target": "inference-service", "params": {"current": "2Gi"}}]
    rep = {"root_cause": "Traffic Spike causing OOM | inference-service OOMKilled first", "hypothesis_id": "H2",
           "confidence": 0.91, "recommended_actions": acts, "blast_radius": {"victims": [], "edges": {}},
           "do_not_do": ["rollback_deployment (code unchanged)", "raise_memory_limit_only"],
           "k8s_gap": "OOMKilled looks identical to a leak; HPA was already pinned at max."}
    rca = {"scope": {"service": "inference-service"}, "pods": _pods("inference-service", 3, restarts=2),
           "k8s_events": [{"message": "the desired replica count is more than the maximum replica count"}]}
    return {"rca_report": rep, "rca": rca, "capacity": {"free_mem_mi": 3000}}, 12.0


def _fx_dependency():
    acts = [{"action": "restore_dependency", "target": "redis-master-0", "rationale": "root of the outage"},
            {"action": "enable_circuit_breaker", "target": "checkout"},
            {"action": "investigate_dependency_resource_limits", "target": "redis-master-0"}]
    rep = {"root_cause": "Downstream dependency failure | dependency redis-master-0 down", "hypothesis_id": "H4",
           "confidence": 0.88, "recommended_actions": acts,
           "blast_radius": {"victims": ["checkout"], "edges": {"checkout->redis-master": 114}},
           "do_not_do": ["restart_callers (they are victims)", "scale_out_callers (more connection attempts)"],
           "k8s_gap": "Kubernetes marks the CALLERS NotReady; it has no concept of caller vs culprit."}
    rca = {"scope": {"service": "checkout"}, "pods": _pods("checkout", 3, ready=False, mem="1Gi"), "k8s_events": []}
    return {"rca_report": rep, "rca": rca}, None


def _fx_leak():
    acts = [{"action": "rollback_deployment", "target": "inference-service", "rationale": "leak aligned with latest deploy"},
            {"action": "rolling_restart", "target": "inference-service", "rationale": "mitigate until fixed"},
            {"action": "raise_memory_limit_temporarily", "target": "inference-service", "params": {"current": "2Gi"}}]
    rep = {"root_cause": "Memory leak in inference-service | OOMKilled repeatedly", "hypothesis_id": "H1",
           "confidence": 0.87, "recommended_actions": acts, "blast_radius": {"victims": [], "edges": {}},
           "do_not_do": ["restart_only (leak returns within hours)"],
           "k8s_gap": "Kubernetes restarts the OOMKilled pod, which resets the leak and hides it."}
    rca = {"scope": {"service": "inference-service"}, "pods": _pods("inference-service", 3, restarts=5), "k8s_events": []}
    return {"rca_report": rep, "rca": rca}, 90.0


if __name__ == "__main__":
    app = create_optimization_subgraph()
    cases = {"1. Traffic-spike OOM (HPA pinned, tight cluster)": _fx_traffic,
             "2. Alert on checkout, culprit is redis": _fx_dependency,
             "3. Real leak: restart would 'work' for a few hours": _fx_leak}
    for title, fx in cases.items():
        print(f"\n{'=' * 8} {title}")
        sig, ttf = fx()
        out = app.invoke({"raw_signals": sig, "ttf_estimate": ttf, "incident_declared": True})
        r = out["raw_signals"]["opt_report"]
        print("\nSELECTED   :", r["selected"], f"({r['why_selected']})")
        print("STEPS      :", *[f"[{s['phase']}] {s['action']} -> {s['target']} {s['params'] or ''}" for s in r["steps"]], sep="\n   > ")
        print("SAFETY/COST:", r["safety"], "/", r["cost"], "| P(relief):", r["p_relief"], "| ETA(min):", r["eta_min"],
              "| recurrence:", r["recurrence_risk"])
        print("AMENDED    :", r["amendments"])
        print("REJECTED   :", *[f"{x['id']} {x['steps']}: {x['why']}" for x in r["rejected"]], sep="\n   - ")
        print("THEN       :", r["then_consider"])
        print("SUCCESS IF :", r["success_criteria"])
        print("ABORT IF   :", r["abort_if"])
        print("APPROVAL   :", r["approval_required"], "| escalate:", out["escalation_flag"], out.get("escalation_reason") or "")