from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState, LogEvent, Hypothesis

def observation_node(state: CognitiveState):
    
    print("[RCA] Observation Agent scoping incident...")
    return {}

def evidence_collector_node(state: CognitiveState):
    
    print("[RCA] Evidence Collector querying Loki/Kubernetes...")
    # Simulated logs
    logs = [
        LogEvent(timestamp="2026-06-13T10:00:00", level="ERROR", message="OOMKilled pod inference-service"),
        LogEvent(timestamp="2026-06-13T10:01:00", level="WARN", message="Memory usage 98%")
    ]
    return {"evidence_pool": logs}

def log_investigator_node(state: CognitiveState):
    
    print("[RCA] Log Investigator analyzing stack traces...")
    return {}

def causal_graph_builder_node(state: CognitiveState):
    
    print("[RCA] Causal Graph Builder connecting temporal events...")
    cg = {
        "Memory Spike": ["OOMKilled"],
        "OOMKilled": ["Service Disruption"]
    }
    return {"causal_graph": cg}

def hypothesis_generator_node(state: CognitiveState):
    
    print("[RCA] Hypothesis Generator proposing causes...")
    h1 = Hypothesis(id="H1", description="Memory Leak in Inference App", confidence=0.6, expected_symptoms=["Gradual memory increase over 24h"])
    h2 = Hypothesis(id="H2", description="Traffic Spike causing OOM", confidence=0.4, expected_symptoms=["Sudden ingress request spike"])
    return {"active_hypotheses": [h1, h2]}

def hypothesis_verifier_node(state: CognitiveState):
    
    print("[RCA] Hypothesis Verifier cross-checking evidence...")
    hyps = state.get("active_hypotheses", [])
    # Simulate finding evidence for H2
    for h in hyps:
        if h.id == "H2":
            h.confidence = 0.95
        else:
            h.confidence = 0.1
    return {"active_hypotheses": hyps}

def rca_consensus_node(state: CognitiveState):
    
    hyps = state.get("active_hypotheses", [])
    if not hyps:
        return {"root_cause_declared": None}
    
    top_hyp = max(hyps, key=lambda x: x.confidence)
    root_cause = top_hyp.description
    
    print(f"[RCA] Consensus Reached - Root Cause: {root_cause} (Confidence: {top_hyp.confidence})")
    
    escalate = False
    reason = None
    if top_hyp.confidence < 0.7:
        escalate = True
        reason = "Low confidence in root cause diagnosis."
        
    return {
        "root_cause_declared": root_cause,
        "escalation_flag": escalate,
        "escalation_reason": reason
    }

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
