from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState
import random

def metric_observer_node(state: CognitiveState):
    """Parses and normalizes raw telemetry."""
    print("[Monitoring] Metric Observer collecting data...")
    metrics = state.get("raw_signals", {}).get("metrics", {})
    return {"raw_signals": {"metrics": metrics, "status": "normalized"}}

def pattern_detector_node(state: CognitiveState):
    """Identifies cyclical behaviors or known failure signatures."""
    metrics = state.get("raw_signals", {}).get("metrics", {})
    mem = metrics.get("memory_pressure", 0)
    patterns = []
    if mem > 0.85:
        patterns.append("Memory Spike Signature")
    if metrics.get("cpu", 0) > 90:
        patterns.append("CPU Saturation")
    print(f"[Monitoring] Pattern Detector found: {patterns}")
    return {"active_patterns": patterns}

def trend_analyzer_node(state: CognitiveState):
    """Calculates moving averages and momentum."""
    # Simulated trend analysis
    print("[Monitoring] Trend Analyzer computing momentum...")
    return {} # In a real implementation, this would update a trend state

def forecast_agent_node(state: CognitiveState):
    """Predicts when a metric will breach critical thresholds."""
    patterns = state.get("active_patterns", [])
    ttf = None
    if "Memory Spike Signature" in patterns:
        ttf = 15.0 # minutes
    print(f"[Monitoring] Forecast Agent predicts TTF: {ttf}m")
    return {"ttf_estimate": ttf}

def risk_assessment_node(state: CognitiveState):
    """Contextualizes anomalies against business impact."""
    ttf = state.get("ttf_estimate")
    risk_score = 0.1
    if ttf is not None and ttf < 30:
        risk_score = 0.9 # High risk
    print(f"[Monitoring] Risk Assessment Agent calculated risk: {risk_score}")
    return {"risk_score": risk_score}

def monitoring_consensus_node(state: CognitiveState):
    """Aggregates findings and decides if an Incident should be declared."""
    risk = state.get("risk_score", 0.0)
    ttf = state.get("ttf_estimate")
    
    incident_declared = False
    escalation_flag = False
    escalation_reason = None
    
    if risk > 0.8:
        incident_declared = True
    
    # Simulate low consensus requiring human intervention
    if 0.5 < risk < 0.8:
        escalation_flag = True
        escalation_reason = "Ambiguous risk score, human verification required."

    print(f"[Monitoring] Consensus Reached - Incident Declared: {incident_declared}")
    return {
        "incident_declared": incident_declared,
        "escalation_flag": escalation_flag,
        "escalation_reason": escalation_reason
    }

def create_monitoring_subgraph():
    workflow = StateGraph(CognitiveState)
    
    workflow.add_node("metric_observer", metric_observer_node)
    workflow.add_node("pattern_detector", pattern_detector_node)
    workflow.add_node("trend_analyzer", trend_analyzer_node)
    workflow.add_node("forecast_agent", forecast_agent_node)
    workflow.add_node("risk_assessment", risk_assessment_node)
    workflow.add_node("consensus", monitoring_consensus_node)
    
    workflow.set_entry_point("metric_observer")
    
    # Fan-out: After metric observer, we can run pattern and trend in parallel
    # LangGraph automatically handles parallel execution if edges go from A -> B and A -> C.
    # We define sequential flow for simplicity, but logically they are separate cognitive steps.
    workflow.add_edge("metric_observer", "pattern_detector")
    workflow.add_edge("pattern_detector", "trend_analyzer")
    workflow.add_edge("trend_analyzer", "forecast_agent")
    workflow.add_edge("forecast_agent", "risk_assessment")
    workflow.add_edge("risk_assessment", "consensus")
    workflow.add_edge("consensus", END)
    
    return workflow.compile()
