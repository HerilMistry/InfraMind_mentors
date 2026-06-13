from langgraph.graph import StateGraph, END
from typing import TypedDict
import uuid
from agents.orchestrator.cognitive_state import CognitiveState

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from agents.monitoring_agent.monitoring_subgraph import create_monitoring_subgraph
from agents.root_cause_agent.rca_subgraph import create_rca_subgraph
from agents.optimization_agent.optimization_subgraph import create_optimization_subgraph
from agents.recovery_agent.recovery_subgraph import create_recovery_subgraph

# --- Subgraph Node Placeholders ---

def hitl_escalation_node(state: CognitiveState):
    print(f"!!! HITL ESCALATION !!! Reason: {state.get('escalation_reason')}")
    return {"consensus_state": "escalated"}

# --- Routing Logic ---

def route_after_monitoring(state: CognitiveState):
    if state.get("escalation_flag"):
        return "hitl"
    if state.get("incident_declared"):
        return "rca_subgraph"
    return END

def route_after_rca(state: CognitiveState):
    if state.get("escalation_flag"):
        return "hitl"
    if state.get("root_cause_declared"):
        return "optimization_subgraph"
    return END

def route_after_optimization(state: CognitiveState):
    if state.get("escalation_flag"):
        return "hitl"
    if state.get("selected_plan"):
        return "recovery_subgraph"
    return END

# --- Graph Construction ---

def create_cognitive_graph():
    workflow = StateGraph(CognitiveState)
    
    monitoring_graph = create_monitoring_subgraph()
    workflow.add_node("monitoring_subgraph", monitoring_graph)
    rca_graph = create_rca_subgraph()
    workflow.add_node("rca_subgraph", rca_graph)
    
    optimization_graph = create_optimization_subgraph()
    workflow.add_node("optimization_subgraph", optimization_graph)
    
    recovery_graph = create_recovery_subgraph()
    workflow.add_node("recovery_subgraph", recovery_graph)
    
    workflow.add_node("hitl", hitl_escalation_node)
    
    workflow.set_entry_point("monitoring_subgraph")
    
    workflow.add_conditional_edges("monitoring_subgraph", route_after_monitoring, {
        "rca_subgraph": "rca_subgraph",
        "hitl": "hitl",
        END: END
    })
    
    workflow.add_conditional_edges("rca_subgraph", route_after_rca, {
        "optimization_subgraph": "optimization_subgraph",
        "hitl": "hitl",
        END: END
    })
    
    workflow.add_conditional_edges("optimization_subgraph", route_after_optimization, {
        "recovery_subgraph": "recovery_subgraph",
        "hitl": "hitl",
        END: END
    })
    
    workflow.add_edge("recovery_subgraph", END)
    workflow.add_edge("hitl", END)
    
    return workflow.compile()

if __name__ == "__main__":
    app = create_cognitive_graph()
    initial_state = {
        "incident_id": str(uuid.uuid4()),
        "raw_signals": {"metrics": {"memory_pressure": 0.95}},
        "active_patterns": [],
        "ttf_estimate": None,
        "risk_score": None,
        "incident_declared": False,
        "evidence_pool": [],
        "causal_graph": {},
        "active_hypotheses": [],
        "root_cause_declared": None,
        "proposed_strategies": [],
        "ranked_strategies": [],
        "selected_plan": None,
        "validation_status": False,
        "dry_run_success": False,
        "execution_success": False,
        "verification_success": False,
        "escalation_flag": False,
        "escalation_reason": None,
        "consensus_state": None
    }
    result = app.invoke(initial_state)
    print("--- Final State ---")
    print(f"Incident Declared: {result.get('incident_declared')}")
    print(f"Root Cause: {result.get('root_cause_declared')}")
    if result.get("selected_plan"):
        print(f"Selected Strategy: {result['selected_plan'].strategy_id}")
    print(f"Execution Success: {result.get('execution_success')}")
