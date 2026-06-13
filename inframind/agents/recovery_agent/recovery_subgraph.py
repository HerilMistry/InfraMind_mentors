from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState

def policy_validation_node(state: CognitiveState):
    
    print("[Recovery] Policy Validation Agent checking maintenance windows...")
    return {"validation_status": True}

def recovery_planner_node(state: CognitiveState):
    
    print("[Recovery] Recovery Planner translating actions into K8s intent...")
    return {}

def failure_simulator_node(state: CognitiveState):
    
    print("[Recovery] Failure Simulator executing K8s --dry-run...")
    # Simulate dry run
    valid = state.get("validation_status", False)
    return {"dry_run_success": valid}

def kubernetes_executor_node(state: CognitiveState):
    
    dry_run = state.get("dry_run_success", False)
    if not dry_run:
        print("[Recovery] Kubernetes Executor skipped due to failed dry-run.")
        return {"execution_success": False}
        
    print("[Recovery] Kubernetes Executor applying patches to cluster...")
    return {"execution_success": True}

def recovery_verifier_node(state: CognitiveState):
    
    print("[Recovery] Recovery Verifier tracking post-execution metrics...")
    exec_success = state.get("execution_success", False)
    # Simulate metric stabilization
    verified = exec_success
    return {"verification_success": verified}

def rollback_node(state: CognitiveState):
    
    print("[Recovery] Rollback Agent restoring previous state...")
    return {}

def recovery_consensus_node(state: CognitiveState):
    
    verified = state.get("verification_success", False)
    exec_success = state.get("execution_success", False)
    
    status = "SUCCESS" if verified else "FAILED_ROLLBACK_REQUIRED"
    print(f"[Recovery] Consensus Reached - Incident Resolution Status: {status}")
    
    escalate = False
    reason = None
    if not verified and exec_success:
        escalate = True
        reason = "Verification failed post-execution, manual rollback oversight required."
        
    return {
        "escalation_flag": escalate,
        "escalation_reason": reason
    }

# Routing function for verifier
def route_after_verification(state: CognitiveState):
    if state.get("verification_success"):
        return "consensus"
    return "rollback"

def create_recovery_subgraph():
    workflow = StateGraph(CognitiveState)
    
    workflow.add_node("policy_validation", policy_validation_node)
    workflow.add_node("recovery_planner", recovery_planner_node)
    workflow.add_node("failure_simulator", failure_simulator_node)
    workflow.add_node("kubernetes_executor", kubernetes_executor_node)
    workflow.add_node("recovery_verifier", recovery_verifier_node)
    workflow.add_node("rollback", rollback_node)
    workflow.add_node("consensus", recovery_consensus_node)
    
    workflow.set_entry_point("policy_validation")
    
    workflow.add_edge("policy_validation", "recovery_planner")
    workflow.add_edge("recovery_planner", "failure_simulator")
    workflow.add_edge("failure_simulator", "kubernetes_executor")
    workflow.add_edge("kubernetes_executor", "recovery_verifier")
    
    workflow.add_conditional_edges("recovery_verifier", route_after_verification, {
        "consensus": "consensus",
        "rollback": "rollback"
    })
    
    workflow.add_edge("rollback", "consensus")
    workflow.add_edge("consensus", END)
    
    return workflow.compile()
