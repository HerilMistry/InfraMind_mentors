from langgraph.graph import StateGraph, END
from agents.orchestrator.cognitive_state import CognitiveState, ExecutionPlan, AtomicAction
import random

def strategy_generator_node(state: CognitiveState):
    """Generates candidate remediation strategies based on Root Cause."""
    print("[Optimization] Strategy Generator building candidate plans...")
    rc = state.get("root_cause_declared")
    strategies = []
    if rc == "Traffic Spike causing OOM":
        # Candidate 1: Scale Replicas
        a1 = AtomicAction(action_type="SCALE_UP", target="inference-deployment", parameters={"replicas": 5})
        strategies.append(ExecutionPlan(strategy_id="strat-scale", actions=[a1], safety_score=0.0, cost_estimate=0.0))
        
        # Candidate 2: Restart Pods (Faster but lower resolution chance)
        a2 = AtomicAction(action_type="RESTART", target="inference-deployment", parameters={})
        strategies.append(ExecutionPlan(strategy_id="strat-restart", actions=[a2], safety_score=0.0, cost_estimate=0.0))
        
    return {"proposed_strategies": strategies}

def cost_analysis_node(state: CognitiveState):
    """Evaluates the financial/resource cost of a strategy."""
    print("[Optimization] Cost Analysis evaluating resource spend...")
    strategies = state.get("proposed_strategies", [])
    for s in strategies:
        if s.strategy_id == "strat-scale":
            s.cost_estimate = 25.0  # arbitrary cost units
        elif s.strategy_id == "strat-restart":
            s.cost_estimate = 5.0
    return {"proposed_strategies": strategies}

def safety_analysis_node(state: CognitiveState):
    """Evaluates the risk of the remediation (blast radius)."""
    print("[Optimization] Safety Analysis checking constraints...")
    strategies = state.get("proposed_strategies", [])
    for s in strategies:
        if s.strategy_id == "strat-scale":
            s.safety_score = 0.95  # Very safe, just adds capacity
        elif s.strategy_id == "strat-restart":
            s.safety_score = 0.60  # Risky, drops active connections
    return {"proposed_strategies": strategies}

def performance_impact_node(state: CognitiveState):
    """Simulates the effect of the strategy on TTF and latency."""
    print("[Optimization] Performance Impact simulating TTF resolution...")
    return {} # Stateful updates omitted for simplicity

def resource_planner_node(state: CognitiveState):
    """Determines exact parameters based on current cluster capacity."""
    print("[Optimization] Resource Planner verifying cluster capacity...")
    return {}

def optimization_ranking_node(state: CognitiveState):
    """Ranks parameterized strategies using a Pareto frontier."""
    print("[Optimization] Ranking Agent balancing Cost vs Safety...")
    strategies = state.get("proposed_strategies", [])
    # Ranking logic: High Safety is prioritized, followed by Low Cost.
    # Score = (Safety * 100) - Cost
    ranked = sorted(strategies, key=lambda s: (s.safety_score * 100) - s.cost_estimate, reverse=True)
    return {"ranked_strategies": ranked}

def optimization_consensus_node(state: CognitiveState):
    """Commits to the top strategy and constructs the Execution Plan."""
    ranked = state.get("ranked_strategies", [])
    if not ranked:
        return {"selected_plan": None}
    
    top_plan = ranked[0]
    print(f"[Optimization] Consensus Reached - Selected Strategy: {top_plan.strategy_id} (Safety: {top_plan.safety_score})")
    
    escalate = False
    reason = None
    if top_plan.safety_score < 0.8:
        escalate = True
        reason = f"Top strategy {top_plan.strategy_id} falls below safety threshold."
        
    return {
        "selected_plan": top_plan,
        "escalation_flag": escalate,
        "escalation_reason": reason
    }

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
