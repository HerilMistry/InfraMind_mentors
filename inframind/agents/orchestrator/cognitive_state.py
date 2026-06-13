from typing import TypedDict, Dict, Any, List, Optional
from pydantic import BaseModel, Field

class LogEvent(BaseModel):
    timestamp: str
    level: str
    message: str

class Hypothesis(BaseModel):
    id: str
    description: str
    confidence: float
    expected_symptoms: List[str] = []

class AtomicAction(BaseModel):
    action_type: str
    target: str
    parameters: Dict[str, Any]

class ExecutionPlan(BaseModel):
    strategy_id: str
    actions: List[AtomicAction]
    safety_score: float
    cost_estimate: float

class CognitiveState(TypedDict):
    incident_id: str
    raw_signals: Dict[str, Any]
    active_patterns: List[str]
    ttf_estimate: Optional[float]
    risk_score: Optional[float]
    incident_declared: bool
    
    evidence_pool: List[LogEvent]
    causal_graph: Dict[str, List[str]] # Simple adjacency list representation
    active_hypotheses: List[Hypothesis]
    root_cause_declared: Optional[str]
    
    proposed_strategies: List[ExecutionPlan]
    ranked_strategies: List[ExecutionPlan]
    selected_plan: Optional[ExecutionPlan]
    
    validation_status: bool
    dry_run_success: bool
    execution_success: bool
    verification_success: bool
    
    escalation_flag: bool
    escalation_reason: Optional[str]
    consensus_state: Optional[str]
