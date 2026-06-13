import os
import uuid
import logging
import threading
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inframind-orchestrator")

app = FastAPI(title="InfraMind Orchestrator", version="1.0.0")

pipeline_status = {"status": "idle", "last_run": None, "result": None}


def run_pipeline(incident_id: str):
    global pipeline_status
    pipeline_status["status"] = "running"
    try:
        from agents.orchestrator.langgraph_flow import create_cognitive_graph
        graph = create_cognitive_graph()
        initial_state = {
            "incident_id": incident_id,
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
            "consensus_state": None,
        }
        result = graph.invoke(initial_state)
        pipeline_status["status"] = "done"
        pipeline_status["last_run"] = incident_id
        pipeline_status["result"] = {
            "incident_declared": result.get("incident_declared"),
            "root_cause": str(result.get("root_cause_declared")),
            "execution_success": result.get("execution_success"),
        }
        logger.info(f"Pipeline complete for {incident_id}: {pipeline_status['result']}")
    except Exception as e:
        pipeline_status["status"] = "error"
        pipeline_status["result"] = str(e)
        logger.error(f"Pipeline error: {e}")


@app.get("/health")
def health():
    return {"status": "ok", "service": "inframind-orchestrator"}


@app.get("/status")
def status():
    return pipeline_status


@app.post("/run")
def trigger_pipeline(background_tasks: BackgroundTasks):
    incident_id = str(uuid.uuid4())
    background_tasks.add_task(run_pipeline, incident_id)
    pipeline_status["status"] = "triggered"
    return {"message": "Pipeline triggered", "incident_id": incident_id}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
