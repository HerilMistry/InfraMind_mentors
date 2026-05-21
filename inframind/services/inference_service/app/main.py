from fastapi import FastAPI
from app.routes.predict import router as predict_router
from app.routes.health import router as health_router
from app.utils.metrics import metrics_response

app = FastAPI(title="InfraMind Inference Service")

app.include_router(health_router)
app.include_router(predict_router)

@app.get("/metrics")
def metrics():
    return metrics_response()