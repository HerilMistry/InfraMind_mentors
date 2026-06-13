from fastapi import FastAPI
from app.routes.predict import router as predict_router
from app.routes.health import router as health_router
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="InfraMind Inference Service")

app.include_router(health_router)
app.include_router(predict_router)

Instrumentator().instrument(app).expose(app, endpoint="/metrics")