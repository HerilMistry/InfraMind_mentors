from fastapi import APIRouter, Body
import time
import os

from app.models.dummy_model import predict_label
from app.models.micro_model import MicroModelClient

from app.utils.metrics import (
    REQUEST_COUNT,
    FAILED_REQUESTS,
    LATENCY
)

router = APIRouter()
micro_enabled = os.getenv("USE_MICRO_MODEL", "false").lower() == "true"


@router.post("/predict")
async def predict(payload: dict = Body(...)):

    REQUEST_COUNT.inc()

    start = time.time()

    try:
        input_text = payload.get("input_text", "")
        if micro_enabled:
            client = MicroModelClient()
            result = await client.generate(input_text)
        else:
            result = predict_label(input_text)

        LATENCY.observe(
            time.time() - start
        )

        return {"prediction": result}

    except Exception as e:

        FAILED_REQUESTS.inc()

        return {
            "status": "error",
            "message": str(e)
        }