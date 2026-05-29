from fastapi import APIRouter
import time

from app.models.dummy_model import predict_label

from app.utils.metrics import (
    REQUEST_COUNT,
    FAILED_REQUESTS,
    LATENCY
)

router = APIRouter()


@router.get("/predict")
def predict(text: str):

    REQUEST_COUNT.inc()

    start = time.time()

    try:
        result = predict_label(text)

        LATENCY.observe(
            time.time() - start
        )

        return result

    except Exception as e:

        FAILED_REQUESTS.inc()

        return {
            "status": "error",
            "message": str(e)
        }