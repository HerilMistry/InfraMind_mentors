import random
import time

def predict_label(text: str):
    time.sleep(random.uniform(0.05, 0.35))

    if not text.strip():
        return {"prediction": "empty", "confidence": 0.0}

    return {
        "prediction": "normal",
        "confidence": round(random.uniform(0.75, 0.99), 3)
    }