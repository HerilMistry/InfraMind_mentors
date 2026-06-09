import os
import httpx

class MicroModelClient:
    """Thin wrapper around the phi3‑mini‑instruct side‑car.

    Communicates via HTTP on localhost:8001 (exposed by the side‑car).
    """
    def __init__(self) -> None:
        self.base_url = os.getenv(
            "MICRO_MODEL_URL",
            "http://127.0.0.1:8001/v1/completions",
        )
        self.headers = {"Content-Type": "application/json"}

    async def generate(self, prompt: str, max_tokens: int = 128) -> str:
        payload = {
            "model": "phi3-mini-instruct",
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(self.base_url, json=payload, headers=self.headers)
            resp.raise_for_status()
            return resp.json()["choices"][0]["text"]
