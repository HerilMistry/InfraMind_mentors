FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all agent code
COPY . .

# Set Python path so intra-package imports resolve correctly
ENV PYTHONPATH=/app

CMD uvicorn agents.orchestrator.server:app --host 0.0.0.0 --port ${PORT:-8080}
