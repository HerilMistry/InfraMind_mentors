FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install langgraph

# Copy all code
COPY . .

# Set Python path to resolve imports correctly
ENV PYTHONPATH=/app

CMD ["python3", "agents/orchestrator/langgraph_flow.py"]
