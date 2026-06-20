<div align="center">

<img src="https://img.shields.io/badge/InfraMind-Cognitive%20SRE-6366F1?style=for-the-badge&logo=kubernetes&logoColor=white" alt="InfraMind"/>

# 🧠 InfraMind — Cognitive SRE Platform

**Autonomous infrastructure intelligence powered by multi-agent AI.**  
*Monitor → Detect → Diagnose → Optimize → Recover — all on autopilot.*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35+-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111+-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![LangGraph](https://img.shields.io/badge/LangGraph-Multi--Agent-4B0082?style=flat-square)](https://langchain-ai.github.io/langgraph)
[![MLflow](https://img.shields.io/badge/MLflow-Experiment%20Tracking-0194E2?style=flat-square&logo=mlflow&logoColor=white)](https://mlflow.org)
[![Groq](https://img.shields.io/badge/Groq-LLaMA%203.3%2070B-F55036?style=flat-square)](https://groq.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestration-326CE5?style=flat-square&logo=kubernetes&logoColor=white)](https://kubernetes.io)

</div>

---

## 🚀 What is InfraMind?

InfraMind is a **production-grade, autonomous Site Reliability Engineering (SRE) platform** that uses a network of cognitive AI agents to continuously monitor your Kubernetes infrastructure. When something goes wrong, InfraMind doesn't just alert you — it **diagnoses the root cause, selects an optimal remediation strategy, and executes recovery actions**, all with full auditability and optional human-in-the-loop approval.

Built for DevOps teams who want to move from reactive firefighting to proactive, AI-driven operations.

---

## ✨ Key Features

| Feature | Description |
|--------|-------------|
| 🔍 **Anomaly Detection** | IsolationForest model continuously monitors CPU, memory, latency, and error rates |
| 🧠 **Multi-Agent AI** | 4 LangGraph subgraphs with 27 cognitive nodes work in parallel |
| 🕸 **Causal Graph RCA** | Bayesian inference builds causal chains to pinpoint root causes |
| ⚖ **Pareto Optimization** | Cost/safety trade-off ranking selects the best remediation strategy |
| 🔁 **Auto-Recovery** | Kubernetes patching with dry-run validation before execution |
| 🙋 **Human-in-the-Loop** | Optional approval gate before executing high-impact actions |
| 💬 **AI Assistant** | Groq-powered LLaMA 3.3 chatbot answers pipeline and architecture questions |
| 📊 **Live Dashboard** | Real-time Streamlit UI with glassmorphism design |
| 🧪 **MLflow Tracking** | Full experiment lifecycle management with model staging |
| 📡 **Prometheus/Grafana** | Full observability stack integration |

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    InfraMind Platform                     │
├──────────────────────────────────────────────────────────┤
│                                                          │
│   ┌─────────────┐    ┌──────────────────────────────┐   │
│   │  Streamlit  │    │     LangGraph Orchestrator   │   │
│   │  Dashboard  │◄───│  ┌──────┐ ┌─────┐ ┌──────┐  │   │
│   │  + Chatbot  │    │  │ Mon. │►│ RCA │►│ Opt. │  │   │
│   └─────────────┘    │  └──────┘ └─────┘ └──┬───┘  │   │
│                      │                    ┌──▼───┐  │   │
│   ┌─────────────┐    │                    │ Rec. │  │   │
│   │  FastAPI    │    │                    └──────┘  │   │
│   │  Inference  │    └──────────────────────────────┘   │
│   │  Service    │                                        │
│   └─────────────┘    ┌──────────┐  ┌──────────────┐    │
│                      │  MLflow  │  │  Prometheus  │    │
│   ┌─────────────┐    │ Tracking │  │  + Grafana   │    │
│   │ Kubernetes  │    └──────────┘  └──────────────┘    │
│   │  Cluster   │                                        │
│   └─────────────┘                                       │
└──────────────────────────────────────────────────────────┘
```

### Agent Subgraphs

#### 👁 Monitoring SubGraph (6 nodes)
Continuously samples Prometheus metrics, detects anomaly signatures, computes trend momentum, forecasts time-to-failure, and calculates a composite risk score. Declares incidents when consensus risk exceeds threshold.

#### 🔍 Root Cause Analysis SubGraph (7 nodes)
Scopes the incident, collects evidence from Loki and the Kubernetes API, builds a causal graph, generates hypotheses, applies Bayesian updates, and reaches consensus on the root cause with confidence scores.

#### ⚖ Optimization SubGraph (7 nodes)
Generates candidate remediation strategies, analyzes cost and safety for each, applies Pareto ranking, and selects the optimal action. Supports HITL approval gate before execution.

#### ⚕ Recovery SubGraph (7 nodes)
Validates policy constraints, generates Kubernetes JSON patches, runs dry-run server-side validation, executes the patch, and verifies recovery by monitoring metric stabilization.

---

## 📂 Project Structure

```
inframind/
├── agents/
│   ├── monitoring_agent/     # Prometheus scraping + pattern detection
│   ├── root_cause_agent/     # Causal graph + Bayesian RCA
│   ├── optimization_agent/   # Strategy ranking + Pareto selection
│   ├── recovery_agent/       # K8s patching + verification
│   ├── orchestrator/         # LangGraph multi-agent coordinator
│   ├── pf_analyzer/          # Performance analysis utilities
│   └── shared/               # Common models and utilities
├── frontend/
│   ├── dashboard.py          # Streamlit app (main UI + chatbot)
│   ├── css/style.css         # Premium glassmorphism design system
│   ├── requirements.txt      # Frontend dependencies
│   └── .streamlit/config.toml
├── services/                 # FastAPI inference service
├── mlops/                    # MLflow training pipeline
├── data/                     # Synthetic dataset generation
├── kubernetes/               # K8s manifests + Helm charts
├── observability/            # Prometheus + Grafana configs
├── simulator/                # Incident simulation tools
├── ci_cd/                    # GitHub Actions pipelines
├── render.yaml               # Render cloud deployment
├── docker-compose.yml        # Local full-stack setup
├── requirements.txt          # Root dependencies
└── .env                      # Environment variables
```

---

## ⚡ Quick Start

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- A running Kubernetes cluster (or minikube for local dev)
- Groq API key → [console.groq.com](https://console.groq.com)

### 1. Clone & Configure

```bash
git clone https://github.com/your-org/inframind.git
cd inframind

# Copy and configure environment variables
cp .env.example .env
```

Edit `.env`:

```env
GROQ_API_KEY=gsk_your_groq_api_key_here

# Observability
PROM_URL=http://localhost:9090

# MLflow
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT=inframind-anomaly-detection
MLFLOW_MODEL_NAME=inframind-anomaly-detector

# API
API_BASE=http://localhost:8000
```

### 2. Local Development (Docker Compose)

```bash
docker-compose up --build
```

This starts:
- FastAPI inference service on `:8000`
- MLflow tracking server on `:5000`
- Prometheus on `:9090`
- Grafana on `:3000`

### 3. Run the Dashboard

```bash
cd frontend
pip install -r requirements.txt
streamlit run dashboard.py
```

Open [http://localhost:8501](http://localhost:8501)

### 4. Deploy to Kubernetes (Minikube)

```bash
# Start minikube
minikube start --cpus=4 --memory=8g

# Build and load Docker images
docker build -t inframind-api:latest ./services
minikube image load inframind-api:latest

# Apply manifests
kubectl apply -f kubernetes/

# Check pods
kubectl get pods -n inframind
```

---

## 🖥 Dashboard Guide

The InfraMind dashboard has **4 tabs**:

| Tab | Contents |
|-----|----------|
| 📊 **Overview** | Live metric cards, time-series charts, system status, MLflow model info |
| ⚙️ **Pipeline** | Cognitive orchestrator flow, causal graph, agent audit log |
| 🔮 **Inference** | Live inference tester to send payloads to the FastAPI service |
| 💬 **AI Assistant** | Groq-powered chatbot for pipeline/architecture questions |

### Simulating an Incident

1. Click **🚨 Trigger Incident** in the sidebar
2. Watch metrics spike (CPU → ~78%, latency → ~340ms)
3. Use **⏩ Step** to advance the pipeline one node at a time
4. Or use **⚡ Full Run** to execute all 24 pipeline steps instantly
5. Approve or reject the HITL remediation gate
6. Watch the system self-heal

---

## 🤖 AI Chatbot

The built-in **InfraMind Assistant** is powered by **Groq's LLaMA 3.3 70B** model and is context-aware of the entire platform architecture. It can answer questions like:

- *"How does the RCA subgraph determine the root cause?"*
- *"What is the Pareto ranking strategy?"*
- *"How does HITL approval work?"*
- *"What metrics trigger an incident?"*
- *"Explain the IsolationForest model used here"*

No external API calls go through your cluster — the chatbot communicates directly with Groq from the frontend.

---

## 🧪 ML Pipeline

InfraMind uses a **scikit-learn IsolationForest** model for real-time anomaly detection:

| Metric | Value |
|--------|-------|
| Model  | IsolationForest |
| Val F1 | 0.821 |
| Val Accuracy | 0.893 |
| Stage | Staging |
| Experiment | `inframind-anomaly-detection` |

To retrain:

```bash
cd mlops
python train.py
```

MLflow automatically tracks parameters, metrics, and artifacts. Promoted models are served via the FastAPI inference endpoint.

---

## 🌐 Cloud Deployment

### Render

```bash
# Deploy using render.yaml (already configured)
render deploy
```

### Environment Variables for Production

Set these in your cloud provider's secrets manager:

```
GROQ_API_KEY
MLFLOW_TRACKING_URI
API_BASE
PROM_URL
GRAFANA_URL
```

---

## 🛡 Security & HITL

InfraMind enforces a **Human-in-the-Loop (HITL)** gate before executing high-impact Kubernetes actions:

1. Optimization subgraph selects remediation strategy
2. Dashboard shows the proposed action and requires operator approval
3. **Approve** → Recovery subgraph executes patch with dry-run validation
4. **Reject** → Incident escalated to on-call team

All agent decisions are logged in the **Agent Audit Log** with timestamps and reasoning.

---

## 🤝 Contributing

```bash
# Fork → clone → branch
git checkout -b feat/your-feature

# Make changes, test
python -m pytest tests/

# PR with clear description
git push origin feat/your-feature
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

Built with ❤️ by the InfraMind team · Powered by LangGraph, Groq, MLflow & Kubernetes

</div>
