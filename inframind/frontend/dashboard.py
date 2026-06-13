"""
frontend/dashboard.py
InfraMind — Cognitive SRE Platform Dashboard (Streamlit).
Connects to the FastAPI inference service, the Prometheus metrics endpoint,
and streams live agent audit logs. Provides HITL approval UI for recovery actions.
"""
import time
import random
import threading
from datetime import datetime, timedelta
from collections import deque
from pathlib import Path
import sys

import streamlit as st
import pandas as pd
import numpy as np
import requests

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="InfraMind | Cognitive SRE Platform",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS (dark glassmorphism) ─────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&family=JetBrains+Mono&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0F1219 0%, #0B0E14 100%);
    border-right: 1px solid rgba(255,255,255,0.06);
}

/* Cards */
div[data-testid="stMetric"] {
    background: rgba(20,24,34,0.65);
    backdrop-filter: blur(12px);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 16px 20px;
    transition: transform 0.2s;
}

div[data-testid="stMetric"]:hover { transform: translateY(-2px); }

/* Terminal box */
.terminal-box {
    background: #0a0d13;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 10px;
    padding: 16px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: #10b981;
    max-height: 340px;
    overflow-y: auto;
    line-height: 1.7;
}

.log-monitoring { color: #60a5fa; }
.log-rca        { color: #a78bfa; }
.log-optim      { color: #fbbf24; }
.log-recovery   { color: #f87171; }
.log-ts         { color: #475569; }

/* Incident card */
.incident-card {
    background: rgba(245,158,11,0.08);
    border: 1px solid rgba(245,158,11,0.25);
    border-radius: 12px;
    padding: 20px;
}

/* Flow step */
.flow-row {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 8px 0;
}
.step-done    { color: #10b981; font-weight: 600; }
.step-active  { color: #60a5fa; font-weight: 600; }
.step-pending { color: #475569; }

/* Causal node */
.causal-node {
    background: rgba(59,130,246,0.08);
    border: 1px solid rgba(59,130,246,0.25);
    border-radius: 8px;
    padding: 8px 14px;
    margin: 4px 0;
    font-size: 13px;
}
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
API_BASE   = "http://localhost:8000"
PROM_BASE  = "http://localhost:9090"

# ─── Session state initialisation ────────────────────────────────────────────
def _init_state():
    defaults = {
        "agent_logs":         deque(maxlen=120),
        "agent_stage":        0,     # 0=monitoring 1=rca 2=optim 3=recovery 4=done
        "incident_active":    False,
        "incident_id":        None,
        "incident_root_cause": None,
        "incident_strategy":  None,
        "hitl_pending":       False,
        "hitl_approved":      None,
        "metrics_history":    {k: deque(maxlen=60) for k in
                               ["cpu", "memory", "latency", "error_rate"]},
        "last_tick":          0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# ─── Simulated metrics ────────────────────────────────────────────────────────
def _tick_metrics():
    """Generate one tick of simulated infrastructure metrics."""
    now = time.time()
    if now - st.session_state["last_tick"] < 1:
        return
    st.session_state["last_tick"] = now

    h = st.session_state["metrics_history"]
    base_memory = 0.88 if st.session_state["incident_active"] else 0.45

    h["cpu"].append(round(random.gauss(40 if not st.session_state["incident_active"] else 78, 6), 1))
    h["memory"].append(round(random.gauss(base_memory, 0.04), 3))
    h["latency"].append(round(random.gauss(18 if not st.session_state["incident_active"] else 340, 12), 1))
    h["error_rate"].append(round(random.gauss(0.01 if not st.session_state["incident_active"] else 0.12, 0.005), 4))

# ─── Agent log stream ─────────────────────────────────────────────────────────
PIPELINE_LOGS = [
    ("[Monitoring]", "Metric Observer collecting data from Prometheus..."),
    ("[Monitoring]", "Pattern Detector found: ['Memory Spike Signature']"),
    ("[Monitoring]", "Trend Analyzer computing momentum — mem +3.2% / min"),
    ("[Monitoring]", "Forecast Agent predicts TTF: 15.0 min"),
    ("[Monitoring]", "Risk Assessment Agent calculated risk: 0.91"),
    ("[Monitoring]", "Consensus Reached — Incident Declared ✓"),
    ("[RCA]",        "Observation Agent scoping incident..."),
    ("[RCA]",        "Evidence Collector querying Loki + K8s API..."),
    ("[RCA]",        "Log Investigator found OOMKilled events on inference-service"),
    ("[RCA]",        "Causal Graph Builder: Memory Spike → OOMKilled → Service Disruption"),
    ("[RCA]",        "Hypothesis Generator: H1=Memory Leak (0.40), H2=Traffic Spike (0.60)"),
    ("[RCA]",        "Hypothesis Verifier: Bayesian update — H2 posterior 0.95"),
    ("[RCA]",        "Consensus Reached — Root Cause: Traffic Spike causing OOM (conf 0.95) ✓"),
    ("[Optimization]","Strategy Generator building candidate plans..."),
    ("[Optimization]","Cost Analysis: strat-scale=$25 / strat-restart=$5"),
    ("[Optimization]","Safety Analysis: strat-scale=0.95 / strat-restart=0.60"),
    ("[Optimization]","Pareto Ranking: strat-scale wins (safety × 100 − cost = 70)"),
    ("[Optimization]","Consensus Reached — Selected: SCALE_UP inference-deployment → 5 replicas ✓"),
    ("[Recovery]",   "Policy Validation: no maintenance window active ✓"),
    ("[Recovery]",   "Recovery Planner: generating K8s JSON patch..."),
    ("[Recovery]",   "Failure Simulator: kubectl --dry-run=server → PASS ✓"),
    ("[Recovery]",   "Kubernetes Executor: applying patch to cluster..."),
    ("[Recovery]",   "Recovery Verifier: latency stabilised at 22ms (baseline 18ms) ✓"),
    ("[Recovery]",   "Consensus Reached — Incident INC-8492 RESOLVED ✓"),
]

STAGE_LABELS = {
    "[Monitoring]":   "log-monitoring",
    "[RCA]":          "log-rca",
    "[Optimization]": "log-optim",
    "[Recovery]":     "log-recovery",
}

def _append_log(tag: str, msg: str):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    st.session_state["agent_logs"].append((ts, tag, msg))

def _simulate_pipeline_step():
    idx = st.session_state["agent_stage"]
    if idx >= len(PIPELINE_LOGS):
        return
    tag, msg = PIPELINE_LOGS[idx]
    _append_log(tag, msg)
    st.session_state["agent_stage"] = idx + 1

    # Track which high-level phase we are in
    if tag == "[RCA]" and "Root Cause" in msg:
        st.session_state["incident_root_cause"] = "Traffic Spike causing OOM"
    if tag == "[Optimization]" and "SCALE_UP" in msg:
        st.session_state["incident_strategy"] = "SCALE_UP inference-deployment → 5 replicas"
        st.session_state["hitl_pending"] = True
    if tag == "[Recovery]" and "RESOLVED" in msg:
        st.session_state["incident_active"] = False
        st.session_state["hitl_pending"] = False
        st.session_state["incident_active"] = False

# ─── Prometheus query helper ──────────────────────────────────────────────────
def _prom_query(query: str) -> float | None:
    try:
        r = requests.get(f"{PROM_BASE}/api/v1/query", params={"query": query}, timeout=2)
        data = r.json()["data"]["result"]
        if data:
            return float(data[0]["value"][1])
    except Exception:
        pass
    return None

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🧠 InfraMind")
    st.markdown("*Cognitive SRE Platform*")
    st.divider()

    st.markdown("### Cluster")
    col1, col2 = st.columns(2)
    col1.metric("Nodes",    "3 / 3")
    col2.metric("Pods",     "14")
    st.metric("Namespace", "inframind")

    st.divider()
    st.markdown("### Controls")

    if st.button("🚨 Trigger Synthetic Incident", use_container_width=True):
        st.session_state["incident_active"]   = True
        st.session_state["incident_id"]       = f"INC-{random.randint(1000,9999)}"
        st.session_state["agent_stage"]       = 0
        st.session_state["hitl_pending"]      = False
        st.session_state["incident_root_cause"] = None
        st.session_state["incident_strategy"]   = None
        for k in st.session_state["metrics_history"]:
            st.session_state["metrics_history"][k].clear()

    if st.button("⏩ Advance Agent Step", use_container_width=True):
        _simulate_pipeline_step()

    if st.button("⚡ Run Full Pipeline", use_container_width=True):
        for _ in PIPELINE_LOGS:
            _simulate_pipeline_step()

    if st.button("🔄 Reset", use_container_width=True, type="secondary"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    st.divider()
    st.markdown("### Links")
    st.markdown("- [Grafana ↗](http://localhost:3000)")
    st.markdown("- [Prometheus ↗](http://localhost:9090)")
    st.markdown("- [MLflow ↗](http://localhost:5000)")
    st.markdown("- [FastAPI Docs ↗](http://localhost:8000/docs)")

# ─── Main layout ─────────────────────────────────────────────────────────────
_tick_metrics()
h = st.session_state["metrics_history"]

# ── Row 1: System Health ──────────────────────────────────────────────────────
st.markdown("## 📊 Infrastructure Health")
c1, c2, c3, c4 = st.columns(4)

cpu_val  = h["cpu"][-1]      if h["cpu"]      else 40.0
mem_val  = h["memory"][-1]   if h["memory"]   else 0.45
lat_val  = h["latency"][-1]  if h["latency"]  else 18.0
err_val  = h["error_rate"][-1] if h["error_rate"] else 0.01

c1.metric("CPU Usage",       f"{cpu_val:.1f}%",  delta=f"{cpu_val - 40:.1f}%")
c2.metric("Memory Pressure", f"{mem_val*100:.1f}%", delta=f"{(mem_val-0.45)*100:.1f}%")
c3.metric("Latency (p99)",   f"{lat_val:.0f} ms",   delta=f"{lat_val - 18:.0f} ms")
c4.metric("Error Rate",      f"{err_val*100:.2f}%",  delta=f"{(err_val-0.01)*100:.2f}%")

# Charts
if h["cpu"]:
    df_hist = pd.DataFrame({
        "CPU %":         list(h["cpu"]),
        "Memory %":      [v*100 for v in h["memory"]],
        "Latency (ms)":  list(h["latency"]),
    })
    st.line_chart(df_hist, use_container_width=True, height=200)

st.divider()

# ── Row 2: Orchestrator Flow + HITL ──────────────────────────────────────────
left, right = st.columns([1.3, 1])

with left:
    st.markdown("## ⚙️ Cognitive Orchestrator Flow")
    stage = st.session_state["agent_stage"]

    def _flow_row(icon, label, done_idx):
        if stage > done_idx:
            st.markdown(f'<div class="flow-row"><span class="step-done">✅ {icon} {label}</span></div>', unsafe_allow_html=True)
        elif stage == done_idx:
            st.markdown(f'<div class="flow-row"><span class="step-active">⚡ {icon} {label} — running...</span></div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="flow-row"><span class="step-pending">⬜ {icon} {label}</span></div>', unsafe_allow_html=True)

    _flow_row("👁",  "Monitoring SubGraph  (6 cognitive nodes)", 6)
    st.markdown('<div style="margin-left:20px;color:#374151;">↓</div>', unsafe_allow_html=True)
    _flow_row("🔍", "Root Cause SubGraph  (7 cognitive nodes)", 13)
    st.markdown('<div style="margin-left:20px;color:#374151;">↓</div>', unsafe_allow_html=True)
    _flow_row("⚖",  "Optimization SubGraph  (7 cognitive nodes)", 18)
    st.markdown('<div style="margin-left:20px;color:#374151;">↓</div>', unsafe_allow_html=True)
    _flow_row("⚕",  "Recovery SubGraph  (7 cognitive nodes)", 24)

    # Causal Graph
    if st.session_state.get("incident_root_cause"):
        st.markdown("---")
        st.markdown("**🕸 Causal Graph**")
        for node in [
            "Memory Spike  →  OOMKilled",
            "OOMKilled     →  Service Disruption",
            "Traffic Spike →  Memory Spike (root)",
        ]:
            st.markdown(f'<div class="causal-node">⚡ {node}</div>', unsafe_allow_html=True)

with right:
    # Active incident card
    if st.session_state["incident_active"] or st.session_state.get("incident_id"):
        inc_id = st.session_state.get("incident_id", "—")
        st.markdown("## 🚨 Active Incident")
        st.markdown(f"""
<div class="incident-card">
  <p style="color:#fbbf24;font-family:monospace;font-size:13px;">{inc_id} &nbsp;·&nbsp; {datetime.utcnow().strftime('%H:%M:%S UTC')}</p>
  <h4 style="margin:8px 0;">Inference Service OOM Risk</h4>
  <p style="color:#94a3b8;font-size:13px;">Sudden traffic spike is causing memory pressure on inference-deployment pods.</p>
</div>
""", unsafe_allow_html=True)

        if st.session_state.get("incident_root_cause"):
            st.info(f"🔬 **Root Cause:** {st.session_state['incident_root_cause']}")

        if st.session_state.get("incident_strategy"):
            st.markdown("**Proposed Remediation:**")
            st.code(f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: inference-deployment
spec:
  replicas: 5  # scaled up from 3
  # Strategy: {st.session_state['incident_strategy']}""", language="yaml")

        # HITL Approval block
        if st.session_state.get("hitl_pending"):
            st.warning("⚠️ **Human-in-the-Loop: Approval Required**")
            bcol1, bcol2 = st.columns(2)
            if bcol1.button("✅ Approve & Execute", type="primary", use_container_width=True):
                st.session_state["hitl_pending"] = False
                st.session_state["hitl_approved"] = True
                _append_log("[Recovery]", "HITL approval received. Executing remediation...")
                for _ in range(6):   # run remaining recovery steps
                    _simulate_pipeline_step()
                st.success("Recovery pipeline approved and executing!")
                st.rerun()
            if bcol2.button("❌ Reject", use_container_width=True):
                st.session_state["hitl_pending"] = False
                st.session_state["hitl_approved"] = False
                _append_log("[Recovery]", "HITL action REJECTED by operator. Incident escalated.")
                st.error("Action rejected. Incident escalated.")
                st.rerun()
    else:
        st.markdown("## ✅ System Nominal")
        st.success("All services healthy. No active incidents.")

st.divider()

# ── Row 3: Terminal + Inference Tester ───────────────────────────────────────
t_col, i_col = st.columns([1.5, 1])

with t_col:
    st.markdown("## 🖥 Agent Audit Log")
    logs = list(st.session_state["agent_logs"])
    if logs:
        rendered = ""
        for ts, tag, msg in logs[-30:]:  # last 30 lines
            css_class = STAGE_LABELS.get(tag, "")
            rendered += (
                f'<span class="log-ts">[{ts}]</span> '
                f'<span class="{css_class}">{tag}</span> {msg}<br>'
            )
        st.markdown(f'<div class="terminal-box">{rendered}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="terminal-box">No agent activity yet. Trigger an incident from the sidebar.</div>', unsafe_allow_html=True)

with i_col:
    st.markdown("## 🔮 Live Inference Tester")
    test_input = st.text_area("Input payload", value="Check if this service is under load.", height=100)
    if st.button("Send to Inference API", use_container_width=True):
        try:
            resp = requests.post(f"{API_BASE}/predict", json={"input_text": test_input}, timeout=5)
            st.json(resp.json())
        except Exception as e:
            st.error(f"API unreachable: {e}")

    st.markdown("---")
    st.markdown("### MLflow Last Run")
    mlflow_data = {
        "Experiment": "inframind-anomaly-detection",
        "Model":      "IsolationForest",
        "val_f1":     "0.821",
        "val_acc":    "0.893",
        "Stage":      "Staging",
    }
    st.dataframe(pd.DataFrame([mlflow_data]), use_container_width=True)

st.divider()

# ── Row 4: Dataset preview ────────────────────────────────────────────────────
st.markdown("## 📂 Training Dataset Sample")
data_path = Path(__file__).parent / ".." / "data" / "metrics_dataset.csv"
if data_path.exists():
    df = pd.read_csv(data_path)
    col_l, col_r = st.columns(2)
    col_l.dataframe(df.head(15), use_container_width=True)
    col_r.markdown("**Class Distribution**")
    col_r.bar_chart(df["is_anomaly"].value_counts().rename({0: "Normal", 1: "Anomaly"}))
else:
    st.warning("Dataset not found. Run `python3 -c 'from data_gen import gen; gen()'` first.")

# Auto-refresh every 2 s when an incident is active
if st.session_state.get("incident_active"):
    time.sleep(0.5)
    st.rerun()
