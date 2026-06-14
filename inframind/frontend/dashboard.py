import time
import random
import os
from datetime import datetime
from collections import deque
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

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

# ─── Load CSS ─────────────────────────────────────────────────────────────────
css_path = Path(__file__).parent / "css" / "style.css"
if css_path.exists():
    with open(css_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
API_BASE  = os.getenv("API_BASE",  "http://localhost:8000")
PROM_BASE = os.getenv("PROM_BASE", "http://localhost:9090")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# ─── Session State ────────────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "agent_logs":          deque(maxlen=120),
        "agent_stage":         0,
        "incident_active":     False,
        "incident_id":         None,
        "incident_root_cause": None,
        "incident_strategy":   None,
        "hitl_pending":        False,
        "hitl_approved":       None,
        "metrics_history":     {k: deque(maxlen=60) for k in ["cpu","memory","latency","error_rate"]},
        "last_tick":           0,
        "chat_messages":       [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# ─── Metrics ──────────────────────────────────────────────────────────────────
def _tick_metrics():
    now = time.time()
    if now - st.session_state["last_tick"] < 1:
        return
    st.session_state["last_tick"] = now
    h = st.session_state["metrics_history"]
    inc = st.session_state["incident_active"]
    h["cpu"].append(round(random.gauss(78 if inc else 40, 6), 1))
    h["memory"].append(round(random.gauss(0.88 if inc else 0.45, 0.04), 3))
    h["latency"].append(round(random.gauss(340 if inc else 18, 12), 1))
    h["error_rate"].append(round(random.gauss(0.12 if inc else 0.01, 0.005), 4))

# ─── Pipeline logs ────────────────────────────────────────────────────────────
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
    ("[RCA]",        "Causal Graph: Memory Spike → OOMKilled → Service Disruption"),
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
    ("[Recovery]",   "Consensus Reached — Incident RESOLVED ✓"),
]

STAGE_LABELS = {
    "[Monitoring]":   "log-monitoring",
    "[RCA]":          "log-rca",
    "[Optimization]": "log-optim",
    "[Recovery]":     "log-recovery",
}

def _append_log(tag, msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    st.session_state["agent_logs"].append((ts, tag, msg))

def _simulate_pipeline_step():
    idx = st.session_state["agent_stage"]
    if idx >= len(PIPELINE_LOGS):
        return
    tag, msg = PIPELINE_LOGS[idx]
    _append_log(tag, msg)
    st.session_state["agent_stage"] = idx + 1
    if tag == "[RCA]" and "Root Cause" in msg:
        st.session_state["incident_root_cause"] = "Traffic Spike causing OOM"
    if tag == "[Optimization]" and "SCALE_UP" in msg:
        st.session_state["incident_strategy"] = "SCALE_UP inference-deployment → 5 replicas"
        st.session_state["hitl_pending"] = True
    if tag == "[Recovery]" and "RESOLVED" in msg:
        st.session_state["incident_active"] = False
        st.session_state["hitl_pending"] = False

# ─── Groq Chatbot ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are InfraMind Assistant, an expert AI embedded in the InfraMind Cognitive SRE Platform.
InfraMind is an autonomous site reliability engineering system that uses multi-agent AI to monitor Kubernetes infrastructure, detect anomalies, perform root cause analysis (RCA), optimize resources, and execute automated recovery actions.

Key components:
- Monitoring SubGraph: 6 cognitive nodes collecting Prometheus metrics, detecting patterns, forecasting failures
- Root Cause Analysis (RCA) SubGraph: 7 nodes using causal graphs and Bayesian inference
- Optimization SubGraph: 7 nodes doing cost/safety trade-off analysis with Pareto ranking
- Recovery SubGraph: 7 nodes for policy validation, dry-run testing, Kubernetes patching

The ML model is an IsolationForest trained for anomaly detection (val_f1=0.821, val_acc=0.893, stage=Staging).
MLflow tracks experiments. Grafana/Prometheus handle observability.

Answer questions about the pipeline, incidents, architecture, metrics, and how the system works.
Be concise, accurate, and professional."""

def call_groq(messages: list) -> str:
    if not GROQ_API_KEY:
        return "⚠️ GROQ_API_KEY not configured. Please set it in your environment variables."
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
                "max_tokens": 512,
                "temperature": 0.6,
            },
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        return "⚠️ Request timed out. Please try again."
    except Exception as e:
        return f"⚠️ Error: {str(e)}"

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <div class="brand-orb">🧠</div>
        <div class="brand-text">
            <h2>InfraMind</h2>
            <small>Cognitive SRE Platform</small>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="cluster-card">
        <h4>Live Cluster</h4>
        <div class="cluster-row">
            <div class="cluster-item">
                <div class="val">3/3</div>
                <div class="lbl">Nodes</div>
            </div>
            <div class="cluster-item">
                <div class="val">14</div>
                <div class="lbl">Pods</div>
            </div>
            <div class="cluster-item">
                <div class="val">4</div>
                <div class="lbl">Agents</div>
            </div>
        </div>
        <div style="margin-top:10px;font-size:0.75rem;color:#8892AA;">
            <span class="live-dot"></span>
            <span>inframind namespace · healthy</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Pipeline Controls**")

    if st.button("🚨  Trigger Incident", use_container_width=True):
        st.session_state["incident_active"]     = True
        st.session_state["incident_id"]         = f"INC-{random.randint(1000,9999)}"
        st.session_state["agent_stage"]         = 0
        st.session_state["hitl_pending"]        = False
        st.session_state["incident_root_cause"] = None
        st.session_state["incident_strategy"]   = None
        for k in st.session_state["metrics_history"]:
            st.session_state["metrics_history"][k].clear()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("⏩ Step", use_container_width=True):
            _simulate_pipeline_step()
    with col2:
        if st.button("⚡ Full Run", use_container_width=True):
            for _ in PIPELINE_LOGS:
                _simulate_pipeline_step()

    if st.button("↺  Reset", use_container_width=True, type="secondary"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    st.markdown("---")
    st.markdown("**Observability Links**")
    grafana_url  = os.getenv("GRAFANA_URL",  "http://localhost:3000")
    mlflow_url   = os.getenv("MLFLOW_URL",   "http://localhost:5000")
    st.markdown(f"""
    <a class="link-item" href="{grafana_url}" target="_blank">📊 &nbsp;Grafana Dashboard</a>
    <a class="link-item" href="{PROM_BASE}" target="_blank">🔥 &nbsp;Prometheus</a>
    <a class="link-item" href="{mlflow_url}" target="_blank">🧪 &nbsp;MLflow Experiments</a>
    <a class="link-item" href="{API_BASE}/docs" target="_blank">📖 &nbsp;API Reference</a>
    """, unsafe_allow_html=True)

# ─── Main Content ─────────────────────────────────────────────────────────────
_tick_metrics()
h = st.session_state["metrics_history"]

# ── Page Header ──
cpu_val = h["cpu"][-1]      if h["cpu"]      else 40.0
mem_val = h["memory"][-1]   if h["memory"]   else 0.45
lat_val = h["latency"][-1]  if h["latency"]  else 18.0
err_val = h["error_rate"][-1] if h["error_rate"] else 0.01

inc_active = st.session_state["incident_active"]

st.markdown("""
<div class="page-title">Infrastructure Intelligence</div>
<div class="page-sub">Real-time cognitive monitoring · Autonomous remediation · AI-powered insights</div>
""", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_pipeline, tab_inference, tab_chat = st.tabs([
    "📊  Overview", "⚙️  Pipeline", "🔮  Inference", "💬  AI Assistant"
])

# ═══════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════
with tab_overview:
    # Metric row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("CPU Usage",       f"{cpu_val:.1f}%",        delta=f"{cpu_val-40:.1f}%")
    c2.metric("Memory Pressure", f"{mem_val*100:.1f}%",    delta=f"{(mem_val-0.45)*100:.1f}%")
    c3.metric("Latency (p99)",   f"{lat_val:.0f} ms",      delta=f"{lat_val-18:.0f} ms")
    c4.metric("Error Rate",      f"{err_val*100:.2f}%",    delta=f"{(err_val-0.01)*100:.2f}%")

    st.markdown("<br>", unsafe_allow_html=True)

    # Chart
    if h["cpu"]:
        df_hist = pd.DataFrame({
            "CPU %":        list(h["cpu"]),
            "Memory %":     [v * 100 for v in h["memory"]],
            "Latency (ms)": list(h["latency"]),
        })
        st.line_chart(df_hist, use_container_width=True, height=220)

    st.markdown("<br>", unsafe_allow_html=True)

    # Status + model info
    left, right = st.columns([1, 1])

    with left:
        if inc_active or st.session_state.get("incident_id"):
            inc_id = st.session_state.get("incident_id", "—")
            st.markdown(f"""
            <div class="status-incident">
                <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.1em;color:#EF4444;font-weight:700;margin-bottom:6px;">
                    🚨 Active Incident
                </div>
                <div style="font-size:1.2rem;font-weight:800;margin-bottom:4px;">{inc_id}</div>
                <div style="font-size:0.82rem;color:#8892AA;">High-severity anomaly detected · Auto-remediation in progress</div>
            </div>
            """, unsafe_allow_html=True)
            if st.session_state.get("incident_root_cause"):
                st.info(f"🔬 **Root Cause:** {st.session_state['incident_root_cause']}")
            if st.session_state.get("incident_strategy"):
                st.markdown("**Proposed Remediation:**")
                st.code(st.session_state['incident_strategy'], language="yaml")
            if st.session_state.get("hitl_pending"):
                st.warning("⚠️ **Human-in-the-Loop: Approval Required**")
                b1, b2 = st.columns(2)
                if b1.button("✅ Approve & Execute", type="primary", use_container_width=True):
                    st.session_state["hitl_pending"] = False
                    st.session_state["hitl_approved"] = True
                    _append_log("[Recovery]", "HITL approval received. Executing remediation...")
                    for _ in range(6):
                        _simulate_pipeline_step()
                    st.rerun()
                if b2.button("❌ Reject", use_container_width=True):
                    st.session_state["hitl_pending"] = False
                    st.session_state["hitl_approved"] = False
                    _append_log("[Recovery]", "HITL action REJECTED. Incident escalated.")
                    st.rerun()
        else:
            st.markdown("""
            <div class="status-nominal">
                <span class="icon">✅</span>
                <div class="title">All Systems Operational</div>
                <div class="sub">No active incidents · All agents healthy · Monitoring active</div>
            </div>
            """, unsafe_allow_html=True)

    with right:
        st.markdown("""
        <div class="mlflow-badge">🧪 MLflow · Last Run</div>
        """, unsafe_allow_html=True)
        mlflow_data = {
            "Experiment":  "inframind-anomaly-detection",
            "Model":       "IsolationForest",
            "Val F1":      "0.821",
            "Val Acc":     "0.893",
            "Stage":       "Staging",
        }
        st.dataframe(pd.DataFrame([mlflow_data]), use_container_width=True, hide_index=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("""
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
            <span class="stat-pill green">✓ Model Healthy</span>
            <span class="stat-pill blue">IsolationForest</span>
            <span class="stat-pill yellow">Staging</span>
        </div>
        """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# TAB 2 — PIPELINE
# ═══════════════════════════════════════════════════════
with tab_pipeline:
    left_p, right_p = st.columns([1.2, 1])

    with left_p:
        st.markdown("""
        <div class="section-heading">
            <div class="icon">⚙️</div>
            Cognitive Orchestrator Flow
        </div>
        """, unsafe_allow_html=True)

        stage = st.session_state["agent_stage"]

        def _flow_row(icon, label, done_idx):
            if stage > done_idx:
                st.markdown(f'<div class="flow-row"><span class="step-done">✅ {icon} {label}</span></div>', unsafe_allow_html=True)
            elif stage == done_idx:
                st.markdown(f'<div class="flow-row"><span class="step-active">⚡ {icon} {label} — running...</span></div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="flow-row"><span class="step-pending">◯ {icon} {label}</span></div>', unsafe_allow_html=True)
            st.markdown('<div style="margin-left:16px;color:#2D3748;font-size:0.8rem;padding:2px 0;">│</div>', unsafe_allow_html=True)

        _flow_row("👁", "Monitoring SubGraph  ·  6 cognitive nodes", 6)
        _flow_row("🔍", "Root Cause SubGraph  ·  7 cognitive nodes", 13)
        _flow_row("⚖", "Optimization SubGraph  ·  7 cognitive nodes", 18)
        _flow_row("⚕", "Recovery SubGraph  ·  7 cognitive nodes", 24)

        if st.session_state.get("incident_root_cause"):
            st.markdown("---")
            st.markdown("""
            <div class="section-heading" style="font-size:0.95rem;">
                <div class="icon">🕸</div> Causal Graph
            </div>
            """, unsafe_allow_html=True)
            for node in [
                "Traffic Spike  →  Memory Spike",
                "Memory Spike   →  OOMKilled",
                "OOMKilled      →  Service Disruption",
            ]:
                st.markdown(f'<div class="causal-node">⚡ {node}</div>', unsafe_allow_html=True)

    with right_p:
        st.markdown("""
        <div class="section-heading">
            <div class="icon">🖥</div>
            Agent Audit Log
        </div>
        """, unsafe_allow_html=True)
        logs = list(st.session_state["agent_logs"])
        if logs:
            rendered = ""
            for ts, tag, msg in logs[-30:]:
                css_class = STAGE_LABELS.get(tag, "")
                rendered += (
                    f'<span class="log-ts">[{ts}]</span> '
                    f'<span class="{css_class}">{tag}</span> {msg}<br>'
                )
            st.markdown(f'<div class="terminal-box">{rendered}</div>', unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="terminal-box">No agent activity yet.<br>Trigger an incident from the sidebar to start the pipeline.</div>',
                unsafe_allow_html=True
            )

# ═══════════════════════════════════════════════════════
# TAB 3 — INFERENCE
# ═══════════════════════════════════════════════════════
with tab_inference:
    st.markdown("""
    <div class="section-heading">
        <div class="icon">🔮</div>
        Live Inference Tester
    </div>
    """, unsafe_allow_html=True)

    inf_col, info_col = st.columns([1, 1])
    with inf_col:
        test_input = st.text_area(
            "Payload",
            value="Check if this service is under load.",
            height=120,
            label_visibility="collapsed"
        )
        if st.button("Send to Inference API →", use_container_width=True):
            try:
                resp = requests.post(
                    f"{API_BASE}/predict",
                    json={"input_text": test_input},
                    timeout=5
                )
                st.json(resp.json())
            except Exception as e:
                st.error(f"API unreachable: {e}")

    with info_col:
        st.markdown("""
        <div style="padding:20px;background:rgba(99,102,241,0.06);border:1px solid rgba(99,102,241,0.15);border-radius:14px;">
            <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.1em;color:#6366F1;font-weight:700;margin-bottom:12px;">Inference Service</div>
            <div style="font-size:0.85rem;color:#8892AA;line-height:1.7;">
                Sends payloads to the <strong style="color:#F0F4FF">FastAPI inference service</strong>.
                The model returns anomaly predictions using IsolationForest.<br><br>
                <strong style="color:#F0F4FF">Endpoint:</strong> <code style="color:#818CF8">/predict</code><br>
                <strong style="color:#F0F4FF">Method:</strong> POST<br>
                <strong style="color:#F0F4FF">Model:</strong> IsolationForest (Staging)
            </div>
        </div>
        """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# TAB 4 — AI ASSISTANT (GROQ CHATBOT)
# ═══════════════════════════════════════════════════════
with tab_chat:
    st.markdown("""
    <div class="section-heading">
        <div class="icon">💬</div>
        InfraMind AI Assistant
    </div>
    <div style="font-size:0.84rem;color:#8892AA;margin-bottom:20px;">
        Ask anything about the pipeline, agents, incidents, architecture, or metrics.
        Powered by <strong style="color:#F0F4FF">Groq · LLaMA 3.3 70B</strong>.
    </div>
    """, unsafe_allow_html=True)

    # Render history
    for msg in st.session_state["chat_messages"]:
        with st.chat_message(msg["role"], avatar="🧠" if msg["role"] == "assistant" else "👤"):
            st.markdown(msg["content"])

    # Suggested prompts (only when empty)
    if not st.session_state["chat_messages"]:
        st.markdown("""
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;">
            <span class="stat-pill blue">💡 Try asking...</span>
        </div>
        """, unsafe_allow_html=True)
        suggestions = [
            "How does the RCA subgraph work?",
            "What triggers an incident?",
            "Explain the Pareto ranking in optimization",
            "How does HITL approval work?",
        ]
        cols = st.columns(2)
        for i, s in enumerate(suggestions):
            if cols[i % 2].button(s, key=f"sug_{i}", use_container_width=True, type="secondary"):
                st.session_state["chat_messages"].append({"role": "user", "content": s})
                with st.spinner("Thinking..."):
                    reply = call_groq(st.session_state["chat_messages"])
                st.session_state["chat_messages"].append({"role": "assistant", "content": reply})
                st.rerun()

    # Chat input
    if prompt := st.chat_input("Ask about the pipeline, agents, incidents…"):
        st.session_state["chat_messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)
        with st.chat_message("assistant", avatar="🧠"):
            with st.spinner(""):
                reply = call_groq(st.session_state["chat_messages"])
            st.markdown(reply)
        st.session_state["chat_messages"].append({"role": "assistant", "content": reply})

    if st.session_state["chat_messages"]:
        if st.button("🗑 Clear Chat", type="secondary"):
            st.session_state["chat_messages"] = []
            st.rerun()

# ─── Auto-refresh when incident active ────────────────────────────────────────
if st.session_state.get("incident_active"):
    time.sleep(0.5)
    st.rerun()
