// Simulated Agent Audit Logs
const logs = [
    "[Monitoring] Metric Observer collecting data...",
    "[Monitoring] Pattern Detector found: ['Memory Spike Signature']",
    "[Monitoring] Trend Analyzer computing momentum...",
    "[Monitoring] Forecast Agent predicts TTF: 15.0m",
    "[Monitoring] Risk Assessment Agent calculated risk: 0.9",
    "[Monitoring] Consensus Reached - Incident Declared: True",
    "[RCA] Observation Agent scoping incident...",
    "[RCA] Evidence Collector querying Loki/Kubernetes...",
    "[RCA] Log Investigator analyzing stack traces...",
    "[RCA] Causal Graph Builder connecting temporal events...",
    "[RCA] Hypothesis Generator proposing causes...",
    "[RCA] Hypothesis Verifier cross-checking evidence...",
    "[RCA] Consensus Reached - Root Cause: Traffic Spike causing OOM (Confidence: 0.95)",
    "[Optimization] Strategy Generator building candidate plans...",
    "[Optimization] Cost Analysis evaluating resource spend...",
    "[Optimization] Safety Analysis checking constraints...",
    "[Optimization] Performance Impact simulating TTF resolution...",
    "[Optimization] Resource Planner verifying cluster capacity...",
    "[Optimization] Ranking Agent balancing Cost vs Safety...",
    "[Optimization] Consensus Reached - Selected Strategy: strat-scale (Safety: 0.95)",
    "[Recovery] Waiting for Human Approval..."
];

const terminal = document.getElementById('terminal-output');

function appendLog(text, index) {
    setTimeout(() => {
        const line = document.createElement('div');
        line.className = 'log-line';
        
        const timestamp = new Date().toISOString().split('T')[1].substring(0, 8);
        const prefix = `<span class="log-prefix">[${timestamp}]</span>`;
        
        // Colorize agent tags
        let coloredText = text;
        if (text.includes("[Monitoring]")) coloredText = text.replace("[Monitoring]", "<span style='color: #60A5FA;'>[Monitoring]</span>");
        if (text.includes("[RCA]")) coloredText = text.replace("[RCA]", "<span style='color: #A78BFA;'>[RCA]</span>");
        if (text.includes("[Optimization]")) coloredText = text.replace("[Optimization]", "<span style='color: #FBBF24;'>[Optimization]</span>");
        if (text.includes("[Recovery]")) coloredText = text.replace("[Recovery]", "<span style='color: #F87171;'>[Recovery]</span>");

        line.innerHTML = prefix + coloredText;
        terminal.appendChild(line);
        terminal.scrollTop = terminal.scrollHeight;
    }, index * 800); // Print a new log every 800ms
}

// Start simulation
document.addEventListener("DOMContentLoaded", () => {
    logs.forEach((log, i) => appendLog(log, i));
});
