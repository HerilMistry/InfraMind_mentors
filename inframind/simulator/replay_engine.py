import pandas as pd
import time
import json
import logging
import requests
import argparse
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    from promptflow.client import PFClient
    pf = PFClient()
    HAS_PF = True
except ImportError:
    HAS_PF = False

class DatasetSimulator:
    def __init__(self, logs_path, metrics_path, endpoint_url="http://localhost:8000/predict"):
        self.logs_path = logs_path
        self.metrics_path = metrics_path
        self.endpoint_url = endpoint_url
        
        logging.info(f"Loading datasets from {logs_path} and {metrics_path}")
        self.logs_df = pd.read_csv(logs_path)
        self.metrics_df = pd.read_csv(metrics_path)
        
    def stream_metrics(self, speed=1.0, limit=None):
        logging.info("Starting metrics stream...")
        count = 0
        for _, row in self.metrics_df.iterrows():
            if limit and count >= limit:
                break
            payload = {
                "cpu_usage_pct": row["cpu_usage_pct"],
                "memory_pressure": row["memory_pressure"],
                "latency_ms": row["latency_ms"],
                "error_rate": row["error_rate"],
                "request_rate": row["request_rate"]
            }
            try:
                logging.info(f"[Metrics Engine] Yielding: {json.dumps(payload)}")
            except Exception as e:
                logging.error(f"Error sending metric: {e}")
            
            time.sleep(1.0 / speed)
            count += 1
            
    def stream_logs(self, speed=1.0, limit=None):
        logging.info("Starting logs stream...")
        count = 0
        
        flow_path = os.path.join(os.path.dirname(__file__), "..", "agents", "pf_analyzer")
        
        for _, row in self.logs_df.iterrows():
            if limit and count >= limit:
                break
            
            # Keep payload lightweight for the logging output
            payload = row.to_dict()
            json_payload = json.dumps(payload)
            
            try:
                logging.info(f"[Logs Engine] Yielding Anomaly ID: {payload.get('Anomaly_ID')} - Type: {payload.get('Anomaly_Type')}")
                
                # Test promptflow integration if available
                if HAS_PF and os.path.exists(flow_path):
                    logging.info(f"Invoking PromptFlow for Anomaly ID: {payload.get('Anomaly_ID')}...")
                    result = pf.test(
                        flow=flow_path,
                        inputs={"anomaly_log": json_payload}
                    )
                    logging.info(f"PromptFlow Result: {result}")
            except Exception as e:
                # We expect auth errors here if Azure isn't configured, but it shows it's working
                logging.error(f"Error processing log with PF: {e}")
                
            time.sleep(1.0 / speed)
            count += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dataset Simulator for InfraMind")
    parser.add_argument("--logs", type=str, default="datasets/logging_monitoring_anomalies.csv")
    parser.add_argument("--metrics", type=str, default="data/metrics_dataset.csv")
    parser.add_argument("--speed", type=float, default=1.0, help="Playback speed multiplier")
    parser.add_argument("--limit", type=int, default=10, help="Number of rows to process")
    parser.add_argument("--mode", type=str, choices=["logs", "metrics", "both"], default="both")
    
    args = parser.parse_args()
    
    simulator = DatasetSimulator(args.logs, args.metrics)
    
    if args.mode in ["metrics", "both"]:
        simulator.stream_metrics(speed=args.speed, limit=args.limit)
        
    if args.mode in ["logs", "both"]:
        simulator.stream_logs(speed=args.speed, limit=args.limit)
