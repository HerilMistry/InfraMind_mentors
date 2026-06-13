from promptflow import tool
import json

@tool
def format_prompt(log_data: str) -> str:
    """
    Formats the raw log anomaly JSON into a clean summary for the LLM.
    """
    try:
        data = json.loads(log_data)
        
        summary = f"""
Anomaly ID: {data.get('Anomaly_ID', 'N/A')}
Type: {data.get('Anomaly_Type', 'N/A')}
Severity: {data.get('Severity', 'N/A')}
Affected Service: {data.get('Affected_Services', 'N/A')}
Resource Usage:
  CPU: {data.get('CPU_Usage_Percent', 'N/A')}%
  Memory: {data.get('Memory_Usage_MB', 'N/A')} MB
  Disk: {data.get('Disk_Usage_Percent', 'N/A')}%
Error Code: {data.get('Error_Code', 'N/A')}
Duration: {data.get('Anomaly_Duration_sec', 'N/A')}s
"""
        return summary
    except Exception as e:
        return f"Error parsing log data: {e}\nRaw Data: {log_data}"
