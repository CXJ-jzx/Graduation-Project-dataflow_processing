"""check_job_name.py"""
import requests
FLINK_REST = "http://192.168.56.151:8081"
resp = requests.get(f"{FLINK_REST}/jobs/overview").json()
for j in resp.get('jobs', []):
    print(f"  状态: {j['state']:12s}  名称: '{j['name']}'  ID: {j['jid']}")
