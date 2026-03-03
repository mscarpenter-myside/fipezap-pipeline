import urllib.request
import json

repo = "mscarpenter-myside/fipezap-pipeline"
url = f"https://api.github.com/repos/{repo}/actions/runs?per_page=1"

req = urllib.request.Request(url, headers={"User-Agent": "python"})
with urllib.request.urlopen(req) as response:
    data = json.loads(response.read().decode())
    
run_id = data['workflow_runs'][0]['id']
print(f"Latest run ID: {run_id}")

jobs_url = data['workflow_runs'][0]['jobs_url']
req = urllib.request.Request(jobs_url, headers={"User-Agent": "python"})
with urllib.request.urlopen(req) as response:
    jobs_data = json.loads(response.read().decode())

for job in jobs_data['jobs']:
    print(f"Job: {job['name']} - Status: {job['status']} - Conclusion: {job['conclusion']}")
    for step in job['steps']:
        if step['conclusion'] == 'failure':
            print(f"  FAILED STEP: {step['name']}")
