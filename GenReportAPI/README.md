
## Gen Oil Production Report API

This project provides a FastAPI-based service to generate production reports from a PostgreSQL database.

### 1. Build the Docker image

```bash
docker build -t gen-oil-prod-report-api .
```

### 2. Run the container
```bash
docker run -d -p 3339:3339 --name gen_oil_prod_report_api gen-oil-prod-report-api
```

### 3. Check logs
```bash
docker logs -f gen_oil_prod_report_api
```

### API Usage
```python
import requests

API_URL = "http://0.0.0.0:3339/report"   
POSTGRES_DB = ""
POSTGRES_USER = ""
POSTGRES_PASSWORD = ""
HOST = 'host.docker.internal'
PORT = 5432

# Request
payload = {
    "query_date": "2025/05/02",
    "POSTGRES_DB": POSTGRES_DB,
    "POSTGRES_USER": POSTGRES_USER,
    "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    "HOST": HOST,
    "PORT": PORT
}

# --- Get DataFrame as JSON ---
resp_df = requests.post(f"{API_URL}/df", json=payload)
if resp_df.status_code == 200:
    data = resp_df.json()
    print(data[:3])
else:
    print("Error:", resp_df.status_code, resp_df.text)

# --- Get DataFrame as CSV ---
resp_csv = requests.post(f"{API_URL}/csv", json=payload)
if resp_csv.status_code == 200:
    with open("report.csv", "w", encoding="utf-8") as f:
        f.write(resp_csv.text)
    print("CSV file saved as report.csv")
else:
    print("Error:", resp_csv.status_code, resp_csv.text)
```