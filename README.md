# Weather Pipeline — Moroccan Cities

A real-time weather ETL pipeline built with a **medallion architecture** (Bronze → Silver → Gold), fetching data from the OpenWeatherMap API for 7 Moroccan cities and orchestrated by Apache Airflow.

---

## Architecture Overview

```
OpenWeatherMap API
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG                    │
│                  (Daily at 6 AM)                        │
│                                                          │
│  [Bronze] ──► [Silver] ──► [Gold] ──► [Quality]         │
└─────────────────────────────────────────────────────────┘
        │           │           │
        ▼           ▼           ▼
     MinIO       MinIO        MinIO
    bronze/     silver/       gold/
```

### Layers

| Layer | Script | Role |
|-------|--------|------|
| **Bronze** | `bronze/weather_ingestion.py` | Fetches raw JSON from API and stores it as-is |
| **Silver** | `silver/silver_transformation.py` | Cleans data, converts Kelvin → Celsius, extracts 15 standardized fields |
| **Gold** | `gold/gold_analytics.py` | Computes a weather score (0–100), ranks cities, identifies best destination |
| **Quality** | `quality/data_quality.py` | Validates all 3 layers with 15+ test cases |

---

## Monitored Cities

Marrakech · Tanger · Dakhla · Essaouira · Casablanca · Fès · Agadir

---

## Data Storage Structure (MinIO)

```
bronze/weather_raw/city={city}/date={YYYY-MM-DD}/data_{HH}h.json
silver/weather_clean/city={city}/date={YYYY-MM-DD}/clean.json
gold/weather_analytics/date={YYYY-MM-DD}/analytics.json
```

---

## Weather Scoring (Gold Layer)

The score (0–100) ranks cities based on ideal travel conditions:

| Criterion | Condition | Points |
|-----------|-----------|--------|
| Temperature | 20–28 °C | 40 |
| Humidity | < 40% | 20 |
| Cloud cover | < 20% | 20 |
| Wind speed | < 3 m/s | 20 |

---

## Prerequisites

- Docker & Docker Compose
- Python 3.8+
- An [OpenWeatherMap API key](https://openweathermap.org/api)

---

## Installation & Setup

### 1. Clone the repository

```bash
git clone https://github.com/ayman-dahraoui/weather-pipeline.git
cd weather-pipeline
```

### 2. Configure credentials

Edit `config.py` and update the following values (or move them to a `.env` file):

```python
API_KEY        = "your_openweathermap_api_key"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
```

### 3. Install Python dependencies (standalone mode)

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

### 4. Start Airflow + MinIO (Docker)

```bash
cd airflow
docker-compose up -d
```

Once running:
- **Airflow UI**: http://localhost:8080 (default: `airflow` / `airflow`)
- **MinIO Console**: http://localhost:9001 (default: `minioadmin` / `minioadmin`)

Create the three MinIO buckets before the first run: `bronze`, `silver`, `gold`.

---

## Running the Pipeline

### Via Airflow (recommended)

The DAG `weather_pipeline` runs automatically every day at **6:00 AM**. It can also be triggered manually from the Airflow UI.

### Standalone (without Airflow)

Run each step in order:

```bash
python bronze/weather_ingestion.py
python silver/silver_transformation.py
python gold/gold_analytics.py
python quality/data_quality.py
```

---

## Project Structure

```
weather-pipeline/
├── config.py                        # Centralized configuration
├── requirements.txt                 # Python dependencies
├── bronze/
│   └── weather_ingestion.py         # API ingestion → MinIO bronze
├── silver/
│   └── silver_transformation.py     # Cleaning & standardization
├── gold/
│   └── gold_analytics.py            # Scoring & city ranking
├── quality/
│   └── data_quality.py              # Data quality tests
└── airflow/
    ├── docker-compose.yaml          # Airflow + PostgreSQL + Redis
    ├── .env                         # Airflow environment variables
    └── dags/
        └── weather_dag.py           # DAG definition
```

---

## Dependencies

| Package | Version | Usage |
|---------|---------|-------|
| `requests` | 2.32.5 | OpenWeatherMap API calls |
| `boto3` | 1.42.91 | MinIO / S3 storage |
| `pandas` | 2.2.3 | Data manipulation |
| Apache Airflow | 3.2.1 | Orchestration |
| PostgreSQL | 16 | Airflow metadata DB |
| Redis | 7.2 | Celery message broker |
| MinIO | latest | Object storage |

---

## DAG Configuration

- **Schedule**: `0 6 * * *` (daily at 6 AM)
- **Retries**: 1 retry with a 5-minute delay
- **Executor**: CeleryExecutor

```
bronze_ingestion
      │
      ▼
silver_transformation
      │
      ▼
gold_analytics
      │
      ▼
data_quality_check
```

---

## Security Notes

> The current `config.py` contains hardcoded credentials for development purposes.  
> Before deploying to production:
> - Move all secrets (API key, MinIO credentials, Fernet key) to a `.env` file
> - Add `.env` to `.gitignore`
> - Enable Airflow authentication and HTTPS

---

## Author

**Ayman Dahraoui** — [GitHub](https://github.com/ayman-dahraoui)
