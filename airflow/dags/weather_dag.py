from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import boto3
import json

# Configuration
API_KEY = "65bc412f66c962c8408186dec92737e4"
MINIO_ENDPOINT = "http://host.docker.internal:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
VILLES = ["Marrakech", "Tanger", "Dakhla", "Essaouira", "Casablanca", "Fes", "Agadir"]

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

# ─── BRONZE ───────────────────────────────
def ingestion_bronze():
    s3 = get_s3()
    date = datetime.now().strftime("%Y-%m-%d")
    heure = datetime.now().strftime("%Hh")
    for ville in VILLES:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={ville}&appid={API_KEY}"
        data = requests.get(url).json()
        fichier = f"weather_raw/city={ville}/date={date}/data_{heure}.json"
        s3.put_object(Bucket="bronze", Key=fichier, Body=json.dumps(data))
        print(f"✅ Bronze: {ville}")

# ─── SILVER ───────────────────────────────
def transformation_silver():
    s3 = get_s3()
    date = datetime.now().strftime("%Y-%m-%d")
    ingestion_timestamp = datetime.now().isoformat()
    for ville in VILLES:
        try:
            prefix = f"weather_raw/city={ville}/"
            objects = s3.list_objects_v2(Bucket="bronze", Prefix=prefix)
            contents = objects.get("Contents", [])
            if not contents:
                continue
            latest = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
            raw = s3.get_object(Bucket="bronze", Key=latest["Key"])
            data = json.loads(raw["Body"].read())
            if "main" not in data:
                continue
            clean = {
                "ville": data.get("name", ville),
                "pays": data.get("sys", {}).get("country", "MA"),
                "latitude": float(data.get("coord", {}).get("lat", 0)),
                "longitude": float(data.get("coord", {}).get("lon", 0)),
                "temperature_c": round(float(data["main"]["temp"]) - 273.15, 2),
                "temp_min_c": round(float(data["main"]["temp_min"]) - 273.15, 2),
                "temp_max_c": round(float(data["main"]["temp_max"]) - 273.15, 2),
                "temperature_ressentie_c": round(float(data["main"]["feels_like"]) - 273.15, 2),
                "humidite_pct": int(data["main"].get("humidity", 0)),
                "pression_hpa": int(data["main"].get("pressure", 0)),
                "conditions_meteo": data["weather"][0].get("main", "Unknown"),
                "description_meteo": data["weather"][0].get("description", ""),
                "vitesse_vent_ms": float(data.get("wind", {}).get("speed", 0)),
                "direction_vent_deg": float(data.get("wind", {}).get("deg", 0)),
                "visibilite_m": int(data.get("visibility", 0)),
                "nuages_pct": int(data.get("clouds", {}).get("all", 0)),
                "event_timestamp": datetime.utcfromtimestamp(data["dt"]).isoformat(),
                "ingestion_timestamp": ingestion_timestamp,
            }
            fichier = f"weather_clean/city={ville}/date={date}/clean.json"
            s3.put_object(Bucket="silver", Key=fichier, Body=json.dumps(clean, ensure_ascii=False, indent=2))
            print(f"✅ Silver: {ville}")
        except Exception as e:
            print(f"❌ Silver erreur {ville}: {e}")

# ─── GOLD ─────────────────────────────────
def analytics_gold():
    s3 = get_s3()
    date = datetime.now().strftime("%Y-%m-%d")
    resultats = []
    for ville in VILLES:
        try:
            prefix = f"weather_clean/city={ville}/"
            objects = s3.list_objects_v2(Bucket="silver", Prefix=prefix)
            contents = objects.get("Contents", [])
            if not contents:
                continue
            latest = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
            raw = s3.get_object(Bucket="silver", Key=latest["Key"])
            data = json.loads(raw["Body"].read())
            score = 0
            temp = data["temperature_c"]
            if 20 <= temp <= 28: score += 40
            elif 15 <= temp < 20 or 28 < temp <= 35: score += 20
            else: score += 5
            if data["humidite_pct"] < 40: score += 20
            elif data["humidite_pct"] < 60: score += 10
            if data["nuages_pct"] < 20: score += 20
            elif data["nuages_pct"] < 50: score += 10
            if data["vitesse_vent_ms"] < 3: score += 20
            elif data["vitesse_vent_ms"] < 6: score += 10
            resultats.append({**data, "score_meteo": score, "date": date})
            print(f"✅ Gold: {ville} → {score}/100")
        except Exception as e:
            print(f"❌ Gold erreur {ville}: {e}")
    resultats = sorted(resultats, key=lambda x: x["score_meteo"], reverse=True)
    for i, r in enumerate(resultats):
        r["classement"] = i + 1
        r["meilleure_destination"] = i == 0
    fichier = f"weather_analytics/date={date}/analytics.json"
    s3.put_object(Bucket="gold", Key=fichier, Body=json.dumps(resultats, ensure_ascii=False, indent=2))
    print(f"🥇 Meilleure destination : {resultats[0]['ville']}")

# ─── QUALITÉ ──────────────────────────────
def qualite_donnees():
    s3 = get_s3()
    date = datetime.now().strftime("%Y-%m-%d")
    erreurs = []
    for ville in VILLES:
        objects = s3.list_objects_v2(Bucket="bronze", Prefix=f"weather_raw/city={ville}/")
        if not objects.get("Contents"):
            erreurs.append(f"Bronze manquant: {ville}")
        objects = s3.list_objects_v2(Bucket="silver", Prefix=f"weather_clean/city={ville}/")
        if not objects.get("Contents"):
            erreurs.append(f"Silver manquant: {ville}")
    objects = s3.list_objects_v2(Bucket="gold", Prefix=f"weather_analytics/date={date}/")
    if not objects.get("Contents"):
        erreurs.append("Gold manquant")
    if erreurs:
        raise Exception(f"Qualité échouée: {erreurs}")
    print("✅ Qualité des données : PASSED !")

# ─── DAG ──────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_pipeline",
    default_args=default_args,
    description="Pipeline météo Maroc — Bronze → Silver → Gold → Qualité",
    schedule="0 6 * * *",
    catchup=False,
    tags=["weather", "morocco", "etl"],
)

t1 = PythonOperator(task_id="ingestion_bronze", python_callable=ingestion_bronze, dag=dag)
t2 = PythonOperator(task_id="transformation_silver", python_callable=transformation_silver, dag=dag)
t3 = PythonOperator(task_id="analytics_gold", python_callable=analytics_gold, dag=dag)
t4 = PythonOperator(task_id="qualite_donnees", python_callable=qualite_donnees, dag=dag)

t1 >> t2 >> t3 >> t4