import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import json
from datetime import datetime
from config import *

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

ingestion_timestamp = datetime.now().isoformat()
date = datetime.now().strftime("%Y-%m-%d")

for ville in VILLES:
    try:
        prefix = f"weather_raw/city={ville}/"
        objects = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
        contents = objects.get("Contents", [])

        if not contents:
            print(f"⚠️ Aucun fichier trouvé pour {ville}")
            continue

        # Prendre seulement le fichier le plus récent
        latest = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
        
        raw = s3.get_object(Bucket=BRONZE_BUCKET, Key=latest["Key"])
        data = json.loads(raw["Body"].read())

        # Vérifier que les données sont valides
        if "main" not in data:
            print(f"⚠️ Données invalides pour {ville} : {data}")
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
        s3.put_object(
            Bucket=SILVER_BUCKET,
            Key=fichier,
            Body=json.dumps(clean, ensure_ascii=False, indent=2)
        )
        print(f"✅ {ville} → silver/{fichier}")

    except Exception as e:
        print(f"❌ Erreur pour {ville} : {e}")

print("🎉 Transformation Silver terminée !")