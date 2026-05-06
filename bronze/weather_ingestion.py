import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import boto3
import json
from datetime import datetime
from config import *

# Connexion MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

date = datetime.now().strftime("%Y-%m-%d")
heure = datetime.now().strftime("%Hh")

for ville in VILLES:
    url = f"{BASE_URL}?q={ville}&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()

    fichier = f"weather_raw/city={ville}/date={date}/data_{heure}.json"
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=fichier,
        Body=json.dumps(data)
    )
    print(f"✅ {ville} → bronze/{fichier}")

print("🎉 Ingestion Bronze terminée !")