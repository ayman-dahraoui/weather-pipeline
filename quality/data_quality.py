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

date = datetime.now().strftime("%Y-%m-%d")
erreurs = []
tests_passes = 0
tests_total = 0

def test(nom, condition, message_erreur):
    global tests_passes, tests_total
    tests_total += 1
    if condition:
        tests_passes += 1
        print(f"  ✅ {nom}")
    else:
        erreurs.append(message_erreur)
        print(f"  ❌ {nom} → {message_erreur}")

# ─────────────────────────────────────────
print("\n" + "="*50)
print("🔍 TEST 1 : BRONZE LAYER")
print("="*50)

for ville in VILLES:
    prefix = f"weather_raw/city={ville}/"
    objects = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    contents = objects.get("Contents", [])
    
    test(
        f"{ville} — fichier existe",
        len(contents) > 0,
        f"Aucun fichier Bronze pour {ville}"
    )
    
    if contents:
        latest = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
        test(
            f"{ville} — fichier non vide",
            latest["Size"] > 0,
            f"Fichier Bronze vide pour {ville}"
        )

# ─────────────────────────────────────────
print("\n" + "="*50)
print("🔍 TEST 2 : SILVER LAYER")
print("="*50)

for ville in VILLES:
    prefix = f"weather_clean/city={ville}/"
    objects = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    contents = objects.get("Contents", [])

    test(
        f"{ville} — fichier Silver existe",
        len(contents) > 0,
        f"Aucun fichier Silver pour {ville}"
    )

    if contents:
        latest = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
        raw = s3.get_object(Bucket=SILVER_BUCKET, Key=latest["Key"])
        data = json.loads(raw["Body"].read())

        # Test types et valeurs
        test(
            f"{ville} — température valide",
            isinstance(data.get("temperature_c"), float) and -10 <= data["temperature_c"] <= 60,
            f"Température invalide : {data.get('temperature_c')}"
        )
        test(
            f"{ville} — humidité valide",
            isinstance(data.get("humidite_pct"), int) and 0 <= data["humidite_pct"] <= 100,
            f"Humidité invalide : {data.get('humidite_pct')}"
        )
        test(
            f"{ville} — vent valide",
            isinstance(data.get("vitesse_vent_ms"), float) and data["vitesse_vent_ms"] >= 0,
            f"Vent invalide : {data.get('vitesse_vent_ms')}"
        )
        test(
            f"{ville} — event_timestamp existe",
            "event_timestamp" in data and data["event_timestamp"] is not None,
            f"event_timestamp manquant"
        )
        test(
            f"{ville} — ingestion_timestamp existe",
            "ingestion_timestamp" in data and data["ingestion_timestamp"] is not None,
            f"ingestion_timestamp manquant"
        )

# ─────────────────────────────────────────
print("\n" + "="*50)
print("🔍 TEST 3 : GOLD LAYER")
print("="*50)

prefix = f"weather_analytics/date={date}/"
objects = s3.list_objects_v2(Bucket=GOLD_BUCKET, Prefix=prefix)
contents = objects.get("Contents", [])

test(
    "Fichier Gold existe",
    len(contents) > 0,
    "Aucun fichier Gold trouvé"
)

if contents:
    raw = s3.get_object(Bucket=GOLD_BUCKET, Key=contents[0]["Key"])
    data = json.loads(raw["Body"].read())

    test(
        "Gold contient 7 villes",
        len(data) == len(VILLES),
        f"Nombre de villes incorrect : {len(data)}"
    )
    test(
        "Scores entre 0 et 100",
        all(0 <= r["score_meteo"] <= 100 for r in data),
        "Score hors limites détecté"
    )
    test(
        "Classement complet",
        all("classement" in r for r in data),
        "Classement manquant dans certaines villes"
    )
    test(
        "Une seule meilleure destination",
        sum(1 for r in data if r["meilleure_destination"]) == 1,
        "Plusieurs ou aucune meilleure destination"
    )
    
    meilleure = next(r for r in data if r["meilleure_destination"])
    test(
        f"Meilleure ville identifiée : {meilleure['ville']}",
        meilleure["classement"] == 1,
        "La meilleure ville n'est pas classée #1"
    )

# ─────────────────────────────────────────
print("\n" + "="*50)
print("📊 RAPPORT FINAL")
print("="*50)
print(f"Tests passés : {tests_passes}/{tests_total}")

if len(erreurs) == 0:
    print("🎉 QUALITÉ DES DONNÉES : PASSED ✅")
else:
    print(f"⚠️  QUALITÉ DES DONNÉES : {len(erreurs)} erreur(s) détectée(s)")
    for e in erreurs:
        print(f"   → {e}")