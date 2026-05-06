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

def calculer_score(data):
    score = 0

    # 1. Température idéale entre 20-28°C (40 points)
    temp = data["temperature_c"]
    if 20 <= temp <= 28:
        score += 40
    elif 15 <= temp < 20 or 28 < temp <= 35:
        score += 20
    else:
        score += 5

    # 2. Humidité faible (20 points)
    humidite = data["humidite_pct"]
    if humidite < 40:
        score += 20
    elif humidite < 60:
        score += 10
    else:
        score += 0

    # 3. Peu de nuages (20 points)
    nuages = data["nuages_pct"]
    if nuages < 20:
        score += 20
    elif nuages < 50:
        score += 10
    else:
        score += 0

    # 4. Vent faible (20 points)
    vent = data["vitesse_vent_ms"]
    if vent < 3:
        score += 20
    elif vent < 6:
        score += 10
    else:
        score += 0

    return score

# Lire les données Silver
resultats = []

for ville in VILLES:
    try:
        prefix = f"weather_clean/city={ville}/"
        objects = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
        contents = objects.get("Contents", [])

        if not contents:
            print(f"⚠️ Aucune donnée Silver pour {ville}")
            continue

        # Fichier le plus récent
        latest = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
        raw = s3.get_object(Bucket=SILVER_BUCKET, Key=latest["Key"])
        data = json.loads(raw["Body"].read())

        # Calculer le score
        score = calculer_score(data)

        resultat = {
            "ville": data["ville"],
            "date": date,
            "temperature_c": data["temperature_c"],
            "humidite_pct": data["humidite_pct"],
            "nuages_pct": data["nuages_pct"],
            "vitesse_vent_ms": data["vitesse_vent_ms"],
            "conditions_meteo": data["conditions_meteo"],
            "description_meteo": data["description_meteo"],
            "score_meteo": score,
            "event_timestamp": data["event_timestamp"],
            "ingestion_timestamp": datetime.now().isoformat(),
        }

        resultats.append(resultat)
        print(f"✅ {ville} → Score : {score}/100")

    except Exception as e:
        print(f"❌ Erreur pour {ville} : {e}")

# Classer les villes par score
resultats = sorted(resultats, key=lambda x: x["score_meteo"], reverse=True)

# Ajouter le classement
for i, r in enumerate(resultats):
    r["classement"] = i + 1
    r["meilleure_destination"] = True if i == 0 else False

# Afficher le classement
print("\n🏆 CLASSEMENT DES VILLES :")
print("-" * 40)
for r in resultats:
    emoji = "🥇" if r["classement"] == 1 else "🥈" if r["classement"] == 2 else "🥉" if r["classement"] == 3 else "  "
    print(f"{emoji} #{r['classement']} {r['ville']:15} Score: {r['score_meteo']}/100 | {r['temperature_c']}°C | {r['conditions_meteo']}")

print(f"\n🎯 Meilleure destination : {resultats[0]['ville']} !")

# Stocker dans Gold
try:
    fichier = f"weather_analytics/date={date}/analytics.json"
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=fichier,
        Body=json.dumps(resultats, ensure_ascii=False, indent=2)
    )
    print(f"\n✅ Résultats stockés → gold/{fichier}")
    print("🎉 Gold Layer terminé !")
except Exception as e:
    print(f"❌ Erreur sauvegarde Gold : {e}")

# Af