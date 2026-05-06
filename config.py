# Configuration commune du projet Weather Pipeline

# MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Buckets
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"

# API OpenWeatherMap
API_KEY = "65bc412f66c962c8408186dec92737e4"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Villes
VILLES = [
    "Marrakech",
    "Tanger",
    "Dakhla",
    "Essaouira",
    "Casablanca",
    "Fes",
    "Agadir"
]