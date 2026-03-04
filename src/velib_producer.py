"""
velib_producer.py
-----------------
Collecte les données en temps réel depuis l'API Open Data Paris (Vélib)
et les publie dans un topic Apache Kafka toutes les N secondes.

Usage :
    python src/velib_producer.py
"""

import os
import json
import time
import logging
import requests

from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
load_dotenv()

API_URL       = os.getenv("API_URL", "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=100")
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "velib-stations")
INTERVAL      = int(os.getenv("VELIB_INTERVAL", 30))

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Initialisation du producer Kafka
# ------------------------------------------------------------------
def create_producer() -> KafkaProducer:
    """Crée et retourne un KafkaProducer avec sérialisation JSON."""
    logger.info(f"Connexion au broker Kafka : {KAFKA_BROKER}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=5,
        retry_backoff_ms=1000,
    )
    logger.info("Producer Kafka initialisé avec succès.")
    return producer


# ------------------------------------------------------------------
# Collecte depuis l'API Vélib
# ------------------------------------------------------------------
def fetch_velib_data() -> list[dict]:
    """
    Appelle l'API Open Data Paris et retourne la liste des stations.
    Retourne une liste vide en cas d'erreur.
    """
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
        records = data.get("results", [])
        logger.info(f"API : {len(records)} stations récupérées.")
        return records
    except requests.exceptions.Timeout:
        logger.error("Timeout lors de l'appel à l'API Vélib.")
        return []
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erreur HTTP : {e}")
        return []
    except Exception as e:
        logger.error(f"Erreur inattendue lors de la collecte : {e}")
        return []


# ------------------------------------------------------------------
# Transformation et enrichissement
# ------------------------------------------------------------------
def transform_record(record: dict) -> dict:
    """
    Nettoie et aplatit un enregistrement brut de l'API Vélib.
    Ajoute un timestamp de collecte.
    """
    geo = record.get("coordonnees_geo") or {}
    return {
        "station_id":               record.get("stationcode", ""),
        "station_name":             record.get("name", ""),
        "coordonnees_geo_lat":      geo.get("lat"),
        "coordonnees_geo_lon":      geo.get("lon"),
        "capacity":                 record.get("capacity", 0),
        "num_bikes_available":      record.get("numbikesavailable", 0),
        "num_docks_available":      record.get("numdocksavailable", 0),
        "num_ebikes_available":     record.get("ebike", 0),
        "num_mechanical_available": record.get("mechanical", 0),
        "is_installed":             record.get("is_installed") == "OUI",
        "is_renting":               record.get("is_renting") == "OUI",
        "is_returning":             record.get("is_returning") == "OUI",
        "last_reported":            record.get("duedate", ""),
        "collected_at":             datetime.utcnow().isoformat(),
    }


# ------------------------------------------------------------------
# Publication dans Kafka
# ------------------------------------------------------------------
def publish_to_kafka(producer: KafkaProducer, records: list[dict]) -> int:
    """
    Envoie chaque station comme message indépendant dans le topic Kafka.
    Retourne le nombre de messages envoyés avec succès.
    """
    sent = 0
    for record in records:
        try:
            message = transform_record(record)
            producer.send(KAFKA_TOPIC, value=message)
            sent += 1
        except Exception as e:
            logger.warning(f"Erreur d'envoi pour la station {record.get('stationcode')} : {e}")

    producer.flush()
    return sent


# ------------------------------------------------------------------
# Boucle principale
# ------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("  Démarrage du producer Vélib → Kafka")
    logger.info(f"  Topic    : {KAFKA_TOPIC}")
    logger.info(f"  Intervalle : {INTERVAL}s")
    logger.info("=" * 60)

    producer = create_producer()

    iteration = 1
    while True:
        logger.info(f"--- Itération #{iteration} ---")

        records = fetch_velib_data()

        if records:
            sent = publish_to_kafka(producer, records)
            logger.info(f"{sent}/{len(records)} messages envoyés dans '{KAFKA_TOPIC}'.")
        else:
            logger.warning("Aucune donnée à publier pour cette itération.")

        logger.info(f"Prochain appel dans {INTERVAL} secondes...\n")
        time.sleep(INTERVAL)
        iteration += 1


if __name__ == "__main__":
    main()
