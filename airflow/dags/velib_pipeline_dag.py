"""
velib_pipeline_dag.py
---------------------
DAG Airflow orchestrant le pipeline de données Vélib :
  1. Vérification de la disponibilité de l'API
  2. Lancement du producer Kafka (collecte des données)
  3. Soumission du job Spark (traitement + stockage)
  4. Validation des données dans PostgreSQL

Planification : toutes les 30 minutes
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import requests
import psycopg2
import logging

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Arguments par défaut du DAG
# ------------------------------------------------------------------
default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
}

# ------------------------------------------------------------------
# Fonctions Python des tâches
# ------------------------------------------------------------------

def check_api_availability(**context):
    """
    Vérifie que l'API Open Data Paris est accessible
    et retourne le nombre de stations disponibles.
    """
    api_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=1"

    logger.info(f"Vérification de l'API : {api_url}")
    response = requests.get(api_url, timeout=15)
    response.raise_for_status()

    data = response.json()
    total = data.get("total_count", 0)
    logger.info(f"API disponible. Nombre total de stations : {total}")

    # Transmission via XCom pour les tâches suivantes
    context["ti"].xcom_push(key="total_stations", value=total)
    return total


def run_spark_job(**context):
    """
    Démarre le job Spark Structured Streaming dans le conteneur
    velib-spark-master (spark-submit), via le socket Docker de l'hôte.

    Le job est un stream continu (awaitAnyTermination) : on le lance en
    tâche de fond et on ne le redémarre pas s'il tourne déjà, pour éviter
    d'empiler plusieurs consommateurs Kafka concurrents à chaque run du DAG.
    """
    import docker

    client = docker.from_env()
    container = client.containers.get("velib-spark-master")

    already_running = container.exec_run("pgrep -f spark_velib_job.py").exit_code == 0
    if already_running:
        logger.info("Job Spark déjà en cours d'exécution, rien à faire.")
        return "already_running"

    container.exec_run(
        cmd=[
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            # Le HOME du user "spark" de l'image (/home/spark) n'existe pas et
            # n'est pas inscriptible : Ivy ne peut pas y créer son cache par
            # défaut. On le redirige vers /tmp, inscriptible par tous.
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.6.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
            "/opt/spark/jobs/spark_velib_job.py",
        ],
        detach=True,
    )
    logger.info("Job Spark Structured Streaming démarré en arrière-plan.")
    return "started"


def validate_postgres_data(**context):
    """
    Vérifie que des données ont bien été insérées dans PostgreSQL
    lors du dernier cycle de collecte (dans les 10 dernières minutes).
    """
    conn = psycopg2.connect(
        host="velib-postgres",
        port=5432,
        dbname="velib_db",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    # Compte les enregistrements récents
    cursor.execute("""
        SELECT COUNT(*)
        FROM stations_velib
        WHERE collected_at >= NOW() - INTERVAL '10 minutes'
    """)
    count = cursor.fetchone()[0]

    # Statistiques globales
    cursor.execute("""
        SELECT
            COUNT(DISTINCT station_id)                  AS nb_stations,
            AVG(num_bikes_available)                    AS avg_bikes,
            SUM(num_bikes_available)                    AS total_bikes,
            SUM(num_ebikes_available)                   AS total_ebikes
        FROM stations_velib
        WHERE collected_at >= NOW() - INTERVAL '10 minutes'
    """)
    stats = cursor.fetchone()

    cursor.close()
    conn.close()

    logger.info(f"Validation PostgreSQL :")
    logger.info(f"  - Enregistrements récents    : {count}")
    if stats[0]:
        logger.info(f"  - Stations distinctes        : {stats[0]}")
        logger.info(f"  - Vélos disponibles (moy)    : {round(stats[1], 2) if stats[1] else 0}")
        logger.info(f"  - Total vélos disponibles    : {stats[2]}")
        logger.info(f"  - Total vélos électriques    : {stats[3]}")

    if count == 0:
        raise ValueError("Aucune donnée insérée dans les 10 dernières minutes !")

    return count


# ------------------------------------------------------------------
# Définition du DAG
# ------------------------------------------------------------------
with DAG(
    dag_id="velib_pipeline",
    default_args=default_args,
    description="Pipeline complet de collecte et traitement des données Vélib",
    schedule_interval="*/30 * * * *",  # Toutes les 30 minutes
    catchup=False,
    tags=["velib", "kafka", "spark", "streaming"],
) as dag:

    # Tâche 1 : Vérification de l'API
    check_api = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_api_availability,
    )

    # Tâche 2 : Lancement du producer Kafka (60 secondes de collecte)
    run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="""
            cd /opt/airflow/src && \
            pip install kafka-python requests python-dotenv --quiet && \
            timeout 60 python velib_producer.py || true
        """,
        env={
            "API_URL":       "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=100",
            "KAFKA_BROKER":  "velib-kafka:9092",
            "KAFKA_TOPIC":   "velib-stations",
            "VELIB_INTERVAL": "30",
        },
    )

    # Tâche 3 : Soumission du job Spark (démarrage du stream, idempotent)
    run_spark = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    # Tâche 4 : Validation des données dans PostgreSQL
    validate_data = PythonOperator(
        task_id="validate_postgres_data",
        python_callable=validate_postgres_data,
    )

    # Ordre d'exécution
    check_api >> run_producer >> run_spark >> validate_data
