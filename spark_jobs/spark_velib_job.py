"""
spark_velib_job.py
------------------
Job Apache Spark Structured Streaming :
  1. Consomme le topic Kafka 'velib-stations'
  2. Désérialise et valide les messages JSON
  3. Stocke les données brutes dans MongoDB (Data Lake)
  4. Calcule des agrégats horaires par station
  5. Charge les résultats dans PostgreSQL

Usage (depuis le conteneur spark-master) :
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
                 org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
      /opt/spark/jobs/spark_velib_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType,
    DoubleType, TimestampType
)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "velib-stations")

PG_HOST         = os.getenv("PGHOST", "postgres")
PG_PORT         = os.getenv("PGPORT", "5432")
PG_DB           = os.getenv("PGDATABASE", "velib_db")
PG_USER         = os.getenv("PGUSER", "airflow")
PG_PASSWORD     = os.getenv("PGPASSWORD", "airflow")
PG_URL          = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

MONGO_URI       = os.getenv("MONGO_URI", "mongodb://mongodb:27017/velib_lake")

# ------------------------------------------------------------------
# Schéma JSON attendu depuis Kafka
# ------------------------------------------------------------------
VELIB_SCHEMA = StructType([
    StructField("station_id",               StringType(),  True),
    StructField("station_name",             StringType(),  True),
    StructField("coordonnees_geo_lat",      DoubleType(),  True),
    StructField("coordonnees_geo_lon",      DoubleType(),  True),
    StructField("capacity",                 IntegerType(), True),
    StructField("num_bikes_available",      IntegerType(), True),
    StructField("num_docks_available",      IntegerType(), True),
    StructField("num_ebikes_available",     IntegerType(), True),
    StructField("num_mechanical_available", IntegerType(), True),
    StructField("is_installed",             BooleanType(), True),
    StructField("is_renting",               BooleanType(), True),
    StructField("is_returning",             BooleanType(), True),
    StructField("last_reported",            StringType(),  True),
    StructField("collected_at",             StringType(),  True),
])

# ------------------------------------------------------------------
# Initialisation de la session Spark
# ------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VelibStreamingJob")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints")
        .getOrCreate()
    )


# ------------------------------------------------------------------
# Lecture depuis Kafka
# ------------------------------------------------------------------
def read_kafka_stream(spark: SparkSession):
    """Lit le stream Kafka et désérialise les messages JSON."""
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Décodage de la valeur binaire → JSON
    parsed = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), VELIB_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Conversion des types de dates
    parsed = parsed.withColumn(
        "collected_at",
        F.to_timestamp(F.col("collected_at"))
    ).withColumn(
        "last_reported",
        F.to_timestamp(F.col("last_reported"))
    )

    return parsed


# ------------------------------------------------------------------
# Écriture dans MongoDB (Data Lake - données brutes)
# ------------------------------------------------------------------
def write_to_mongodb(batch_df, batch_id):
    """Écrit un micro-batch dans MongoDB comme Data Lake."""
    if batch_df.count() == 0:
        return
    (
        batch_df.write
        .format("mongodb")
        .option("spark.mongodb.write.connection.uri", MONGO_URI)
        .option("collection", "raw_stations")
        .mode("append")
        .save()
    )
    print(f"[Batch {batch_id}] {batch_df.count()} enregistrements écrits dans MongoDB.")


# ------------------------------------------------------------------
# Écriture dans PostgreSQL (données nettoyées)
# ------------------------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    """Écrit un micro-batch dans la table stations_velib de PostgreSQL."""
    if batch_df.count() == 0:
        return

    pg_props = {
        "user":     PG_USER,
        "password": PG_PASSWORD,
        "driver":   "org.postgresql.Driver"
    }

    # Données brutes dans stations_velib
    (
        batch_df
        .filter(F.col("station_id").isNotNull() & (F.col("station_id") != ""))
        .write
        .jdbc(url=PG_URL, table="stations_velib", mode="append", properties=pg_props)
    )

    # Agrégats horaires dans stats_horaires
    stats = (
        batch_df
        .withColumn("heure", F.date_trunc("hour", F.col("collected_at")))
        .groupBy("station_id", "station_name", "heure")
        .agg(
            F.avg("num_bikes_available").alias("avg_bikes_available"),
            F.avg("num_docks_available").alias("avg_docks_available"),
            F.avg("num_ebikes_available").alias("avg_ebikes_available"),
            F.min("num_bikes_available").alias("min_bikes_available"),
            F.max("num_bikes_available").alias("max_bikes_available"),
            F.count("*").alias("nb_snapshots"),
        )
    )

    (
        stats.write
        .jdbc(url=PG_URL, table="stats_horaires", mode="append", properties=pg_props)
    )

    print(f"[Batch {batch_id}] Données écrites dans PostgreSQL.")


# ------------------------------------------------------------------
# Pipeline principal
# ------------------------------------------------------------------
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Job Spark Vélib - Structured Streaming")
    print(f"  Kafka  : {KAFKA_BROKER} / {KAFKA_TOPIC}")
    print(f"  Postgres : {PG_URL}")
    print(f"  MongoDB  : {MONGO_URI}")
    print("=" * 60)

    stream_df = read_kafka_stream(spark)

    # Query 1 : Stockage brut dans MongoDB (Data Lake)
    query_mongo = (
        stream_df.writeStream
        .foreachBatch(write_to_mongodb)
        .option("checkpointLocation", "/tmp/spark_checkpoints/mongo")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Query 2 : Données nettoyées + agrégats dans PostgreSQL
    query_pg = (
        stream_df.writeStream
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "/tmp/spark_checkpoints/postgres")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Streaming démarré. En attente de messages Kafka...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
