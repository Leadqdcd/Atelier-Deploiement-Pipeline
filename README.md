# Vélib Big Data Pipeline

Pipeline de traitement de données distribuées en temps réel basé sur les données des stations Vélib de Paris.

---

## Sujet

Ce projet collecte en continu les données de disponibilité des stations Vélib (vélos en libre-service parisiens) depuis l'API Open Data Paris, les ingère via Apache Kafka, les traite avec Apache Spark Structured Streaming, et les stocke dans un Data Lake (MongoDB) et une base analytique (PostgreSQL). L'ensemble est orchestré par Apache Airflow et supervisé via Prometheus/Grafana.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          PIPELINE VÉLIB                             │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  API Vélib   │───▶│    Kafka     │───▶│    Spark Streaming   │  │
│  │ Open Data    │    │  (KRaft)     │    │   (Traitement)       │  │
│  │    Paris     │    │              │    └──────────┬───────────┘  │
│  └──────────────┘    └──────────────┘               │              │
│                                               ┌──────┴──────┐      │
│                                               ▼             ▼      │
│                                         ┌──────────┐ ┌──────────┐ │
│                                         │ MongoDB  │ │PostgreSQL│ │
│                                         │(Data Lake│ │(Analyses)│ │
│                                         │  brut)   │ │          │ │
│                                         └──────────┘ └──────────┘ │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │              Apache Airflow (Orchestration)                 │    │
│  │  check_api ──▶ kafka_producer ──▶ spark_job ──▶ validate  │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  ┌──────────────────────────┐                                      │
│  │  Prometheus + Grafana    │  (Monitoring)                        │
│  └──────────────────────────┘                                      │
└─────────────────────────────────────────────────────────────────────┘
```

Le job Spark est un **stream continu** (Structured Streaming, déclenchement
toutes les 30s) : une fois démarré, il consomme Kafka en continu et écrit
en microbatch dans MongoDB et PostgreSQL. La tâche Airflow `run_spark_job`
est idempotente — elle démarre le stream s'il n'est pas déjà actif, sans le
relancer à chaque exécution du DAG.

### Composants

| Composant | Rôle | Port (local & VPS) |
|---|---|---|
| **API Vélib** | Source de données temps réel | - |
| **Kafka (KRaft)** | Ingestion et gestion des flux | 9092 (interne), 9094 (hôte) |
| **Apache Spark** | Traitement distribué + transformations | 7077 (master), 8083 (UI) |
| **MongoDB** | Data Lake (données brutes) | 27017 |
| **PostgreSQL** | Stockage analytique structuré | 5434 |
| **Apache Airflow** | Orchestration du pipeline | 8082 |
| **Prometheus** | Collecte de métriques | 9090 |
| **Grafana** | Visualisation du monitoring | 3000 |

> Les ports Airflow (8082), Spark UI (8083) et PostgreSQL (5434) ne sont
> **pas** les ports par défaut de ces outils (8080/8081/5432 ou 5433). Ils
> ont été décalés pour permettre au projet de tourner sans conflit à la
> fois en local et sur le VPS mutualisé de l'atelier (voir
> [Déploiement sur le VPS](#déploiement-sur-le-vps)).

---

## Structure du projet

```
velib-pipeline/
├── docker-compose.yml           # Orchestration des conteneurs (local + VPS)
├── docker-compose.override.yml  # Surcharge LOCALE UNIQUEMENT (voir Configuration)
├── .env                         # Variables d'environnement (non versionné)
├── .gitignore
├── requirements.txt
│
├── src/
│   └── velib_producer.py        # Producer Kafka (collecte API Vélib)
│
├── airflow/
│   └── dags/
│       └── velib_pipeline_dag.py   # DAG Airflow (orchestration)
│
├── spark_jobs/
│   └── spark_velib_job.py       # Job Spark Structured Streaming
│
├── sql/
│   └── init.sql                 # Schéma PostgreSQL (création des tables)
│
├── monitoring/
│   └── prometheus.yml           # Configuration Prometheus
│
└── docs/
    ├── rapport-analyse.md       # Rapport d'analyse (captures d'écran)
    └── images/                  # Captures d'écran du rapport
```

---

## Prérequis

- Docker Desktop installé et démarré (avec au moins ~5 Go de RAM alloués)
- Git
- Python 3.10+ (pour exécuter le producer hors Docker, en local)

---

## Installation

### 1. Cloner le dépôt

```bash
git clone <url-du-dépôt>
cd Atelier-Deploiement-Pipeline
```

### 2. Configurer l'environnement Python (optionnel, pour lancer le producer hors Docker)

```bash
python -m venv Env

# Windows
source Env/Scripts/activate
# Linux / macOS
source Env/bin/activate

pip install -r requirements.txt
```

### 3. Configurer les variables d'environnement

Le fichier `.env` est déjà présent avec des valeurs par défaut adaptées au
lancement local (`PGPORT=5434`, `KAFKA_BROKER=localhost:9094`, etc.).
Adaptez-le si besoin (voir [Configuration](#configuration)).

---

## Configuration

### `docker-compose.override.yml` (local uniquement)

Ce fichier est chargé **automatiquement** par `docker compose` s'il est
présent à côté de `docker-compose.yml` — aucune action nécessaire pour
l'utiliser en local. Il monte le socket Docker de la machine hôte dans le
conteneur `airflow-scheduler`, ce qui permet à la tâche Airflow
`run_spark_job` de déclencher `spark-submit` dans le conteneur
`velib-spark-master` via le SDK Python `docker` (déjà inclus dans l'image
`apache/airflow`).

⚠️ **Ce fichier ne doit jamais être déployé sur un serveur mutualisé** :
monter le socket Docker équivaut à donner un accès root au moteur Docker
de la machine. Il reste donc local et n'est pas copié lors du déploiement
VPS (voir plus bas).

### Variables `.env` principales

| Variable | Rôle |
|---|---|
| `PGHOST` / `PGPORT` | Connexion PostgreSQL depuis l'hôte (scripts locaux hors Docker) |
| `KAFKA_BROKER` | Broker Kafka utilisé par `velib_producer.py` lancé hors Docker (`localhost:9094`) |
| `API_URL` | Endpoint de l'API Open Data Paris |
| `VELIB_INTERVAL` | Intervalle (s) entre deux collectes du producer |

---

## Exécution (en local)

### 1. Lancer l'infrastructure Docker

```bash
docker compose up -d
```

> Le démarrage complet prend 1 à 3 minutes (initialisation Airflow + PostgreSQL + téléchargement des images).

### 2. Vérifier que les services sont actifs

```bash
docker compose ps
```

Tous les services doivent être `running`/`healthy`. Vérifier en particulier
Airflow :

```bash
curl http://localhost:8082/health
```

### 3. Activer et déclencher le DAG

Depuis l'interface web (http://localhost:8082, identifiants `admin` / `admin`) :
1. Rechercher le DAG `velib_pipeline`.
2. Activer le DAG (bouton toggle).
3. Déclencher une exécution via le bouton "Trigger DAG".

Ou en ligne de commande :

```bash
docker exec velib-airflow-scheduler airflow dags unpause velib_pipeline
docker exec velib-airflow-scheduler airflow dags trigger velib_pipeline
```

Le DAG exécute dans l'ordre :
1. `check_api_availability` — vérifie que l'API Open Data Paris répond.
2. `run_kafka_producer` — collecte les données pendant 60s et les publie dans Kafka.
3. `run_spark_job` — démarre le job Spark Structured Streaming (idempotent, tourne en continu ensuite).
4. `validate_postgres_data` — vérifie que des données récentes sont bien présentes dans PostgreSQL.

Le stream Spark restant actif en continu après son démarrage, relancer le
DAG périodiquement (ou laisser le scheduler le faire, planification toutes
les 30 minutes) permet d'alimenter Kafka avec de nouvelles données au fil
du temps.

### 4. Lancer le producer manuellement (optionnel, en continu, hors Airflow)

```bash
python src/velib_producer.py
```

### 5. Accéder aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| Spark UI | http://localhost:8083 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |

### 6. Arrêter l'infrastructure

```bash
docker compose down

# Pour supprimer également les volumes (données)
docker compose down -v
```

---

## Vérification des résultats

### PostgreSQL

```bash
docker exec -it velib-postgres psql -U airflow -d velib_db
```

```sql
-- Stations avec le plus de vélos disponibles
SELECT station_name, num_bikes_available, num_ebikes_available
FROM stations_velib
WHERE collected_at >= NOW() - INTERVAL '30 minutes'
ORDER BY num_bikes_available DESC
LIMIT 10;

-- Taux de remplissage moyen par station
SELECT station_name,
       ROUND(AVG(num_bikes_available::decimal / NULLIF(capacity, 0) * 100), 1) AS taux_remplissage_pct
FROM stations_velib
WHERE capacity > 0
GROUP BY station_name
ORDER BY taux_remplissage_pct DESC;

-- Agrégats horaires calculés par Spark
SELECT * FROM stats_horaires ORDER BY heure DESC LIMIT 10;
```

### MongoDB (Data Lake)

```bash
docker exec -it velib-mongodb mongosh velib_lake
```

```javascript
db.raw_stations.countDocuments()
db.raw_stations.find().sort({ collected_at: -1 }).limit(5)
```

### Spark

Interface http://localhost:8083 : le worker doit apparaître dans
**Workers** et le job `VelibStreamingJob` dans **Running Applications**.

---

## Déploiement sur le VPS

Le pipeline est également déployé sur le VPS mutualisé de l'atelier
(`81.17.98.238`), partagé avec d'autres projets (Jenkins, SonarQube,
d'autres ateliers Docker/Hadoop).

### Différences avec l'exécution locale

- **Pas de `docker-compose.override.yml`** : pour des raisons de sécurité
  (serveur mutualisé), le socket Docker n'est pas monté dans le conteneur
  Airflow. La tâche `run_spark_job` du DAG échouera donc côté VPS. Le job
  Spark doit y être démarré manuellement par SSH :

  ```bash
  ssh -i ~/.ssh/velib_vps_ed25519 root@81.17.98.238
  docker exec -d velib-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    /opt/spark/jobs/spark_velib_job.py
  ```

- **Ports décalés** (8082/8083/5434 au lieu des ports par défaut) pour
  éviter les conflits avec les autres projets déjà présents sur le VPS
  (Jenkins sur 8080, un autre Airflow sur 8081, un autre PostgreSQL sur
  5433).
- **Limites mémoire** (`mem_limit`) ajoutées à chaque service dans
  `docker-compose.yml`, le VPS étant partagé avec plusieurs autres stacks
  et disposant de 11 Go de RAM au total.

### Procédure de déploiement

```bash
# Depuis la machine locale, dans le dossier du projet :
tar --exclude='.git' --exclude='__pycache__' --exclude='Env' -czf /tmp/velib-pipeline.tar.gz .
scp -i ~/.ssh/velib_vps_ed25519 /tmp/velib-pipeline.tar.gz root@81.17.98.238:/tmp/

ssh -i ~/.ssh/velib_vps_ed25519 root@81.17.98.238 \
  "mkdir -p /opt/velib-pipeline && tar -xzf /tmp/velib-pipeline.tar.gz -C /opt/velib-pipeline && \
   cd /opt/velib-pipeline && docker compose up -d"
```

Interfaces accessibles à `http://81.17.98.238:<port>` (mêmes ports que la
table ci-dessus).

---

## Limites connues

- **Monitoring non instrumenté** : `monitoring/prometheus.yml` déclare des
  cibles pour Kafka, Spark et Airflow, mais aucun de ces services n'expose
  nativement de métriques au format Prometheus (il faudrait un JMX
  exporter pour Kafka, la config `metrics.properties` pour Spark, et un
  statsd-exporter pour Airflow). Ces cibles apparaîtront donc `DOWN` sur
  http://localhost:9090/targets, et Grafana ne dispose d'aucun dashboard
  provisionné. Amélioration possible pour une itération future.
- **Job Spark non orchestrable par Airflow sur le VPS** : voir
  [Déploiement sur le VPS](#déploiement-sur-le-vps) — démarrage manuel requis.
- **Ressources du VPS mutualisées** : le serveur peut être sous forte
  charge selon l'activité des autres projets qui y tournent, ce qui peut
  ralentir le démarrage des conteneurs (notamment Airflow webserver).

---

## Rapport d'analyse

Voir [`docs/rapport-analyse.md`](docs/rapport-analyse.md) pour les captures
d'écran du pipeline en fonctionnement, des DAGs Airflow et des résultats
obtenus.

---

## Auteur

Atelier Déploiement Pipeline — Léa
