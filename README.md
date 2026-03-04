# Vélib Big Data Pipeline

Pipeline de traitement de données distribuées en temps réel basé sur les données des stations Vélib de Paris.

---

## Sujet

Ce projet collecte en continu les données de disponibilité des stations Vélib (vélos en libre-service parisiens) depuis l'API Open Data Paris, les ingère via Apache Kafka, les traite avec Apache Spark Structured Streaming, et les stocke dans un Data Lake (MongoDB) et une base analytique (PostgreSQL).

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

### Composants

| Composant | Rôle | Port |
|---|---|---|
| **API Vélib** | Source de données temps réel | - |
| **Kafka (KRaft)** | Ingestion et gestion des flux | 9092 |
| **Apache Spark** | Traitement distribué + transformations | 7077 / 8081 |
| **MongoDB** | Data Lake (données brutes) | 27017 |
| **PostgreSQL** | Stockage analytique structuré | 5433 |
| **Apache Airflow** | Orchestration du pipeline | 8080 |
| **Prometheus** | Collecte de métriques | 9090 |
| **Grafana** | Visualisation du monitoring | 3000 |

---

## Structure du projet

```
velib-pipeline/
├── docker-compose.yml          # Orchestration des conteneurs
├── .env                        # Variables d'environnement (non versionné)
├── .gitignore
├── requirements.txt
│
├── src/
│   └── velib_producer.py       # Producer Kafka (collecte API Vélib)
│
├── airflow/
│   └── dags/
│       └── velib_pipeline_dag.py  # DAG Airflow (orchestration)
│
├── spark_jobs/
│   └── spark_velib_job.py      # Job Spark Structured Streaming
│
├── sql/
│   └── init.sql                # Schéma PostgreSQL (création des tables)
│
└── monitoring/
    └── prometheus.yml          # Configuration Prometheus
```

---

## Instructions de lancement

### Prérequis

- Docker Desktop installé et démarré
- Git
- Python 3.10+ (pour l'environnement virtuel local)

### 1. Cloner le dépôt

```bash
git clone https://github.com/<votre-username>/velib-pipeline.git
cd velib-pipeline
```

### 2. Configurer l'environnement

```bash
# Créer l'environnement virtuel
python -m venv Env

# Activer l'environnement (Windows)
source Env/Scripts/activate

# Activer l'environnement (Linux / macOS)
source Env/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

Copier le fichier `.env.example` et le renommer en `.env` :

```bash
cp .env.example .env
```

### 3. Lancer l'infrastructure Docker

```bash
docker-compose up -d
```

> Le démarrage complet prend environ 2-3 minutes (initialisation Airflow + PostgreSQL).

### 4. Vérifier que les services sont actifs

```bash
docker-compose ps
```

Tous les services doivent être en état `running` ou `healthy`.

### 5. Accéder aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:8081 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |

### 6. Activer le DAG Airflow

1. Ouvrir http://localhost:8080
2. Rechercher le DAG `velib_pipeline`
3. Activer le DAG (bouton toggle)
4. Déclencher manuellement une première exécution via le bouton "Trigger DAG"

### 7. Lancer le producer manuellement (optionnel)

```bash
python src/velib_producer.py
```

### 8. Arrêter l'infrastructure

```bash
docker-compose down

# Pour supprimer également les volumes (données)
docker-compose down -v
```

---

## Données collectées

L'API Vélib retourne pour chaque station :

| Champ | Description |
|---|---|
| `station_id` | Identifiant unique de la station |
| `station_name` | Nom de la station |
| `capacity` | Capacité totale de la station |
| `num_bikes_available` | Vélos disponibles (total) |
| `num_docks_available` | Places libres |
| `num_ebikes_available` | Vélos électriques disponibles |
| `num_mechanical_available` | Vélos mécaniques disponibles |
| `is_installed` | Station installée ? |
| `is_renting` | Location active ? |
| `is_returning` | Retour actif ? |
| `collected_at` | Horodatage de collecte |

---

## Requêtes SQL utiles

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

-- Evolution horaire globale
SELECT DATE_TRUNC('hour', collected_at) AS heure,
       ROUND(AVG(num_bikes_available), 0) AS avg_bikes
FROM stations_velib
GROUP BY heure
ORDER BY heure DESC;
```

---

## Auteur

Projet réalisé dans le cadre d'un atelier Big Data — Master Data Engineering.
