-- ============================================================
-- Initialisation de la base de données Vélib
-- Créée automatiquement au démarrage du conteneur PostgreSQL
-- ============================================================

-- Création de la base de données dédiée
CREATE DATABASE velib_db;

-- Connexion à la base velib_db
\c velib_db;

-- ============================================================
-- TABLE PRINCIPALE : stations_velib
-- Stocke les snapshots temps réel de chaque station
-- ============================================================
CREATE TABLE IF NOT EXISTS stations_velib (
    id                      SERIAL PRIMARY KEY,
    station_id              VARCHAR(50)     NOT NULL,
    station_name            VARCHAR(255)    NOT NULL,
    coordonnees_geo_lat     DOUBLE PRECISION,
    coordonnees_geo_lon     DOUBLE PRECISION,
    capacity                INTEGER,
    num_bikes_available     INTEGER,
    num_docks_available     INTEGER,
    num_ebikes_available    INTEGER,
    num_mechanical_available INTEGER,
    is_installed            BOOLEAN,
    is_renting              BOOLEAN,
    is_returning            BOOLEAN,
    last_reported           TIMESTAMP,
    collected_at            TIMESTAMP       DEFAULT NOW()
);

-- ============================================================
-- TABLE AGREGATS : stats_horaires
-- Synthèse horaire par station (calculée par Spark)
-- ============================================================
CREATE TABLE IF NOT EXISTS stats_horaires (
    id                      SERIAL PRIMARY KEY,
    station_id              VARCHAR(50)     NOT NULL,
    station_name            VARCHAR(255),
    heure                   TIMESTAMP       NOT NULL,
    avg_bikes_available     DOUBLE PRECISION,
    avg_docks_available     DOUBLE PRECISION,
    avg_ebikes_available    DOUBLE PRECISION,
    min_bikes_available     INTEGER,
    max_bikes_available     INTEGER,
    nb_snapshots            INTEGER,
    created_at              TIMESTAMP       DEFAULT NOW()
);

-- ============================================================
-- INDEX pour performances
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_stations_station_id
    ON stations_velib(station_id);

CREATE INDEX IF NOT EXISTS idx_stations_collected_at
    ON stations_velib(collected_at);

CREATE INDEX IF NOT EXISTS idx_stats_heure
    ON stats_horaires(heure);

CREATE INDEX IF NOT EXISTS idx_stats_station_id
    ON stats_horaires(station_id);

-- ============================================================
-- Droits d'accès
-- ============================================================
GRANT ALL PRIVILEGES ON DATABASE velib_db TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
