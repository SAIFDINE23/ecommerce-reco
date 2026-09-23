# Système de recommandation e-commerce hybride (batch + temps réel)

Moteur de recommandation qui apprend les goûts durables de chaque utilisateur
chaque nuit (Spark, ALS), capte son intention du moment en temps réel
(Kafka, Spark Structured Streaming) et combine les deux dans une API (FastAPI).
Réentraînement orchestré par Airflow, cycle de vie des modèles géré par MLflow.

**Données :** [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
(REES46, ~110 M d'événements, octobre-novembre 2019).

## Statut

🚧 Semaine 1 : fondations (environnement, ingestion, exploration).

## Démarrage rapide

Prérequis : Docker Desktop (WSL2 sous Windows), une clé API Kaggle dans `~/.kaggle/kaggle.json`.

```bash
# 1. Télécharger les données (~4,3 Go, une seule fois)
bash scripts/download_data.sh

# 2. Adapter les ressources Spark à sa machine
cp .env.example .env

# 3. Construire l'image et démarrer le cluster Spark
docker compose build
docker compose up -d

# 4. Vérifier que tout fonctionne (compte les 42 M d'événements d'octobre)
docker compose exec spark-client spark-submit spark_jobs/smoke_test.py data/raw/2019-Oct.csv
```

| Interface | Adresse |
|---|---|
| Spark master | http://localhost:8090 |
| Spark worker | http://localhost:8081 |
| Job Spark en cours | http://localhost:4040 |
| JupyterLab | http://localhost:8888 |