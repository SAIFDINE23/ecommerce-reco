# Système de recommandation e-commerce hybride (batch + temps réel)

Moteur de recommandation qui apprend les goûts durables de chaque utilisateur
chaque nuit (Spark, ALS), capte son intention du moment en temps réel
(Kafka, Spark Structured Streaming) et combine les deux dans une API (FastAPI).
Réentraînement orchestré par Airflow, cycle de vie des modèles géré par MLflow.

**Données :** [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
(REES46, ~110 M d'événements, octobre-novembre 2019).

## Statut

🚧 Semaine 1 : fondations (environnement, ingestion, exploration).
