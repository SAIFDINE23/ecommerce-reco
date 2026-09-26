# Serveur MLflow : l'interface web + l'API qui reçoit les runs d'entraînement.
# Même version que le client installé dans l'image Spark (docker/spark/requirements.txt).
FROM python:3.11-slim-bookworm

RUN pip install --no-cache-dir mlflow==3.16.1

WORKDIR /mlflow
EXPOSE 5000