"""Vérifie que le conteneur spark-client peut écrire dans le serveur MLflow.

Crée l'expérience « test-connexion » avec un run qui contient :
  - 2 paramètres, 1 métrique, 1 petit fichier (artifact).

Exemple :
  docker compose exec spark-client python spark_jobs/mlflow_smoke_test.py
"""
import tempfile
from pathlib import Path

import mlflow


def main():
    # L'adresse du serveur vient de la variable MLFLOW_TRACKING_URI (docker-compose.yml).
    print("Serveur MLflow :", mlflow.get_tracking_uri())

    mlflow.set_experiment("test-connexion")
    with mlflow.start_run(run_name="hello-mlflow") as run:
        mlflow.log_params({"rank": 32, "alpha": 20})     # les réglages
        mlflow.log_metric("recall_at_10", 0.24155)      # le résultat

        with tempfile.TemporaryDirectory() as tmp:       # un fichier quelconque
            note = Path(tmp) / "note.txt"
            note.write_text("Bonjour MLflow, ceci est un artifact.\n")
            mlflow.log_artifact(str(note))

        print("Run créé   :", run.info.run_id)
        print("Artifacts  :", run.info.artifact_uri)
    print("OK : ouvrez http://localhost:5000 → expérience « test-connexion »")


if __name__ == "__main__":
    main()