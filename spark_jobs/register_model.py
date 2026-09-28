"""Enregistre le modèle d'un run MLflow dans le Model Registry et lui donne un alias.

Pourquoi un script séparé de l'entraînement ? Entraîner et PROMOUVOIR sont deux décisions
différentes : on entraîne souvent, on ne met un modèle en production qu'après vérification.
(En semaine 5, c'est Airflow qui appellera ce script après avoir comparé champion et challenger.)

Exemple (dans le conteneur spark-client, pas besoin de Spark) :
  python spark_jobs/register_model.py --run-id <RUN_ID> --alias champion
Le modèle se charge ensuite partout avec l'adresse :  models:/als-recommender@champion
"""
import argparse

import mlflow
from mlflow import MlflowClient

MODEL_NAME = "als-recommender"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True, help="Run MLflow qui contient le modèle (als_model/)")
    p.add_argument("--artifact-path", default="als_model")
    p.add_argument("--name", default=MODEL_NAME, help="Nom du modèle dans le Registry")
    p.add_argument("--alias", default="champion", help="Étiquette à poser sur cette version")
    args = p.parse_args()

    client = MlflowClient()
    run = client.get_run(args.run_id)

    # Garde-fous : on ne promeut qu'un run terminé, évalué sur le TEST, et qui contient un modèle.
    if run.info.status != "FINISHED":
        raise SystemExit(f"Run {args.run_id} non terminé ({run.info.status}).")
    if run.data.params.get("split") != "test":
        raise SystemExit("On ne promeut que des modèles évalués sur le split 'test'.")

    # 1) Nouvelle VERSION du modèle nommé (v1, v2, ...) à partir des fichiers du run.
    version = mlflow.register_model(f"runs:/{args.run_id}/{args.artifact_path}", args.name)

    # 2) Description lisible dans l'interface : réglages + scores de test.
    p_, m = run.data.params, run.data.metrics
    client.update_model_version(
        args.name, version.version,
        description=(f"ALS rank={p_.get('rank')} alpha={p_.get('alpha')} regParam={p_.get('regParam')} | "
                     f"test : recall@10 connus={m.get('known_recall_at_10', float('nan')):.4f}, "
                     f"tous={m.get('all_recall_at_10', float('nan')):.4f}"))
    for key in ("known_recall_at_10", "known_ndcg_at_10", "all_recall_at_10"):
        if key in m:
            client.set_model_version_tag(args.name, version.version, key, f"{m[key]:.5f}")

    # 3) L'ALIAS : une étiquette qui pointe vers UNE version. Changer de champion = déplacer l'étiquette.
    client.set_registered_model_alias(args.name, args.alias, version.version)

    print(f"Modèle '{args.name}' version {version.version} enregistré, alias '{args.alias}'.")
    print(f"Adresse à utiliser pour le charger : models:/{args.name}@{args.alias}")


if __name__ == "__main__":
    main()