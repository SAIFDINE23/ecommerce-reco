"""Affiche le classement des runs MLflow d'un découpage (val ou test), du meilleur au moins bon.

Exemple (dans le conteneur spark-client) :
  python analysis/compare_runs.py --split val
  python analysis/compare_runs.py --split val --metric known_ndcg_at_10
"""
import argparse

import mlflow
import pandas as pd

from spark_jobs.lib.tracking import EXPERIMENT

COLUMNS = {  # colonne MLflow -> nom court affiché
    "tags.mlflow.runName": "run",
    "params.rank": "rank",
    "params.alpha": "alpha",
    "params.regParam": "reg",
    "params.maxIter": "iter",
    "metrics.known_recall_at_10": "recall_known",
    "metrics.known_ndcg_at_10": "ndcg_known",
    "metrics.all_recall_at_10": "recall_all",
    "metrics.known_coverage": "coverage",
    "metrics.fit_s": "fit_s",
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--split", default="val", choices=["val", "test"])
    p.add_argument("--metric", default="known_recall_at_10", help="Métrique de classement")
    p.add_argument("--experiment", default=EXPERIMENT)
    args = p.parse_args()

    runs = mlflow.search_runs(
        experiment_names=[args.experiment],
        filter_string=f"params.split = '{args.split}' and attributes.status = 'FINISHED'",
        order_by=[f"metrics.{args.metric} DESC"],
    )
    if runs.empty:
        print("Aucun run terminé pour ce découpage.")
        return

    table = runs[[c for c in COLUMNS if c in runs.columns]].rename(columns=COLUMNS)
    # Référence : le run de popularité du même découpage (le score à battre).
    pop = table["run"].str.contains("popularity", na=False)
    best_pop = table.loc[pop, "recall_known"].max() if pop.any() else None
    if best_pop:
        table["vs_pop"] = (table["recall_known"] / best_pop - 1).map(lambda x: f"{x:+.0%}")

    pd.set_option("display.width", 200)
    print(f"Classement des runs '{args.split}' par {args.metric} :\n")
    print(table.to_string(index=False, float_format=lambda x: f"{x:.4f}"))


if __name__ == "__main__":
    main()