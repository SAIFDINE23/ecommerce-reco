"""Baseline de popularité : recommander à TOUT LE MONDE les produits les plus
mis au panier / achetés pendant les derniers jours de la période d'entraînement.

C'est le score à battre : si un modèle personnalisé (ALS) ne fait pas mieux,
il ne sert à rien.

Les scores sont aussi envoyés dans MLflow (même expérience et mêmes noms
de métriques que ALS), pour comparer les deux modèles dans l'interface.

Exemple :
  spark-submit spark_jobs/baseline_popularity.py --pop-start 2019-10-18 --pop-end 2019-10-24
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import mlflow
from pyspark.sql import functions as F

from spark_jobs.lib.metrics import evaluate
from spark_jobs.lib.session import get_spark
from spark_jobs.lib.splits import TARGET_EVENTS
from spark_jobs.lib.tracking import EXPERIMENT, log_metric_groups
from spark_jobs.lib.transforms import filter_dates


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--train", default="data/lake/interactions/train_filtered")
    p.add_argument("--truth", default="data/lake/eval/test_truth")
    p.add_argument("--pop-start", default="2019-10-18", help="Début de la fenêtre de popularité")
    p.add_argument("--pop-end", default="2019-10-24", help="Fin (= dernier jour d'entraînement)")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--report-dir", default="reports/eval")
    p.add_argument("--experiment", default=EXPERIMENT)
    args = p.parse_args()

    t0 = time.time()
    spark = get_spark("baseline-popularity")

    # 1) Les K produits les plus mis au panier / achetés sur la fenêtre (avant le test !).
    events = filter_dates(spark.read.parquet(args.lake), args.pop_start, args.pop_end)
    top = (events.where(F.col("event_type").isin(*TARGET_EVENTS))
                 .groupBy("product_id").count()
                 .orderBy(F.desc("count"))
                 .limit(args.k).collect())
    top_ids = [r["product_id"] for r in top]

    # 2) La même liste pour chaque utilisateur du test.
    truth = spark.read.parquet(args.truth)
    recs = truth.select("user_id").withColumn(
        "recs", F.array(*[F.lit(pid).cast("bigint") for pid in top_ids]))

    catalog_size = spark.read.parquet(args.train).select("product_id").distinct().count()

    # 3) Évaluation : tous les utilisateurs, puis séparément connus / inconnus du modèle.
    results = {
        "all": evaluate(recs, truth, args.k, catalog_size),
        "known": evaluate(recs, truth.where("known"), args.k),
        "cold": evaluate(recs, truth.where("not known"), args.k),
    }

    # 4) MLflow : un run « popularity » dans la même expérience que ALS.
    mlflow.set_experiment(args.experiment)
    with mlflow.start_run(run_name=f"popularity_{args.pop_start}_{args.pop_end}"):
        mlflow.set_tags({"model_type": "popularity", "evaluation": "test_25-31_oct"})
        mlflow.log_params({"pop_start": args.pop_start, "pop_end": args.pop_end, "k": args.k})
        log_metric_groups(results)
        mlflow.log_dict({"top_products": top_ids}, "top_products.json")

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": "popularity",
        "popularity_window": [args.pop_start, args.pop_end],
        "top_products": [{"product_id": r["product_id"], "carts_purchases": r["count"]} for r in top],
        "metrics": results,
        "duration_s": round(time.time() - t0, 1),
    }
    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"baseline_popularity_{stamp}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()