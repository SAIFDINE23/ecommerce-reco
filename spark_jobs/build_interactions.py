"""Construit la table d'interactions à partir du data lake, sur une période donnée.

Exemple (période d'entraînement d'octobre) :
  spark-submit spark_jobs/build_interactions.py \
      --start-date 2019-10-01 --end-date 2019-10-24 --out data/lake/interactions/train
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.interactions import EVENT_WEIGHTS, build_interactions
from spark_jobs.lib.session import get_spark
from spark_jobs.lib.transforms import filter_dates


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--out", required=True, help="Dossier Parquet de sortie")
    p.add_argument("--report-dir", default="reports/interactions")
    args = p.parse_args()

    t0 = time.time()
    spark = get_spark("build-interactions")

    # Grâce au partitionnement par jour, Spark ne lit QUE les dossiers de la période.
    events = filter_dates(spark.read.parquet(args.lake), args.start_date, args.end_date)
    inter = build_interactions(events)

    inter.write.mode("overwrite").parquet(args.out)

    # Contrôle : on relit la table écrite pour décrire ce qu'elle contient.
    written = spark.read.parquet(args.out)
    s = written.agg(
        F.count("*").alias("pairs"),
        F.countDistinct("user_id").alias("users"),
        F.sum("n_views").alias("views"),
        F.sum("n_carts").alias("carts"),
        F.sum("n_purchases").alias("purchases"),
        F.sum(F.when(F.col("n_purchases") > 0, 1).otherwise(0)).alias("pairs_with_purchase"),
        F.percentile_approx("score", [0.5, 0.9, 0.99]).alias("score_pcts"),
        F.max("score").alias("score_max"),
    ).first()
    products = written.select("product_id").distinct().count()

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "period": [args.start_date, args.end_date],
        "weights": EVENT_WEIGHTS,
        "pairs": s["pairs"],
        "users": s["users"],
        "products": products,
        "events": {"view": s["views"], "cart": s["carts"], "purchase": s["purchases"]},
        "pairs_with_purchase": s["pairs_with_purchase"],
        "score_median_p90_p99": s["score_pcts"],
        "score_max": s["score_max"],
        "duration_s": round(time.time() - t0, 1),
    }
    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"interactions_{stamp}.json").write_text(json.dumps(report, indent=2))

    print(json.dumps(report, indent=2))
    print("\nExemple : les 5 couples au score le plus élevé")
    written.orderBy(F.desc("score")).show(5)
    spark.stop()


if __name__ == "__main__":
    main()