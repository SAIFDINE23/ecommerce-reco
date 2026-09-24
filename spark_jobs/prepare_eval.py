"""Prépare le jeu d'entraînement filtré et la vérité terrain du test (découpage temporel).

  entraînement : interactions du 1er au 24 octobre (déjà construites en 2.1), filtrées
  test         : ce que les utilisateurs ont mis au panier ou acheté du 25 au 31 octobre

Exemple :
  spark-submit spark_jobs/prepare_eval.py \
      --train data/lake/interactions/train \
      --test-start 2019-10-25 --test-end 2019-10-31
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.session import get_spark
from spark_jobs.lib.splits import build_ground_truth, filter_interactions
from spark_jobs.lib.transforms import filter_dates


def describe(df):
    r = df.agg(F.count("*").alias("pairs"),
               F.countDistinct("user_id").alias("users")).first()
    return {"pairs": r["pairs"], "users": r["users"],
            "products": df.select("product_id").distinct().count()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--train", default="data/lake/interactions/train")
    p.add_argument("--test-start", default="2019-10-25")
    p.add_argument("--test-end", default="2019-10-31")
    p.add_argument("--min-user-events", type=int, default=5)
    p.add_argument("--min-item-events", type=int, default=10)
    p.add_argument("--out-train", default="data/lake/interactions/train_filtered")
    p.add_argument("--out-truth", default="data/lake/eval/test_truth")
    p.add_argument("--report-dir", default="reports/eval")
    args = p.parse_args()

    t0 = time.time()
    spark = get_spark("prepare-eval")

    # 1) Entraînement : on filtre les utilisateurs et produits trop rares.
    train = spark.read.parquet(args.train)
    before = describe(train)
    filtered = filter_interactions(train, args.min_user_events, args.min_item_events)
    filtered.write.mode("overwrite").parquet(args.out_train)
    filtered = spark.read.parquet(args.out_train)
    after = describe(filtered)

    # 2) Test : la vérité terrain, construite UNIQUEMENT à partir de la période de test.
    test_events = filter_dates(spark.read.parquet(args.lake), args.test_start, args.test_end)
    truth = build_ground_truth(test_events)

    # On marque les utilisateurs du test connus du modèle (présents dans l'entraînement filtré).
    known_users = filtered.select("user_id").distinct().withColumn("known", F.lit(True))
    truth = (truth.join(known_users, "user_id", "left")
                  .withColumn("known", F.coalesce("known", F.lit(False))))
    truth.write.mode("overwrite").parquet(args.out_truth)
    truth = spark.read.parquet(args.out_truth)

    t = truth.agg(F.count("*").alias("users"),
                  F.sum(F.col("known").cast("int")).alias("known_users"),
                  F.avg("n_truth").alias("avg_items")).first()

    # Quelle part des produits du test le modèle a-t-il déjà vus à l'entraînement ?
    test_items = truth.select(F.explode("truth").alias("product_id")).distinct()
    train_items = filtered.select("product_id").distinct()
    n_test_items = test_items.count()
    n_known_items = test_items.join(train_items, "product_id", "left_semi").count()

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "filters": {"min_user_events": args.min_user_events, "min_item_events": args.min_item_events},
        "train_before_filter": before,
        "train_after_filter": after,
        "test_period": [args.test_start, args.test_end],
        "test_users": t["users"],
        "test_users_known_by_model": t["known_users"],
        "test_users_known_pct": round(100 * t["known_users"] / t["users"], 1),
        "avg_truth_items_per_user": round(t["avg_items"], 2),
        "test_items": n_test_items,
        "test_items_seen_in_train_pct": round(100 * n_known_items / n_test_items, 1),
        "duration_s": round(time.time() - t0, 1),
    }
    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"prepare_eval_{stamp}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    print("\nExemple de vérité terrain (utilisateurs connus du modèle) :")
    truth.where("known").orderBy(F.desc("n_truth")).show(5, truncate=80)
    spark.stop()


if __name__ == "__main__":
    main()