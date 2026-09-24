"""Benchmark : la même question posée au CSV brut puis au data lake Parquet.

Question : « combien d'achats par grande catégorie, tel jour ? »

Usage (depuis le conteneur spark-client) :
  spark-submit spark_jobs/benchmark_formats.py --csv data/raw/2019-Nov.csv --date 2019-11-15
"""
import argparse
import re
import time

from pyspark.sql import functions as F

from spark_jobs.lib.schema import RAW_EVENTS_SCHEMA
from spark_jobs.lib.session import get_spark
from spark_jobs.lib.transforms import parse_events


def scan_info(df) -> list[str]:
    """Extrait du plan d'exécution ce que Spark lit vraiment (dossiers, filtres, colonnes)."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    found = re.findall(r"(PartitionFilters|PushedFilters|ReadSchema): (\[[^\]]*\]|struct<[^>]*>)", plan)
    return list(dict.fromkeys(f"{k}: {v}" for k, v in found))   # sans doublons, ordre conservé


def timed(label: str, df) -> dict:
    t0 = time.time()
    rows = {r["category_l1"]: r["purchases"] for r in df.collect()}
    seconds = time.time() - t0
    print(f"\n=== {label} : {seconds:.1f} s ===")
    for info in scan_info(df):
        print(f"  {info[:140]}")
    return {"seconds": seconds, "rows": rows}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="data/raw/2019-Nov.csv")
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--date", default="2019-11-15")
    args = p.parse_args()

    spark = get_spark("benchmark-csv-vs-parquet")

    # 1) CSV brut : il faut tout lire, convertir le texte en date, puis filtrer.
    csv_df = (parse_events(spark.read.csv(args.csv, header=True, schema=RAW_EVENTS_SCHEMA))
              .where((F.col("event_date") == args.date) & (F.col("event_type") == "purchase"))
              .groupBy("category_l1").agg(F.count("*").alias("purchases")))

    # 2) Data lake Parquet : Spark ne lit que le dossier du jour et les colonnes utiles.
    pq_df = (spark.read.parquet(args.lake)
             .where((F.col("event_date") == args.date) & (F.col("event_type") == "purchase"))
             .groupBy("category_l1").agg(F.count("*").alias("purchases")))

    csv = timed("CSV brut", csv_df)
    pq = timed("Parquet (data lake)", pq_df)

    print(f"\nAchats le {args.date} par catégorie (Parquet) :")
    for cat, n in sorted(pq["rows"].items(), key=lambda kv: -kv[1]):
        print(f"  {cat:<14} {n:>8,}".replace(",", " "))

    # Les chiffres ne sont pas forcément identiques : le lake est dédoublonné, pas le CSV.
    total_csv, total_pq = sum(csv["rows"].values()), sum(pq["rows"].values())
    print(f"\nTotal achats : CSV = {total_csv:,}   Parquet = {total_pq:,}   "
          f"(écart = doublons supprimés à l'ingestion)".replace(",", " "))
    print(f"Accélération : Parquet est {csv['seconds'] / pq['seconds']:.0f}x plus rapide\n")

    spark.stop()


if __name__ == "__main__":
    main()