"""Ingestion : CSV bruts REES46 -> data lake Parquet partitionné par date.

Ce que fait le job :
  1. lit les CSV avec un schéma imposé ;
  2. parse les types et ajoute des colonnes dérivées ;
  3. isole les lignes invalides dans une zone de quarantaine (au lieu de les supprimer en silence) ;
  4. supprime les doublons ;
  5. écrit data/lake/events/event_date=YYYY-MM-DD/ ;
  6. produit un rapport qualité JSON.

Idempotent : relancer le job sur les mêmes dates réécrit uniquement ces partitions
(partitionOverwriteMode=dynamic). C'est ce qui permettra à Airflow de rejouer une journée.

Exemples :
  # tout octobre, 10 % des utilisateurs (conseillé sur un laptop)
  spark-submit spark_jobs/ingest.py --input data/raw/2019-Oct.csv --sample-pct 10

  # une seule journée (mode incrémental, utilisé plus tard par Airflow)
  spark-submit spark_jobs/ingest.py --input data/raw/2019-Nov.csv \
      --start-date 2019-11-01 --end-date 2019-11-01
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.schema import RAW_EVENTS_SCHEMA
from spark_jobs.lib.session import get_spark
from spark_jobs.lib.transforms import (LAKE_COLUMNS, add_reject_reason, deduplicate,
                                       filter_dates, parse_events, sample_users)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", required=True, nargs="+",
                   help="Un ou plusieurs CSV (.csv ou .csv.gz), wildcards acceptés")
    p.add_argument("--lake", default="data/lake/events", help="Dossier de sortie Parquet")
    p.add_argument("--quarantine", default="data/quarantine/events", help="Lignes rejetées")
    p.add_argument("--report-dir", default="reports/ingest")
    p.add_argument("--start-date", help="YYYY-MM-DD (inclus)")
    p.add_argument("--end-date", help="YYYY-MM-DD (inclus)")
    p.add_argument("--sample-pct", type=int, default=100,
                   help="Pourcentage d'utilisateurs à garder (1-100). 100 = tout.")
    return p.parse_args(argv)


def run(args) -> dict:
    t0 = time.time()
    spark = get_spark("ingest-events")

    raw = spark.read.csv(args.input, header=True, schema=RAW_EVENTS_SCHEMA, mode="PERMISSIVE")

    events = parse_events(raw)
    events = filter_dates(events, args.start_date, args.end_date)
    events = sample_users(events, args.sample_pct)
    classified = add_reject_reason(events)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # --- Passe 1 : une seule agrégation pour compter les lignes valides (par jour) et les rejets
    reason_counts, valid_dates = {}, set()
    for r in classified.groupBy("reject_reason", "event_date").count().collect():
        reason = r["reject_reason"] or "valid"
        reason_counts[reason] = reason_counts.get(reason, 0) + r["count"]
        if reason == "valid":
            valid_dates.add(r["event_date"])
    rows_read = sum(reason_counts.values())
    rows_valid = reason_counts.pop("valid", 0)

    # --- Quarantaine : on garde les lignes rejetées (un sous-dossier par exécution) pour analyse
    if reason_counts:
        (classified.where(F.col("reject_reason").isNotNull())
            .write.mode("overwrite").partitionBy("reject_reason")
            .parquet(f"{args.quarantine}/run={stamp}"))

    # --- Passe 2 : nettoyage + écriture du lake
    clean = deduplicate(classified.where(F.col("reject_reason").isNull())).select(*LAKE_COLUMNS)
    (clean
        .repartition("event_date")                   # ~1 fichier par jour : pas de "small files"
        .sortWithinPartitions("user_id", "event_time")  # meilleure compression + lectures par user
        .write.mode("overwrite")                     # dynamic : seules les dates présentes sont réécrites
        .partitionBy("event_date")
        .parquet(args.lake))

    # --- Contrôle : relecture (rapide, c'est du Parquet) des partitions écrites
    # On ne relit que les dates traitées par CETTE exécution (le lake peut contenir d'autres mois).
    written = spark.read.parquet(args.lake).where(F.col("event_date").isin(list(valid_dates)))
    per_day = {str(r["event_date"]): r["count"]
               for r in written.groupBy("event_date").count().orderBy("event_date").collect()}
    rows_written = sum(per_day.values())

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "inputs": args.input,
        "date_range": [args.start_date, args.end_date],
        "sample_pct": args.sample_pct,
        "rows_read": rows_read,
        "rows_rejected": sum(reason_counts.values()),
        "rejected_by_reason": reason_counts,
        "rows_valid": rows_valid,
        "duplicates_removed": rows_valid - rows_written,
        "rows_written": rows_written,
        "days_written": len(per_day),
        "rows_per_day": per_day,
        "duration_s": round(time.time() - t0, 1),
    }

    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"ingest_{stamp}.json").write_text(json.dumps(report, indent=2))

    print(json.dumps({k: v for k, v in report.items() if k != "rows_per_day"}, indent=2))
    return report


if __name__ == "__main__":
    run(parse_args())