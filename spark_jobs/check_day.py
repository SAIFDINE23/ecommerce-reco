"""Contrôle qualité d'une journée du data lake, AVANT de s'en servir pour entraîner ou publier.

On compare le jour D aux 7 jours précédents (la « normale ») :
  - volume       : le nombre d'événements ne doit pas s'effondrer ni exploser ;
  - achats       : il doit y en avoir, et leur part doit rester plausible ;
  - visiteurs    : le nombre de personnes distinctes doit rester plausible.

Deux niveaux :
  ALERTE  (WARN) : écart notable, on le signale mais on continue (ex. un jour de soldes) ;
  BLOQUANT (FAIL): donnée manifestement cassée -> le job sort avec le code 1,
                   Airflow passe la tâche en rouge et NE publie RIEN ce jour-là.

Le verdict est aussi écrit dans reports/quality/day_<D>.json : l'entraînement des nuits
suivantes le relit pour écarter les journées cassées de sa fenêtre.

Exemple (dans spark-client) :
  spark-submit spark_jobs/check_day.py --date 2019-11-15
"""
import argparse
import json
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.session import get_spark
from spark_jobs.lib.transforms import filter_dates

# (nom, colonne, seuil ALERTE, seuil BLOQUANT) : rapport au jour normal (médiane des 7 jours d'avant).
# Exemple : volume 0,25 = 4 fois moins d'événements que d'habitude -> BLOQUANT.
RULES = [
    ("volume",          "events",        (0.5, 2.0), (0.2, 5.0)),
    ("visiteurs",       "users",         (0.5, 2.0), (0.2, 5.0)),
    ("part d'achats",   "purchase_rate", (0.5, 2.0), (0.2, 5.0)),
    ("part de paniers", "cart_rate",     (0.5, 2.0), (0.2, 5.0)),
]


def daily_stats(events):
    """Une ligne par jour : nombre d'événements, de personnes, d'achats, de paniers."""
    rows = (events.groupBy("event_date")
            .agg(F.count("*").alias("events"),
                 F.countDistinct("user_id").alias("users"),
                 F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
                 F.sum((F.col("event_type") == "cart").cast("int")).alias("carts"))
            .collect())
    stats = {}
    for r in rows:
        stats[r["event_date"].isoformat()] = {
            "events": r["events"], "users": r["users"],
            "purchases": r["purchases"], "carts": r["carts"],
            "purchase_rate": r["purchases"] / r["events"],
            "cart_rate": r["carts"] / r["events"],
        }
    return stats


def judge(day: dict | None, history: list[dict]) -> tuple[str, list[dict]]:
    """Renvoie ("OK" | "WARN" | "FAIL", détail de chaque règle)."""
    if day is None or day["events"] == 0:
        return "FAIL", [{"rule": "données présentes", "status": "FAIL", "detail": "aucun événement ce jour-là"}]
    checks = []
    if day["purchases"] == 0:
        checks.append({"rule": "achats présents", "status": "FAIL", "detail": "0 achat sur la journée"})
    if len(history) < 3:
        checks.append({"rule": "historique", "status": "WARN",
                       "detail": f"seulement {len(history)} jour(s) de référence, comparaison limitée"})
    else:
        for name, col, (w_lo, w_hi), (f_lo, f_hi) in RULES:
            normal = statistics.median(h[col] for h in history)
            ratio = day[col] / normal if normal else float("inf")
            status = ("FAIL" if not f_lo <= ratio <= f_hi else
                      "WARN" if not w_lo <= ratio <= w_hi else "OK")
            checks.append({"rule": name, "status": status, "ratio": round(ratio, 3),
                           "value": round(day[col], 5), "normal": round(normal, 5)})
    statuses = {c["status"] for c in checks}
    verdict = "FAIL" if "FAIL" in statuses else "WARN" if "WARN" in statuses else "OK"
    return verdict, checks


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="Jour contrôlé, YYYY-MM-DD")
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--history-days", type=int, default=7)
    p.add_argument("--report-dir", default="reports/quality")
    args = p.parse_args()

    t0 = time.time()
    d = date.fromisoformat(args.date)
    first = (d - timedelta(days=args.history_days)).isoformat()
    spark = get_spark("check-day")

    # Grâce au partitionnement par jour, Spark ne lit que les 8 dossiers concernés.
    stats = daily_stats(filter_dates(spark.read.parquet(args.lake), first, args.date))
    spark.stop()

    day = stats.get(args.date)
    history = [stats[k] for k in sorted(stats) if k != args.date]
    verdict, checks = judge(day, history)

    report = {
        "date": args.date,
        "verdict": verdict,
        "ok": verdict != "FAIL",
        "day": day,
        "history_days_found": len(history),
        "checks": checks,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
    }
    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"day_{args.date}.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))

    print(f"\nContrôle qualité du {args.date} (comparé aux {len(history)} jours précédents)")
    if day:
        print(f"  événements {day['events']:,} | visiteurs {day['users']:,} | "
              f"paniers {day['carts']:,} | achats {day['purchases']:,}".replace(",", " "))
    for c in checks:
        extra = (f"jour {c['value']:<10} normale {c['normal']:<10} rapport x{c['ratio']}"
                 if "ratio" in c else c["detail"])
        print(f"  [{c['status']:^4}] {c['rule']:<17} {extra}")
    print(f"VERDICT : {verdict}")

    if verdict == "FAIL":
        # Code de sortie 1 : Airflow passe la tâche en rouge, les étapes suivantes ne tournent pas.
        sys.exit(1)


if __name__ == "__main__":
    main()