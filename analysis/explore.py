"""Exploration du data lake : les chiffres qui guident la construction du modèle.

Lit data/lake/events (Parquet) et produit dans docs/eda/ :
  * summary.json / summary.md : les indicateurs clés ;
  * 4 graphiques PNG (activité par jour, événements par utilisateur,
    concentration de la popularité, activité par heure).

Usage (depuis le conteneur spark-client) :
  spark-submit analysis/explore.py
"""
import argparse
import json
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # pas d'écran dans le conteneur : on écrit directement des fichiers PNG
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from spark_jobs.lib.session import get_spark

# Palette sobre et constante pour tous les graphiques (une seule série par graphique).
COLOR = "#2a78d6"
INK, MUTED, GRID = "#1d1d1b", "#6b6a66", "#e6e4df"

# Tranches pour la distribution du nombre d'événements par utilisateur (et par session).
BUCKETS = [(1, 1, "1"), (2, 2, "2"), (3, 4, "3-4"), (5, 9, "5-9"), (10, 19, "10-19"),
           (20, 49, "20-49"), (50, 99, "50-99"), (100, None, "100+")]


def bucketize(col):
    """Transforme un nombre en libellé de tranche (calculé dans Spark, pas en Python)."""
    expr = None
    for lo, hi, label in BUCKETS:
        cond = (col >= lo) if hi is None else col.between(lo, hi)
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr


def style(ax, title, subtitle=None):
    ax.set_title(title, loc="left", fontsize=13, color=INK, pad=24 if subtitle else 10)
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9.5, color=MUTED)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)


def fmt_thousands(ax, axis="y"):
    fmt = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}".replace(",", " "))
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--out", default="docs/eda")
    args = p.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    spark = get_spark("exploration")
    ev = spark.read.parquet(args.lake)
    s = {}  # les indicateurs clés, remplis au fur et à mesure

    # 1) Volumes globaux --------------------------------------------------------------
    print("1/6 Volumes globaux...")
    by_type = {r["event_type"]: r["count"] for r in ev.groupBy("event_type").count().collect()}
    s["events"] = sum(by_type.values())
    s["events_by_type"] = by_type
    # Un seul countDistinct par requête : plusieurs dans la même agrégation multiplient
    # les données à trier, ce qui est très lent sur 110 M de lignes.
    s["products"] = ev.select("product_id").distinct().count()

    # 2) Activité par jour ------------------------------------------------------------
    print("2/6 Activité par jour...")
    daily = sorted((str(r["event_date"]), r["count"])
                   for r in ev.groupBy("event_date").count().collect())
    s["top_days"] = sorted(daily, key=lambda d: -d[1])[:5]
    months = {}
    for d, n in daily:
        months.setdefault(d[:7], []).append(n)
    s["avg_events_per_day"] = {m: round(sum(v) / len(v)) for m, v in months.items()}

    fig, ax = plt.subplots(figsize=(10, 4))
    xs = list(range(len(daily)))
    ax.plot(xs, [n for _, n in daily], color=COLOR, linewidth=2, solid_capstyle="round")
    peak = max(range(len(daily)), key=lambda i: daily[i][1])
    ax.scatter([peak], [daily[peak][1]], s=40, color=COLOR, edgecolor="white", linewidth=2, zorder=3)
    right_half = peak > len(daily) / 2
    ax.annotate(f"{daily[peak][0]} : {daily[peak][1]:,}".replace(",", " "),
                (peak, daily[peak][1]), xytext=(-10 if right_half else 10, 0), textcoords="offset points",
                ha="right" if right_half else "left", va="center", fontsize=9, color=INK)
    lo, hi = min(n for _, n in daily), daily[peak][1]
    ax.set_ylim(lo - 0.05 * (hi - lo), hi + 0.12 * (hi - lo))
    ticks = [i for i, (d, _) in enumerate(daily) if d.endswith(("-01", "-15"))]
    ax.set_xticks(ticks, [daily[i][0][5:] for i in ticks])
    fmt_thousands(ax)
    style(ax, "Événements par jour", "Octobre et novembre 2019, toutes actions confondues")
    fig.tight_layout()
    fig.savefig(out / "daily_events.png", dpi=150)
    plt.close(fig)

    # 3) Événements par utilisateur ---------------------------------------------------
    print("3/6 Événements par utilisateur...")
    per_user = ev.groupBy("user_id").agg(
        F.count("*").alias("n"),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("buyer"))
    u = per_user.agg(F.count("*").alias("users"),
                     F.percentile_approx("n", 0.5).alias("median"),
                     F.avg("n").alias("mean"),
                     F.sum(F.when(F.col("n") >= 5, 1).otherwise(0)).alias("ge5"),
                     F.sum(F.when(F.col("n") == 1, 1).otherwise(0)).alias("eq1"),
                     F.sum("buyer").alias("buyers")).first()
    s["users"] = u["users"]
    s["events_per_user"] = {"median": u["median"], "mean": round(u["mean"], 1)}
    s["users_single_event_pct"] = round(100 * u["eq1"] / s["users"], 1)
    s["users_ge5_events_pct"] = round(100 * u["ge5"] / s["users"], 1)
    s["users_who_bought_pct"] = round(100 * u["buyers"] / s["users"], 1)

    dist = {r["bucket"]: r["count"]
            for r in per_user.groupBy(bucketize(F.col("n")).alias("bucket")).count().collect()}
    labels = [b[2] for b in BUCKETS]
    values = [dist.get(lab, 0) for lab in labels]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(labels, values, width=0.55, color=COLOR)
    for i, v in enumerate(values):
        ax.text(i, v, f"{100 * v / s['users']:.0f} %", ha="center", va="bottom", fontsize=8.5, color=MUTED)
    fmt_thousands(ax)
    ax.set_xlabel("Nombre d'événements de l'utilisateur sur les 2 mois", color=MUTED, fontsize=9)
    style(ax, "Utilisateurs par niveau d'activité",
          f"{s['users']:,} utilisateurs ; la plupart n'ont que très peu d'événements".replace(",", " "))
    fig.tight_layout()
    fig.savefig(out / "events_per_user.png", dpi=150)
    plt.close(fig)

    # 4) Concentration de la popularité (achats par produit) -------------------------
    print("4/6 Popularité des produits...")
    counts = sorted((r["count"] for r in ev.where(F.col("event_type") == "purchase")
                     .groupBy("product_id").count().collect()), reverse=True)
    total = sum(counts)
    s["products_purchased"] = len(counts)
    cum, share_products, share_purchases = 0, [0.0], [0.0]
    for i, c in enumerate(counts, 1):
        cum += c
        share_products.append(100 * i / len(counts))
        share_purchases.append(100 * cum / total)
    top1 = counts[: max(1, len(counts) // 100)]
    top20 = counts[: max(1, len(counts) // 5)]
    s["top1pct_products_share_of_purchases"] = round(100 * sum(top1) / total, 1)
    s["top20pct_products_share_of_purchases"] = round(100 * sum(top20) / total, 1)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(share_products, share_purchases, color=COLOR, linewidth=2)
    ax.fill_between(share_products, share_purchases, color=COLOR, alpha=0.10)
    x20 = 20
    y20 = s["top20pct_products_share_of_purchases"]
    ax.scatter([x20], [y20], s=40, color=COLOR, edgecolor="white", linewidth=2, zorder=3)
    ax.annotate(f"20 % des produits = {y20:.0f} % des achats", (x20, y20),
                xytext=(10, -18), textcoords="offset points", fontsize=9, color=INK)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 102)
    ax.set_xlabel("% des produits achetés, du plus vendu au moins vendu", color=MUTED, fontsize=9)
    ax.set_ylabel("% cumulé des achats", color=MUTED, fontsize=9)
    style(ax, "Concentration des achats",
          f"Les 1 % de produits les plus vendus font {s['top1pct_products_share_of_purchases']:.0f} % des achats")
    fig.tight_layout()
    fig.savefig(out / "popularity_concentration.png", dpi=150)
    plt.close(fig)

    # 5) Activité par heure (UTC) -----------------------------------------------------
    print("5/6 Activité par heure...")
    hourly = dict((r["h"], r["count"]) for r in
                  ev.groupBy(F.hour("event_time").alias("h")).count().collect())
    hours = list(range(24))
    vals = [100 * hourly.get(h, 0) / s["events"] for h in hours]
    s["peak_hour_utc"] = max(hours, key=lambda h: hourly.get(h, 0))
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(hours, vals, width=0.6, color=COLOR)
    ax.set_xticks(hours, [f"{h}h" for h in hours], fontsize=8)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f} %"))
    style(ax, "Répartition des événements par heure (UTC)",
          f"Pic à {s['peak_hour_utc']}h UTC")
    fig.tight_layout()
    fig.savefig(out / "hourly_activity.png", dpi=150)
    plt.close(fig)

    # 6) Sessions et matrice utilisateurs x produits ----------------------------------
    print("6/6 Sessions et densité de la matrice (le plus long)...")
    per_session = ev.groupBy("user_session").agg(
        F.count("*").alias("n"),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("bought"))
    ss = per_session.agg(F.count("*").alias("sessions"),
                         F.percentile_approx("n", 0.5).alias("median"),
                         F.avg("bought").alias("conv")).first()
    s["sessions"] = ss["sessions"]
    s["events_per_session_median"] = ss["median"]
    s["sessions_with_purchase_pct"] = round(100 * ss["conv"], 2)

    pairs = ev.select("user_id", "product_id").distinct().count()
    s["user_product_pairs"] = pairs
    s["matrix_density_pct"] = round(100 * pairs / (s["users"] * s["products"]), 5)
    s["duration_s"] = round(time.time() - t0, 1)

    (out / "summary.json").write_text(json.dumps(s, indent=2))
    (out / "summary.md").write_text(to_markdown(s))
    print(to_markdown(s))
    spark.stop()


def to_markdown(s: dict) -> str:
    n = lambda v: f"{v:,}".replace(",", " ")  # noqa: E731
    lines = [
        "| Indicateur | Valeur |", "|---|---|",
        f"| Événements | {n(s['events'])} |",
        f"| Utilisateurs | {n(s['users'])} |",
        f"| Produits | {n(s['products'])} |",
        f"| Sessions | {n(s['sessions'])} |",
        f"| Événements par utilisateur (médiane / moyenne) | {s['events_per_user']['median']} / {s['events_per_user']['mean']} |",
        f"| Utilisateurs avec 1 seul événement | {s['users_single_event_pct']} % |",
        f"| Utilisateurs avec au moins 5 événements | {s['users_ge5_events_pct']} % |",
        f"| Utilisateurs ayant acheté au moins une fois | {s['users_who_bought_pct']} % |",
        f"| Sessions contenant un achat | {s['sessions_with_purchase_pct']} % |",
        f"| Événements par session (médiane) | {s['events_per_session_median']} |",
        f"| Part des achats faite par le top 1 % des produits | {s['top1pct_products_share_of_purchases']} % |",
        f"| Part des achats faite par le top 20 % des produits | {s['top20pct_products_share_of_purchases']} % |",
        f"| Couples (utilisateur, produit) distincts | {n(s['user_product_pairs'])} |",
        f"| Densité de la matrice utilisateurs x produits | {s['matrix_density_pct']} % |",
        f"| Heure la plus active (UTC) | {s['peak_hour_utc']}h |",
        "", "Jours les plus actifs :", "",
    ]
    lines += [f"- {d} : {n(c)} événements" for d, c in s["top_days"]]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()