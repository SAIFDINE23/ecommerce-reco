"""Listes de popularité récentes -> Redis : le filet de sécurité de l'API pour le cold start.

Pour une personne inconnue (aucun vecteur ALS), l'API a besoin de quelque chose à montrer :
  pop:global          les 100 produits les plus mis au panier / achetés sur les 7 derniers jours
  pop:cat:<catégorie> les 50 premiers de chaque grande catégorie (electronics, appliances...)
                      -> dès le 1er clic, on sait dans quelle catégorie la personne navigue
  pop:meta            pour quel jour et quelle fenêtre ces listes ont été calculées

Comme pour les vecteurs ALS, on écrit d'abord la nouvelle version À CÔTÉ (pop:v<jour>:...),
puis on bascule pop:meta en une seule écriture : l'API ne voit jamais une liste à moitié écrite.

Exemple (dans spark-client) :
  spark-submit spark_jobs/export_popularity.py --date 2019-11-14
  python -c "import redis; r=redis.Redis(host='redis'); print(r.hgetall('pop:meta'))"
"""
import argparse
import os
import time
from datetime import date, datetime, timedelta, timezone

import redis
from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_jobs.lib.session import get_spark
from spark_jobs.lib.splits import TARGET_EVENTS
from spark_jobs.lib.transforms import filter_dates

META_KEY = "pop:meta"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="Dernier jour inclus, YYYY-MM-DD")
    p.add_argument("--days", type=int, default=7, help="Taille de la fenêtre (jours)")
    p.add_argument("--k-global", type=int, default=100)
    p.add_argument("--k-category", type=int, default=50)
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    p.add_argument("--ttl-days", type=int, default=3)
    args = p.parse_args()

    t0 = time.time()
    first = (date.fromisoformat(args.date) - timedelta(days=args.days - 1)).isoformat()
    spark = get_spark("export-popularity")

    # Score d'un produit = nombre de PERSONNES distinctes qui l'ont mis au panier ou acheté.
    # (Compter des personnes plutôt que des clics : un robot qui ajoute 500 fois le même
    #  produit ne doit pas le propulser en tête.)
    scores = (filter_dates(spark.read.parquet(args.lake), first, args.date)
              .where(F.col("event_type").isin(*TARGET_EVENTS))
              .groupBy("category_l1", "product_id")
              .agg(F.countDistinct("user_id").alias("buyers")))
    scores = scores.cache()

    top_global = (scores.groupBy("product_id").agg(F.sum("buyers").alias("buyers"))
                  .orderBy(F.desc("buyers"), "product_id").limit(args.k_global).collect())

    rank = F.row_number().over(Window.partitionBy("category_l1")
                               .orderBy(F.desc("buyers"), "product_id"))
    per_cat = (scores.withColumn("rank", rank).where(F.col("rank") <= args.k_category)
               .orderBy("category_l1", "rank").collect())
    spark.stop()

    by_cat: dict[str, list[int]] = {}
    for row in per_cat:
        by_cat.setdefault(row["category_l1"], []).append(row["product_id"])

    # Écriture : nouvelle version à côté, puis bascule de pop:meta (une seule transaction).
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    prefix, ttl = f"pop:v{args.date}", args.ttl_days * 86400
    pipe = r.pipeline(transaction=True)
    keys = {f"{prefix}:global": [row["product_id"] for row in top_global]}
    keys.update({f"{prefix}:cat:{cat}": ids for cat, ids in by_cat.items()})
    for key, ids in keys.items():
        pipe.delete(key)
        pipe.rpush(key, *ids)          # liste ORDONNÉE : le 1er élément est le plus populaire
        pipe.expire(key, ttl)
    pipe.delete(META_KEY)
    pipe.hset(META_KEY, mapping={
        "prefix": prefix, "date": args.date, "window": f"{first}..{args.date}",
        "categories": ",".join(sorted(by_cat)),
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    pipe.execute()

    print(f"\nPopularité du {first} au {args.date} -> Redis ({prefix}:*)")
    print(f"  global : {len(keys[f'{prefix}:global'])} produits, top 5 = {keys[f'{prefix}:global'][:5]}")
    for cat in sorted(by_cat):
        print(f"  {cat:<14} {len(by_cat[cat]):>3} produits, top 3 = {by_cat[cat][:3]}")
    print(f"Durée : {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()