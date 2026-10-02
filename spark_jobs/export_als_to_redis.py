"""Exporte les vecteurs du modèle ALS « champion » (MLflow) vers Redis, pour l'API temps réel.

    MLflow  models:/als-recommender@champion
       │  chargement du modèle Spark (les 2 tables de vecteurs)
       ▼
    Redis   als:v<version>:item_ids / item_factors   tous les produits (2 blocs binaires)
            als:v<version>:user:<id>                 1 vecteur par personne ACTIVE récemment
            als:meta                                 quelle version est « en service »

Pourquoi des vecteurs et pas des listes de 100 produits pré-calculées ?
  - pré-calculer le top 100 de ~1 million de personnes est très long (des dizaines de minutes) ;
  - avec les vecteurs, l'API calcule x · y pour tous les produits en quelques millisecondes,
    et peut aussi donner un score ALS à des candidats venus d'ailleurs (session, popularité).

Changement de version sans coupure : on écrit la nouvelle version à côté de l'ancienne,
PUIS on bascule als:meta ; l'ancienne version expire toute seule (TTL).

Exemple (dans spark-client) :
  spark-submit spark_jobs/export_als_to_redis.py
  spark-submit spark_jobs/export_als_to_redis.py --active-since 2019-10-18 --active-until 2019-10-24
"""
import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.spark
import numpy as np
from mlflow import MlflowClient
import uuid
import shutil

from pyspark.sql import functions as F

from serving.als_vectors import META_KEY, connect, load_items, top_k, user_key
from spark_jobs.lib.session import get_spark
from spark_jobs.lib.transforms import filter_dates


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model-name", default="als-recommender")
    p.add_argument("--alias", default="champion")
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--active-since", default="2019-10-18",
                   help="On exporte les personnes actives entre ces 2 dates (les autres = cold start)")
    p.add_argument("--active-until", default="2019-10-24")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    p.add_argument("--ttl-days", type=int, default=3, help="Durée de vie des vecteurs (renouvelée à chaque export)")
    p.add_argument("--batch", type=int, default=5000, help="Écritures Redis par paquet")
    args = p.parse_args()

    t0 = time.time()
    r = connect(args.redis_url)
    r.ping()
    spark = get_spark("export-als-to-redis")
    ttl = args.ttl_days * 86400

    # 1) Le modèle en service : quelle version, et ses vecteurs.
    mv = MlflowClient().get_model_version_by_alias(args.model_name, args.alias)
    version = mv.version
        # Le modèle est téléchargé dans un dossier PARTAGÉ (le projet, monté dans tous les conteneurs) :
    # sinon il irait dans /tmp du seul conteneur spark-client, invisible pour le worker.
    download_dir = Path("checkpoints/mlflow_tmp") / f"load_{uuid.uuid4().hex[:8]}"
    download_dir.mkdir(parents=True)
    model = mlflow.spark.load_model(f"models:/{args.model_name}@{args.alias}",
                                    dst_path=str(download_dir.resolve()))
    als = model.stages[-1]
    rank = als.rank
    print(f"Modèle {args.model_name} v{version} (@{args.alias}, run {mv.run_id[:8]}…), rank {rank}")

    # 2) Les produits : ~100 000 vecteurs -> 2 blocs binaires (ids + matrice), lus d'un coup par l'API.
    items = als.itemFactors.orderBy("id").collect()
    item_ids = np.array([row.id for row in items], dtype=np.int64)
    item_factors = np.array([row.features for row in items], dtype=np.float32)
    pipe = r.pipeline(transaction=False)
    pipe.set(f"als:v{version}:item_ids", item_ids.tobytes(), ex=ttl)
    pipe.set(f"als:v{version}:item_factors", item_factors.tobytes(), ex=ttl)
    pipe.execute()
    print(f"Produits : {len(item_ids):,} vecteurs ({item_factors.nbytes / 1e6:.1f} Mo)".replace(",", " "))

    # 3) Les personnes ACTIVES récemment et connues du modèle -> 1 clé chacune (rank x 4 octets).
    active = (filter_dates(spark.read.parquet(args.lake), args.active_since, args.active_until)
              .select(F.col("user_id").cast("int").alias("id")).distinct())
    users = als.userFactors.join(active, "id", "inner")
    n_users, pipe = 0, r.pipeline(transaction=False)
    for row in users.toLocalIterator(prefetchPartitions=True):   # partition par partition : peu de RAM
        pipe.set(user_key(version, row.id), np.asarray(row.features, dtype=np.float32).tobytes(), ex=ttl)
        n_users += 1
        if n_users % args.batch == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    print(f"Personnes actives exportées : {n_users:,}".replace(",", " "))

    # 4) Contrôle de cohérence AVANT la bascule : Redis + numpy doit donner le même top 10 que Spark.
    sample = [row.id for row in users.select("id").limit(3).collect()]
    meta_new = {"version": version, "rank": rank}
    subset = spark.createDataFrame([(u,) for u in sample], [als.getUserCol()])
    spark_top = {row[0]: [rec[0] for rec in row.recommendations]      # (user_id, [(product_id, score)...])
                 for row in als.recommendForUserSubset(subset, 10).collect()}
    for uid in sample:
        ours = [pid for pid, _ in top_k(r, meta_new, item_ids, item_factors, uid, 10)]
        common = len(set(ours) & set(spark_top[uid]))
        print(f"  contrôle personne {uid} : {common}/10 produits identiques à Spark")
        assert common >= 9, "Incohérence entre Redis et le modèle Spark : bascule annulée"

    # 5) La BASCULE : l'API lira désormais cette version.
    r.hset(META_KEY, mapping={
        "version": version, "model_name": args.model_name, "alias": args.alias, "run_id": mv.run_id,
        "rank": rank, "n_items": len(item_ids), "n_users": n_users,
        "active_window": f"{args.active_since}..{args.active_until}",
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    print(f"Bascule faite : als:meta -> version {version}. Durée totale {time.time() - t0:.0f} s")

    # 6) Petite démonstration de la vitesse côté API.
    meta, ids, factors = load_items(r)
    t = time.perf_counter()
    top_k(r, meta, ids, factors, sample[0], 100)
    print(f"Top 100 d'une personne calculé depuis Redis en {(time.perf_counter() - t) * 1000:.1f} ms")
    spark.stop()
    shutil.rmtree(download_dir, ignore_errors=True)       # ménage : le modèle téléchargé ne sert plus

if __name__ == "__main__":
    main()