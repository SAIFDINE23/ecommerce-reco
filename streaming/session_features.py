"""Fiche de session de chaque visiteur, tenue à jour en temps réel dans Redis.

    Kafka « clics »  ──►  Spark Structured Streaming (micro-batch toutes les 5 s)  ──►  Redis

Pour chaque visiteur, Redis contient 3 clés :
  user:<id>:session  (HASH)  la session en cours : session_id, vues, paniers, achats,
                             heure du dernier clic, dernière catégorie vue
  user:<id>:recent   (LIST)  ses 20 derniers produits, le plus récent en premier
  user:<id>:cats     (ZSET)  les catégories de la session en cours et leur nombre de clics
Quand le visiteur change de session (nouveau user_session), les compteurs repartent de zéro.
Toutes les clés expirent après --ttl secondes sans activité (ménage automatique).

Lancer (dans spark-client) :
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
      --conf spark.jars.ivy=/opt/project/.ivy2 streaming/session_features.py
Puis lancer le simulateur dans un 2e terminal. Lire une fiche : streaming/show_session.py
"""
import argparse
import os
import time
from collections import Counter

import redis
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.lib.session import get_spark
from streaming.traffic_monitor import parse_clicks

BATCH_KEY = "stream:session_features:last_batch_id"   # dernier micro-batch écrit (anti-doublon)
RECENT_SIZE = 20


def keys(user_id: int) -> tuple[str, str, str]:
    return f"user:{user_id}:session", f"user:{user_id}:recent", f"user:{user_id}:cats"


def summarize(batch: DataFrame) -> DataFrame:
    """Résumé d'un micro-batch : une ligne par (visiteur, session)."""
    is_type = lambda t: (F.col("event_type") == t).cast("int")
    return (batch
            .groupBy("user_id", "user_session")
            .agg(F.min("event_time").alias("first_time"),
                 # formatée par Spark (en UTC) : un .collect() convertirait l'heure dans le fuseau local
                 F.date_format(F.max("event_time"), "yyyy-MM-dd HH:mm:ss").alias("last_time"),
                 F.sum(is_type("view")).alias("views"),
                 F.sum(is_type("cart")).alias("carts"),
                 F.sum(is_type("purchase")).alias("purchases"),
                 F.max_by("category_code", "event_time").alias("last_category"),
                 F.max("sent_at_ms").alias("sent_at_ms"),
                 # produits dans l'ordre chronologique (tri du tableau de (heure, produit))
                 F.transform(F.array_sort(F.collect_list(F.struct("event_time", "product_id"))),
                             lambda s: s["product_id"]).alias("products"),
                 F.collect_list("category_code").alias("categories")))


def make_batch_writer(r: redis.Redis, ttl: int):
    """Renvoie la fonction appelée par Spark à chaque micro-batch (foreachBatch, dans le driver)."""

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        # 1) Anti-doublon : après un plantage, Spark peut rejouer le DERNIER micro-batch.
        #    Comme HINCRBY n'est pas idempotent, on ignore un batch déjà écrit.
        last = r.get(BATCH_KEY)
        if last is not None and batch_id <= int(last):
            print(f"micro-batch {batch_id} déjà écrit : ignoré")
            return

        t0 = time.time()
        rows = summarize(batch).orderBy("user_id", "first_time").collect()
        if not rows:
            r.set(BATCH_KEY, batch_id)
            return

        # 2) Quelle session chaque visiteur a-t-il en ce moment dans Redis ? (1 aller-retour réseau)
        users = sorted({row.user_id for row in rows})
        pipe = r.pipeline(transaction=False)
        for uid in users:
            pipe.hget(keys(uid)[0], "session_id")
        current = dict(zip(users, pipe.execute()))

        # 3) Toutes les écritures du micro-batch, en UNE transaction (tout ou rien).
        now_ms = int(time.time() * 1000)
        pipe = r.pipeline(transaction=True)
        for row in rows:
            skey, rkey, ckey = keys(row.user_id)
            if current.get(row.user_id) != row.user_session:     # nouvelle session : on repart de zéro
                pipe.delete(skey, ckey)
                current[row.user_id] = row.user_session
            pipe.hset(skey, mapping={
                "session_id": row.user_session,
                "last_event_time": row.last_time,
                "last_category": row.last_category or "",
                "updated_at_ms": now_ms,
            })
            pipe.hincrby(skey, "views", row.views)
            pipe.hincrby(skey, "carts", row.carts)
            pipe.hincrby(skey, "purchases", row.purchases)
            if row.products:
                pipe.lpush(rkey, *row.products)          # le plus récent finit en tête de liste
                pipe.ltrim(rkey, 0, RECENT_SIZE - 1)
            for cat, n in Counter(c for c in row.categories if c).items():
                pipe.zincrby(ckey, n, cat)
            for k in (skey, rkey, ckey):
                pipe.expire(k, ttl)
        pipe.set(BATCH_KEY, batch_id)
        pipe.execute()

        # 4) Suivi : combien, en combien de temps, et avec quelle latence (envoi -> fiche Redis).
        write_ms = (time.time() - t0) * 1000
        lat = sorted(now_ms - row.sent_at_ms for row in rows)
        p50, p95 = lat[len(lat) // 2] / 1000, lat[int(len(lat) * 0.95)] / 1000
        best = max(rows, key=lambda x: x.views + x.carts + x.purchases)
        print(f"micro-batch {batch_id:>4} | {len(rows):>6} sessions mises à jour | "
              f"{write_ms:>6.0f} ms | latence p50 {p50:.1f} s, p95 {p95:.1f} s | "
              f"ex. visiteur {best.user_id}")

    return write_batch


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="clics")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    p.add_argument("--starting-offsets", default="latest",
                   help="1er lancement seulement : latest = seulement les nouveaux clics")
    p.add_argument("--max-per-trigger", type=int, default=100_000)
    p.add_argument("--trigger", default="5 seconds")
    p.add_argument("--ttl", type=int, default=7200, help="Durée de vie des clés sans activité (s)")
    p.add_argument("--checkpoint", default="checkpoints/session_features")
    args = p.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    r.ping()                                              # échoue tout de suite si Redis est injoignable

    spark = get_spark("session-features")
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_per_trigger)
           .load())

    query = (parse_clicks(raw).writeStream
             .queryName("session_features")
             .foreachBatch(make_batch_writer(r, args.ttl))
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=args.trigger)
             .start())
    print(f"Kafka '{args.topic}' -> Redis ({args.redis_url}) — micro-batch toutes les {args.trigger}. "
          f"Ctrl+C pour arrêter.")
    query.awaitTermination()


if __name__ == "__main__":
    main()