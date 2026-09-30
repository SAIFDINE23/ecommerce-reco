"""Tableau de bord temps réel du site : Spark Structured Streaming lit le topic Kafka « clics »
en continu et affiche, toutes les 10 secondes, l'activité par tranches de 5 minutes
(selon l'HEURE DES CLICS, pas l'heure de traitement).

    Kafka « clics »  ──►  micro-batch toutes les 10 s  ──►  agrégats par fenêtre de 5 min  ──►  écran

Lancer (dans spark-client). Le connecteur Kafka de Spark est téléchargé au 1er lancement,
puis gardé dans .ivy2/ (dossier du projet) :
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
      --conf spark.jars.ivy=/opt/project/.ivy2 streaming/traffic_monitor.py
Puis, dans un 2e terminal, lancer le simulateur (streaming/replay_events.py). Arrêt : Ctrl+C.
"""
import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.lib.session import get_spark

# Le contrat du message JSON écrit par replay_events.py
CLICK_SCHEMA = T.StructType([
    T.StructField("event_time", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("category_code", T.StringType()),
    T.StructField("brand", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("user_session", T.StringType()),
    T.StructField("sent_at_ms", T.LongType()),
])


def parse_clicks(raw: DataFrame) -> DataFrame:
    """Messages Kafka (value = octets JSON) -> une ligne par clic, avec de vraies colonnes typées."""
    return (raw
            .select(F.from_json(F.col("value").cast("string"), CLICK_SCHEMA).alias("c"))
            .select("c.*")
            .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
            .where(F.col("event_time").isNotNull() & F.col("user_id").isNotNull()))  # JSON illisible -> ignoré


def traffic_by_window(clicks: DataFrame, window: str, watermark: str) -> DataFrame:
    """Activité du site par fenêtre de temps (heure des clics)."""
    is_type = lambda t: (F.col("event_type") == t).cast("int")
    return (clicks
            # Latence : de l'envoi par le "site" jusqu'au micro-batch Spark qui traite le clic.
            # (calculée AVANT l'agrégation : Spark refuse current_timestamp() à l'intérieur d'un avg)
            .withColumn("latency_s", (F.unix_millis(F.current_timestamp()) - F.col("sent_at_ms")) / 1000)
            # Watermark : on accepte les clics en retard jusqu'à `watermark` derrière le plus récent vu ;
            # au-delà, la fenêtre est considérée terminée et Spark libère sa mémoire.
            .withWatermark("event_time", watermark)
            .groupBy(F.window("event_time", window).alias("w"))
            .agg(F.count("*").alias("clics"),
                 F.sum(is_type("view")).alias("vues"),
                 F.sum(is_type("cart")).alias("paniers"),
                 F.sum(is_type("purchase")).alias("achats"),
                 F.approx_count_distinct("user_id").alias("visiteurs"),
                 F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))), 0).alias("ca"),
                 F.round(F.avg("latency_s"), 1).alias("latence_s")))


def show_batch(batch: DataFrame, batch_id: int) -> None:
    """Appelé à chaque micro-batch : affiche les fenêtres mises à jour, triées par heure."""
    rows = (batch.orderBy("w")
                 .select(F.date_format("w.start", "MM-dd HH:mm").alias("fenetre"),
                         "clics", "vues", "paniers", "achats", "visiteurs", "ca", "latence_s")
                 .collect())
    if not rows:
        return
    print(f"\n=== micro-batch {batch_id} : {len(rows)} fenêtre(s) mise(s) à jour ===")
    print(f"{'fenêtre':<13}{'clics':>8}{'vues':>8}{'paniers':>9}{'achats':>8}"
          f"{'visiteurs':>11}{'CA':>10}{'latence':>9}")
    for r in rows:
        print(f"{r.fenetre:<13}{r.clics:>8}{r.vues:>8}{r.paniers:>9}{r.achats:>8}"
              f"{r.visiteurs:>11}{(r.ca or 0):>10.0f}{r.latence_s:>8.1f}s")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="clics")
    p.add_argument("--starting-offsets", default="earliest",
                   help="1er lancement seulement : earliest = tout relire, latest = seulement le nouveau")
    p.add_argument("--max-per-trigger", type=int, default=100_000, help="Max de messages par micro-batch")
    p.add_argument("--window", default="5 minutes")
    p.add_argument("--watermark", default="2 minutes")
    p.add_argument("--trigger", default="10 seconds")
    p.add_argument("--checkpoint", default="checkpoints/traffic_monitor")
    args = p.parse_args()

    spark = get_spark("traffic-monitor")
    # Peu de partitions d'état : 48 (valeur des jobs batch) serait trop pour un petit flux.
    # (Figé dans le checkpoint au 1er lancement.)
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_per_trigger)
           .load())

    traffic = traffic_by_window(parse_clicks(raw), args.window, args.watermark)

    query = (traffic.writeStream
             .queryName("traffic_monitor")
             .outputMode("update")                          # n'émettre que les fenêtres qui ont changé
             .foreachBatch(show_batch)
             .option("checkpointLocation", args.checkpoint)  # où on en est dans Kafka + l'état des fenêtres
             .trigger(processingTime=args.trigger)
             .start())
    print(f"En écoute sur '{args.topic}' ({args.bootstrap}) — micro-batch toutes les {args.trigger}. "
          f"Ctrl+C pour arrêter.")
    query.awaitTermination()


if __name__ == "__main__":
    main()