"""Simulateur de site e-commerce : rejoue les clics du data lake dans Kafka,
dans l'ordre chronologique et en temps accéléré, comme s'ils arrivaient en direct.

    data lake Parquet (novembre)  ──►  topic Kafka « clics »
                                       clé    = user_id  (tous les clics d'une personne restent dans l'ordre)
                                       valeur = le clic en JSON

Exemples (dans le conteneur spark-client) :
  python streaming/replay_events.py --start "2019-11-15 00:00:00" --hours 2 --speed 60
  python streaming/replay_events.py --start "2019-11-15 00:00:00" --hours 1 --speed 0     # le plus vite possible
  python streaming/replay_events.py --start "2019-11-15 00:00:00" --hours 1 --dry-run     # sans Kafka, pour voir
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

COLUMNS = ["event_time", "event_type", "product_id", "category_code", "brand",
           "price", "user_id", "user_session"]


def load_hour(dataset, start: datetime, sample_pct: int) -> pa.Table:
    """Les clics d'UNE heure, triés par heure du clic (le lake est trié par utilisateur, pas par heure)."""
    end = start + timedelta(hours=1)
    days = sorted({start.strftime("%Y-%m-%d"), (end - timedelta(microseconds=1)).strftime("%Y-%m-%d")})
    flt = (ds.field("event_date").isin(days)                 # élagage des dossiers (partitions)
           & (ds.field("event_time") >= pa.scalar(start, pa.timestamp("us")))
           & (ds.field("event_time") < pa.scalar(end, pa.timestamp("us"))))
    table = dataset.to_table(columns=COLUMNS, filter=flt)
    if sample_pct < 100:                                      # garder ~sample_pct % des utilisateurs
        keep = pc.less(pc.subtract(table["user_id"],
                                   pc.multiply(pc.divide(table["user_id"], 100), 100)), sample_pct)
        table = table.filter(keep)
    return table.sort_by([("event_time", "ascending")])


def to_message(row: dict) -> tuple[bytes, bytes]:
    """Un clic -> (clé, valeur) prêts pour Kafka."""
    value = {
        "event_time": row["event_time"].strftime("%Y-%m-%d %H:%M:%S"),   # heure du clic (UTC)
        "event_type": row["event_type"],
        "product_id": row["product_id"],
        "category_code": row["category_code"],
        "brand": row["brand"],
        "price": row["price"],
        "user_id": row["user_id"],
        "user_session": row["user_session"],
        "sent_at_ms": int(time.time() * 1000),   # heure d'envoi réelle : servira à mesurer la latence
    }
    return str(row["user_id"]).encode(), json.dumps(value, separators=(",", ":")).encode()


def make_producer(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "replay-simulator",
        "acks": "all",                 # le broker confirme l'écriture
        "enable.idempotence": True,    # pas de doublon si un envoi est réessayé
        "linger.ms": 20,               # on regroupe les messages par paquets de 20 ms (plus efficace)
        "compression.type": "lz4",
    })


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--topic", default="clics")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--start", required=True, help='Début du rejeu, ex. "2019-11-15 00:00:00" (UTC)')
    p.add_argument("--hours", type=float, default=1.0, help="Durée rejouée (heures de novembre)")
    p.add_argument("--speed", type=float, default=60.0,
                   help="Accélération : 60 = 1 h de novembre en 1 min. 0 = le plus vite possible")
    p.add_argument("--sample-pct", type=int, default=100, help="% d'utilisateurs rejoués (alléger)")
    p.add_argument("--dry-run", action="store_true", help="Ne rien envoyer : afficher quelques messages")
    p.add_argument("--log-every", type=float, default=10.0, help="Secondes entre deux lignes de suivi")
    args = p.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
    end = start + timedelta(hours=args.hours)
    dataset = ds.dataset(args.lake, format="parquet", partitioning="hive")
    producer = None if args.dry_run else make_producer(args.bootstrap)

    errors = []
    def on_delivery(err, msg):                 # appelé par Kafka pour chaque message confirmé (ou raté)
        if err is not None:
            errors.append(err)

    print(f"Rejeu {start} -> {end} | vitesse x{args.speed:g} | topic '{args.topic}' | "
          f"{'DRY-RUN' if args.dry_run else args.bootstrap}")
    wall0 = time.monotonic()                   # l'horloge réelle au départ
    sent, shown, last_log, last_sent = 0, 0, wall0, 0

    hour = start
    while hour < end:
        table = load_hour(dataset, hour, args.sample_pct)
        for row in table.to_pylist():
            if row["event_time"] >= end:
                break
            # Horloge simulée : le clic doit partir quand (temps réel écoulé) x vitesse = (temps novembre écoulé).
            if args.speed > 0:
                target = wall0 + (row["event_time"] - start).total_seconds() / args.speed
                delay = target - time.monotonic()
                if delay > 0.005:
                    time.sleep(delay)

            key, value = to_message(row)
            if producer is None:
                if shown < 5:
                    print(f"  clé={key.decode()}  valeur={value.decode()}")
                    shown += 1
            else:
                while True:
                    try:
                        producer.produce(args.topic, key=key, value=value, on_delivery=on_delivery)
                        break
                    except BufferError:        # file d'attente locale pleine : on laisse Kafka respirer
                        producer.poll(0.5)
                producer.poll(0)               # traite les confirmations reçues
            sent += 1

            now = time.monotonic()
            if now - last_log >= args.log_every:
                rate = (sent - last_sent) / (now - last_log)
                print(f"  heure simulée {row['event_time']} | envoyés {sent:,} | {rate:,.0f} msg/s"
                      .replace(",", " "))
                last_log, last_sent = now, sent
        hour += timedelta(hours=1)

    if producer is not None:
        left = producer.flush(30)              # attendre que tout soit confirmé
        if left:
            print(f"ATTENTION : {left} messages non confirmés")
    elapsed = time.monotonic() - wall0
    print(f"Terminé : {sent:,} clics en {elapsed:,.1f} s ({sent / max(elapsed, 1e-9):,.0f} msg/s), "
          f"{len(errors)} erreurs".replace(",", " "))


if __name__ == "__main__":
    main()