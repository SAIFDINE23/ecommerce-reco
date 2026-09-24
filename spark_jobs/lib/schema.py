"""Schéma des CSV bruts REES46.

On impose le schéma au lieu d'utiliser `inferSchema=True` :
  * inferSchema relit tout le fichier une première fois (~14 Go) juste pour deviner les types ;
  * un type deviné peut changer d'un mois à l'autre et casser le pipeline en silence.
"""
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

RAW_EVENTS_SCHEMA = StructType([
    StructField("event_time", StringType()),     # "2019-10-01 00:00:00 UTC" -> converti en date à l'étape 4.2
    StructField("event_type", StringType()),     # view | cart | purchase
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),  # ex. "electronics.smartphone" (souvent vide)
    StructField("brand", StringType()),          # souvent vide
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])

# Format du texte de event_time, pour le convertir en vrai horodatage.
EVENT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss 'UTC'"

# Les seuls types d'événements valides.
VALID_EVENT_TYPES = ("view", "cart", "purchase")

# Colonnes qui identifient un événement unique (sert à supprimer les doublons).
EVENT_KEY = ["event_time", "event_type", "product_id", "user_id", "user_session"]