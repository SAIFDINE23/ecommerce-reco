"""Transformations pures (DataFrame -> DataFrame), testées unitairement.

Aucune UDF Python : tout passe par l'API DataFrame, exécutée dans la JVM,
donc beaucoup plus rapide sur des centaines de millions de lignes.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.lib.schema import EVENT_KEY, EVENT_TIME_FORMAT, VALID_EVENT_TYPES


def parse_events(raw: DataFrame) -> DataFrame:
    """Types propres + colonnes dérivées utiles pour la suite du projet."""
    return (raw
        .withColumn("event_time", F.to_timestamp("event_time", EVENT_TIME_FORMAT))
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("brand", F.lower(F.trim("brand")))
        .withColumn("category_code", F.lower(F.trim("category_code")))
        # "electronics.smartphone" -> "electronics" : niveau de catégorie exploitable même
        # quand le code est incomplet ; "unknown" quand il est absent.
        .withColumn("category_l1",
                    F.coalesce(F.split("category_code", r"\.").getItem(0), F.lit("unknown")))
        .withColumn("event_date", F.to_date("event_time")))


def add_reject_reason(df: DataFrame) -> DataFrame:
    """Ajoute `reject_reason` (null si la ligne est valide).

    Une seule raison par ligne, la première règle qui échoue, pour pouvoir
    compter les rejets en une seule agrégation.
    """
    reason = (F.when(F.col("event_time").isNull(), "invalid_event_time")
               .when(F.col("user_id").isNull(), "missing_user_id")
               .when(F.col("product_id").isNull(), "missing_product_id")
               .when(F.col("user_session").isNull(), "missing_session")
               .when(~F.col("event_type").isin(*VALID_EVENT_TYPES)
                     | F.col("event_type").isNull(), "invalid_event_type")
               .when(F.col("price").isNull() | (F.col("price") < 0), "invalid_price"))
    return df.withColumn("reject_reason", reason)


def filter_dates(df: DataFrame, start_date: str | None, end_date: str | None) -> DataFrame:
    """Ne garde que [start_date, end_date] (inclus). Sert au traitement incrémental."""
    if start_date:
        df = df.where(F.col("event_date") >= F.lit(start_date).cast("date"))
    if end_date:
        df = df.where(F.col("event_date") <= F.lit(end_date).cast("date"))
    return df


def sample_users(df: DataFrame, pct: int) -> DataFrame:
    """Échantillonne des UTILISATEURS (pas des lignes) de façon déterministe.

    Échantillonner des lignes au hasard casserait les historiques et les sessions ;
    ici on garde 100 % des événements de pct % des utilisateurs, et le même
    utilisateur est toujours sélectionné d'une exécution à l'autre.
    """
    if pct >= 100:
        return df
    return df.where(F.pmod(F.xxhash64("user_id"), F.lit(100)) < pct)


def deduplicate(df: DataFrame) -> DataFrame:
    """Le dataset contient des événements strictement dupliqués (double envoi du tracker)."""
    return df.dropDuplicates(EVENT_KEY)


# Colonnes finales écrites dans le data lake, dans cet ordre.
LAKE_COLUMNS = ["event_time", "event_type", "product_id", "category_id", "category_code",
                "category_l1", "brand", "price", "user_id", "user_session", "event_date"]