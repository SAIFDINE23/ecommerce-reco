"""Popularité récente : les produits les plus mis au panier / achetés sur une fenêtre de dates."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.lib.splits import TARGET_EVENTS
from spark_jobs.lib.transforms import filter_dates


def top_popular(events: DataFrame, start_date: str, end_date: str, k: int) -> list[int]:
    """Renvoie les k product_id les plus mis au panier / achetés entre start_date et end_date."""
    rows = (filter_dates(events, start_date, end_date)
            .where(F.col("event_type").isin(*TARGET_EVENTS))
            .groupBy("product_id").count()
            .orderBy(F.desc("count"), "product_id")
            .limit(k).collect())
    return [r["product_id"] for r in rows]