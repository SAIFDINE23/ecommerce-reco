"""Création de la SparkSession, commune à tous les jobs du projet."""
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Crée (ou récupère) la session Spark.

    On ne fixe PAS le master ici : dans Docker il vient de la variable MASTER
    (spark://spark-master:7077), et sans elle Spark tourne en mode local.
    Le même code marche donc sur le cluster, en local et dans les tests.
    """
    spark = (SparkSession.builder
             .appName(app_name)
             # Valeurs sûres même si spark-defaults.conf n'est pas chargé (tests, hors Docker).
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark