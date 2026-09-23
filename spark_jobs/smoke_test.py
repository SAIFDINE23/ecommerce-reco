"""Test de fumée du cluster Spark.

Vérifie que le cluster fonctionne de bout en bout sur les vraies données :
le driver (spark-client) envoie le travail, les executors (spark-worker)
lisent le CSV en parallèle, et le résultat revient au driver.

Usage (depuis le conteneur spark-client) :
  spark-submit spark_jobs/smoke_test.py data/raw/2019-Oct.csv
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/2019-Oct.csv"

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext.getConf()
print(f"\nMaster          : {spark.sparkContext.master}")
print(f"Mémoire driver  : {conf.get('spark.driver.memory', '1g (défaut)')}")
print(f"Mémoire executor: {conf.get('spark.executor.memory', '1g (défaut)')}")

t0 = time.time()
df = spark.read.csv(path, header=True)
print(f"Partitions lues : {df.rdd.getNumPartitions()} (= tâches exécutées en parallèle)")

counts = df.groupBy("event_type").count().orderBy(F.desc("count")).collect()
total = sum(r["count"] for r in counts)

print(f"\nFichier : {path}")
print(f"Lignes  : {total:,}".replace(",", " "))
for r in counts:
    print(f"  {str(r['event_type']):<10} {r['count']:>12,}  ({100 * r['count'] / total:.2f} %)".replace(",", " "))
print(f"Durée   : {time.time() - t0:.0f} s\n")

spark.stop()