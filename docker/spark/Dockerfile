# Image unique pour tout le cluster Spark (master, worker, client).
# Pourquoi une seule image ? Le driver et les executors doivent avoir EXACTEMENT
# la même version de Python et de PySpark, sinon les jobs plantent.
FROM python:3.11-slim-bookworm

# Java 17 (Spark tourne dans la JVM), procps (outils système utilisés par Spark),
# tini (gère proprement l'arrêt des processus dans un conteneur).
# Le lien /opt/java donne un JAVA_HOME valable sur PC (amd64) comme sur Mac (arm64).
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/project \
    PYSPARK_PYTHON=python3 \
    SPARK_CONF_DIR=/opt/project/conf/spark

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/project
ENTRYPOINT ["/usr/bin/tini", "--"]