# Image unique pour tout le cluster Spark (master, worker, client).
# Pourquoi une seule image ? Le driver et les executors doivent avoir EXACTEMENT
# la même version de Python et de PySpark, sinon les jobs plantent.
FROM python:3.11-slim-bookworm

# Java 17 (Spark tourne dans la JVM), procps (outils système utilisés par Spark),
# tini (gère proprement l'arrêt des processus dans un conteneur).
# Le lien /opt/java donne un JAVA_HOME valable sur PC (amd64) comme sur Mac (arm64).
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini git \
 && rm -rf /var/lib/apt/lists/* \
 # Le projet monté appartient à votre utilisateur WSL, pas à root : on dit à git de lui faire confiance
 # (sinon « dubious ownership »). Sert à MLflow pour noter le commit du code de chaque run.
 && git config --system --add safe.directory /opt/project \
 # JAVA_HOME indépendant de l'architecture (amd64 sur PC, arm64 sur Mac M1/M2)
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/project \
    PYSPARK_PYTHON=python3 \
    SPARK_CONF_DIR=/opt/project/conf/spark

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --timeout 60 --retries 10 -r /tmp/requirements.txt
# Librairies ajoutées en cours de projet, dans une COUCHE SÉPARÉE : les modifier ne refait
# que cette petite étape, sans re-télécharger pyspark (317 Mo) de la couche du dessus.
COPY docker/spark/requirements-extra.txt /tmp/requirements-extra.txt
RUN pip install --no-cache-dir --timeout 60 --retries 10 -r /tmp/requirements-extra.txt

WORKDIR /opt/project
ENTRYPOINT ["/usr/bin/tini", "--"]