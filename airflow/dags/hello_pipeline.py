"""Semaine 5.1 — premier DAG : vérifier qu'Airflow sait parler à tous les services du projet.

    check_services  ──►  spark_smoke_test
    (Redis, MLflow,      (un vrai job Spark lancé dans spark-client,
     spark-client)        exécuté par le cluster)
"""
import json
import urllib.request

import docker
import pendulum
import redis
from airflow.sdk import dag, task

from reco.docker_exec import run_in_container


@dag(
    dag_id="hello_pipeline",
    schedule=None,                                   # lancé à la main (bouton ▶ dans l'interface)
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["semaine-5", "test"],
    doc_md=__doc__,
)
def hello_pipeline():

    @task
    def check_services() -> dict:
        """Chaque service répond-il ? (échoue tout de suite si l'un d'eux est éteint)"""
        status = {}
        status["redis"] = redis.Redis.from_url("redis://redis:6379/0").ping()
        with urllib.request.urlopen("http://mlflow:5000/health", timeout=10) as resp:
            status["mlflow"] = resp.status == 200
        status["spark-client"] = docker.from_env().containers.get("spark-client").status
        print(json.dumps(status, indent=2))
        assert status["redis"] and status["mlflow"] and status["spark-client"] == "running"
        return status

    @task
    def spark_smoke_test() -> None:
        """Un vrai job Spark, lancé par Airflow mais exécuté par le cluster."""
        run_in_container("spark-client", ["spark-submit", "spark_jobs/smoke_test.py", "data/raw/2019-Oct.csv"])

    check_services() >> spark_smoke_test()


hello_pipeline()