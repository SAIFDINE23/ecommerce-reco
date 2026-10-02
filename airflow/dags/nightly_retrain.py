"""Le pipeline de la nuit : chaque nuit à 2 h, on traite la journée de la veille.

Version 5.2 (les fondations) :

    quel_jour ──► controle_qualite ──► popularite_redis
                  (le jour D est-il    (top 100 global + top 50 par
                   normal ? sinon       catégorie sur 7 jours -> Redis)
                   STOP, rien publié)

Version 5.3 (à venir) : challenger ALS, comparaison avec le champion, promotion, export des vecteurs.

« Quel jour traiter ? » — la DATE LOGIQUE
  Airflow donne à chaque exécution une date logique = l'heure à laquelle elle était PRÉVUE.
  L'exécution prévue le 16/11/2019 à 02:00 traite donc la journée du 15/11 (la veille).
  C'est ce qui permet de REJOUER le passé : Airflow crée les exécutions du 14 au 18 novembre
  2019 comme si le système avait tourné à l'époque (commande « backfill »).
  Pour un test à la main, on peut aussi forcer le jour avec le paramètre « day ».
"""
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, task

from reco.docker_exec import run_in_container

SPARK = "spark-client"


@dag(
    dag_id="nightly_retrain",
    schedule="0 2 * * *",                                   # tous les jours à 02:00 (UTC)
    # Les données couvrent novembre 2019 : le DAG n'existe « que » sur cette période.
    start_date=pendulum.datetime(2019, 11, 2, tz="UTC"),
    end_date=pendulum.datetime(2019, 12, 1, tz="UTC"),
    catchup=False,             # ne PAS lancer tout seul les 30 nuits passées : on choisit avec backfill
    max_active_runs=1,         # une nuit à la fois, dans l'ordre (chaque nuit dépend du champion de la veille)
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"day": Param(None, type=["null", "string"],
                         description="Forcer le jour traité (YYYY-MM-DD). Vide = la veille de la date logique.")},
    tags=["semaine-5", "production"],
    doc_md=__doc__,
)
def nightly_retrain():

    @task
    def quel_jour(**context) -> str:
        """Le jour D traité par cette exécution (renvoyé aux tâches suivantes via XCom)."""
        if context["params"].get("day"):
            day = context["params"]["day"]
        elif context.get("logical_date"):
            day = (context["logical_date"] - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            raise ValueError("Pas de date logique : renseigner le paramètre 'day' (ex. 2019-11-14).")
        print(f"Cette exécution traite la journée du {day}")
        return day

    @task(retries=0)   # une donnée cassée le restera : inutile de réessayer
    def controle_qualite(day: str) -> None:
        run_in_container(SPARK, ["spark-submit", "spark_jobs/check_day.py", "--date", day])

    @task
    def popularite_redis(day: str) -> None:
        run_in_container(SPARK, ["spark-submit", "spark_jobs/export_popularity.py", "--date", day])

    day = quel_jour()
    controle_qualite(day) >> popularite_redis(day)


nightly_retrain()