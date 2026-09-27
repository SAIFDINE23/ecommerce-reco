"""Entraîne un modèle ALS (feedback implicite), l'évalue sur la semaine de validation
(--split val) ou de test (--split test) et enregistre tout dans MLflow.

  - utilisateurs connus du modèle      -> recommandations ALS personnalisées
  - utilisateurs inconnus (cold start) -> liste de popularité (repli)

Exemples :
  spark-submit spark_jobs/train_als.py --split val                # réglage (1-17 oct. -> 18-24 oct.)
  spark-submit spark_jobs/train_als.py --split val --rank 64 --alpha 40
  spark-submit spark_jobs/train_als.py --split test               # note finale (1-24 -> 25-31 oct.)
  spark-submit spark_jobs/train_als.py --split val --no-log-model # essai sans sauvegarder le modèle
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.spark
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from spark_jobs.lib.metrics import evaluate
from spark_jobs.lib.popularity import top_popular
from spark_jobs.lib.session import get_spark
from spark_jobs.lib.splits import SPLITS, resolve_split
from spark_jobs.lib.tracking import EXPERIMENT, log_metric_groups

INT_MAX = 2**31 - 1  # ALS de Spark exige des identifiants entiers sur 32 bits


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--split", choices=sorted(SPLITS), default="test",
                   help="val = régler les paramètres, test = note finale")
    p.add_argument("--train", default=None, help="Par défaut : celui du --split")
    p.add_argument("--truth", default=None, help="Par défaut : celui du --split")
    p.add_argument("--rank", type=int, default=32, help="Taille des vecteurs de goûts")
    p.add_argument("--reg-param", type=float, default=0.1, help="Régularisation (le frein)")
    p.add_argument("--alpha", type=float, default=20.0, help="Poids des clics face aux cases vides")
    p.add_argument("--max-iter", type=int, default=10, help="Nombre d'allers-retours")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--pop-start", default=None, help="Par défaut : celui du --split")
    p.add_argument("--pop-end", default=None, help="Par défaut : celui du --split")
    p.add_argument("--experiment", default=EXPERIMENT)
    p.add_argument("--run-name", default=None)
    p.add_argument("--log-model", action=argparse.BooleanOptionalAction, default=True,
                   help="Sauvegarder le modèle dans MLflow (--no-log-model pour un essai rapide)")
    p.add_argument("--report-dir", default="reports/eval")
    args = p.parse_args()
    resolve_split(args)

    t0 = time.time()
    spark = get_spark("train-als")
    # Dossiers partagés par tous les conteneurs (le projet est monté partout au même endroit).
    spark.sparkContext.setCheckpointDir("checkpoints/als")
    shared_tmp = str(Path("checkpoints/mlflow_tmp").resolve())

    mlflow.set_experiment(args.experiment)
    run_name = args.run_name or f"{args.split}_als_r{args.rank}_a{args.alpha:g}_l{args.reg_param:g}_i{args.max_iter}"

    with mlflow.start_run(run_name=run_name) as run:
        # 1) Les RÉGLAGES : on les note avant de commencer.
        mlflow.set_tags({"model_type": "als", "split": args.split})
        mlflow.log_params({
            "rank": args.rank, "regParam": args.reg_param, "alpha": args.alpha,
            "maxIter": args.max_iter, "k": args.k, "split": args.split,
            "implicitPrefs": True, "nonnegative": True, "seed": 42,
            "cold_start_fallback": f"popularity_{args.pop_start}_{args.pop_end}",
            "train_path": args.train, "truth_path": args.truth,
        })

        # 2) Données d'entraînement : (user_id, product_id, confidence)
        train = spark.read.parquet(args.train).select("user_id", "product_id", "confidence")
        mx = train.agg(F.max("user_id").alias("u"), F.max("product_id").alias("p")).first()
        assert mx["u"] <= INT_MAX and mx["p"] <= INT_MAX, "Identifiants trop grands pour ALS"
        train = (train.withColumn("user_id", F.col("user_id").cast("int"))
                      .withColumn("product_id", F.col("product_id").cast("int")))

        # 3) Entraînement
        als = ALS(userCol="user_id", itemCol="product_id", ratingCol="confidence",
                  implicitPrefs=True, rank=args.rank, regParam=args.reg_param,
                  alpha=args.alpha, maxIter=args.max_iter, nonnegative=True,
                  coldStartStrategy="drop", seed=42)
        t_fit = time.time()
        model = als.fit(train)
        fit_s = round(time.time() - t_fit, 1)
        print(f"Entraînement terminé en {fit_s} s")

        # 4) Recommandations ALS pour les utilisateurs du test CONNUS du modèle
        truth = spark.read.parquet(args.truth)
        known = truth.where("known").select(F.col("user_id").cast("int").alias("user_id"))
        t_rec = time.time()
        als_recs = (model.recommendForUserSubset(known, args.k)
                    .select(F.col("user_id").cast("bigint").alias("user_id"),
                            F.col("recommendations.product_id").cast("array<bigint>").alias("recs")))
        als_recs = als_recs.cache()
        n_als = als_recs.count()
        rec_s = round(time.time() - t_rec, 1)

        # 5) Repli popularité pour les utilisateurs INCONNUS du modèle
        top_ids = top_popular(spark.read.parquet(args.lake), args.pop_start, args.pop_end, args.k)
        pop_array = F.array(*[F.lit(pid).cast("bigint") for pid in top_ids])
        cold_recs = truth.where("not known").select("user_id").withColumn("recs", pop_array)
        hybrid_recs = als_recs.unionByName(cold_recs)

        catalog_size = spark.read.parquet(args.train).select("product_id").distinct().count()

        # 6) Les SCORES
        metrics = {
            "known": evaluate(als_recs, truth.where("known"), args.k, catalog_size),
            "all": evaluate(hybrid_recs, truth, args.k, catalog_size),
        }
        log_metric_groups(metrics)
        mlflow.log_metrics({"fit_s": fit_s, "recommend_s": rec_s, "catalog_size": catalog_size})

        # 7) Le MODÈLE : rangé dans MLflow (plus besoin de data/models/).
        model_uri = None
        if args.log_model:
            info = mlflow.spark.log_model(
                model, artifact_path="als_model",
                dfs_tmpdir=shared_tmp,                 # dossier temporaire visible par le worker
                pip_requirements=["pyspark==3.5.3"],   # évite une détection automatique lente
            )
            model_uri = info.model_uri

        # 8) Le RAPPORT : aussi en JSON dans reports/ (lisible sans MLflow)
        duration_s = round(time.time() - t0, 1)
        mlflow.log_metric("duration_s", duration_s)
        report = {
            "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "model": "als",
            "split": args.split,
            "mlflow_run_id": run.info.run_id,
            "model_uri": model_uri,
            "params": {"rank": args.rank, "regParam": args.reg_param, "alpha": args.alpha,
                       "maxIter": args.max_iter, "k": args.k},
            "users_with_als_recs": n_als,
            "metrics": metrics,
            "fit_s": fit_s, "recommend_s": rec_s, "duration_s": duration_s,
        }
        mlflow.log_dict(report, "report.json")

        out = Path(args.report_dir)
        out.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        (out / f"als_{args.split}_{stamp}.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
        print(f"\nRun MLflow : http://localhost:5000/#/experiments/"
              f"{run.info.experiment_id}/runs/{run.info.run_id}")

    spark.stop()


if __name__ == "__main__":
    main()