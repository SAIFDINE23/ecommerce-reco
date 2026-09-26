"""Entraîne un modèle ALS (feedback implicite) et l'évalue sur la semaine de test.

  - utilisateurs connus du modèle   -> recommandations ALS personnalisées
  - utilisateurs inconnus (cold start) -> liste de popularité (repli)

Exemple :
  spark-submit spark_jobs/train_als.py --rank 32 --reg-param 0.1 --alpha 20 --max-iter 10
"""
import argparse
import glob
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from spark_jobs.lib.metrics import evaluate
from spark_jobs.lib.popularity import top_popular
from spark_jobs.lib.session import get_spark

INT_MAX = 2**31 - 1  # ALS de Spark exige des identifiants entiers sur 32 bits


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--train", default="data/lake/interactions/train_filtered")
    p.add_argument("--truth", default="data/lake/eval/test_truth")
    p.add_argument("--rank", type=int, default=32, help="Taille des vecteurs de goûts")
    p.add_argument("--reg-param", type=float, default=0.1, help="Régularisation (anti par cœur)")
    p.add_argument("--alpha", type=float, default=20.0, help="Poids de la confiance")
    p.add_argument("--max-iter", type=int, default=10, help="Nombre d'alternances")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--pop-start", default="2019-10-18")
    p.add_argument("--pop-end", default="2019-10-24")
    p.add_argument("--model-dir", default="data/models/als")
    p.add_argument("--report-dir", default="reports/eval")
    args = p.parse_args()

    t0 = time.time()
    spark = get_spark("train-als")
    # Sauvegardes intermédiaires : évitent qu'un calcul itératif très long ne fasse planter Spark.
    spark.sparkContext.setCheckpointDir("checkpoints/als")

    # 1) Données d'entraînement : (user_id, product_id, confidence)
    train = spark.read.parquet(args.train).select("user_id", "product_id", "confidence")
    mx = train.agg(F.max("user_id").alias("u"), F.max("product_id").alias("p")).first()
    assert mx["u"] <= INT_MAX and mx["p"] <= INT_MAX, "Identifiants trop grands pour ALS"
    train = (train.withColumn("user_id", F.col("user_id").cast("int"))
                  .withColumn("product_id", F.col("product_id").cast("int")))

    # 2) Entraînement
    als = ALS(userCol="user_id", itemCol="product_id", ratingCol="confidence",
              implicitPrefs=True, rank=args.rank, regParam=args.reg_param,
              alpha=args.alpha, maxIter=args.max_iter, nonnegative=True,
              coldStartStrategy="drop", seed=42)
    t_fit = time.time()
    model = als.fit(train)
    fit_s = round(time.time() - t_fit, 1)
    print(f"Entraînement terminé en {fit_s} s")

    # 3) Recommandations ALS pour les utilisateurs du test CONNUS du modèle
    truth = spark.read.parquet(args.truth)
    known = truth.where("known").select(F.col("user_id").cast("int").alias("user_id"))
    t_rec = time.time()
    als_recs = (model.recommendForUserSubset(known, args.k)
                .select(F.col("user_id").cast("bigint").alias("user_id"),
                        F.col("recommendations.product_id").cast("array<bigint>").alias("recs")))
    als_recs = als_recs.cache()
    n_als = als_recs.count()
    rec_s = round(time.time() - t_rec, 1)

    # 4) Repli popularité pour les utilisateurs INCONNUS du modèle
    top_ids = top_popular(spark.read.parquet(args.lake), args.pop_start, args.pop_end, args.k)
    pop_array = F.array(*[F.lit(pid).cast("bigint") for pid in top_ids])
    cold_recs = truth.where("not known").select("user_id").withColumn("recs", pop_array)
    hybrid_recs = als_recs.unionByName(cold_recs)

    catalog_size = spark.read.parquet(args.train).select("product_id").distinct().count()

    # 5) Évaluation
    metrics = {
        "als_on_known_users": evaluate(als_recs, truth.where("known"), args.k, catalog_size),
        "hybrid_all_users": evaluate(hybrid_recs, truth, args.k, catalog_size),
    }

    # 6) Sauvegarde du modèle (les vecteurs appris) et du rapport
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_path = f"{args.model_dir}/{stamp}"
    model.write().overwrite().save(model_path)

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": "als",
        "params": {"rank": args.rank, "regParam": args.reg_param, "alpha": args.alpha,
                   "maxIter": args.max_iter, "k": args.k},
        "model_path": model_path,
        "users_with_als_recs": n_als,
        "metrics": metrics,
        "fit_s": fit_s,
        "recommend_s": rec_s,
        "duration_s": round(time.time() - t0, 1),
    }

    # Comparaison avec le dernier rapport de la baseline de popularité, s'il existe.
    baselines = sorted(glob.glob(f"{args.report_dir}/baseline_popularity_*.json"))
    if baselines:
        base = json.loads(Path(baselines[-1]).read_text())["metrics"]
        k = args.k
        report["vs_popularity"] = {
            f"known_users_recall@{k}": [base["known_users"][f"recall@{k}"],
                                        metrics["als_on_known_users"][f"recall@{k}"]],
            f"known_users_ndcg@{k}": [base["known_users"][f"ndcg@{k}"],
                                      metrics["als_on_known_users"][f"ndcg@{k}"]],
            f"all_users_recall@{k}": [base["all_users"][f"recall@{k}"],
                                      metrics["hybrid_all_users"][f"recall@{k}"]],
            "coverage": [base["all_users"]["coverage"], metrics["hybrid_all_users"]["coverage"]],
        }

    out = Path(args.report_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"als_{stamp}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    print("\nExemple : recommandations ALS pour 3 utilisateurs, et ce qu'ils ont vraiment fait")
    (als_recs.join(truth.select("user_id", "truth"), "user_id")
             .orderBy("user_id").show(3, truncate=110))
    spark.stop()


if __name__ == "__main__":
    main()