"""Lecture des vecteurs ALS du champion dans Redis, et calcul des candidats À LA DEMANDE.

C'est exactement le calcul du cours sur ALS : score(personne, produit) = x_personne · y_produit.
  - les vecteurs y des ~100 000 produits sont chargés UNE fois en mémoire (une matrice numpy) ;
  - le vecteur x d'une personne est lu dans Redis (256 octets) ;
  - un produit matrice x vecteur donne ses scores pour TOUS les produits, en quelques millisecondes.
L'API (semaine 6) utilisera ces fonctions.

Exemple (dans le conteneur spark-client) :
  python -m serving.als_vectors --user 512475445 --k 10
"""
import argparse
import os
import time

import numpy as np
import redis

META_KEY = "als:meta"


def connect(url: str | None = None) -> redis.Redis:
    # decode_responses=False : les vecteurs sont stockés en OCTETS (float32), pas en texte
    return redis.Redis.from_url(url or os.environ.get("REDIS_URL", "redis://redis:6379/0"))


def user_key(version: int | str, user_id: int | str) -> str:
    return f"als:v{version}:user:{user_id}"


def load_items(r: redis.Redis) -> tuple[dict, np.ndarray, np.ndarray]:
    """Charge la version courante : (méta, ids des produits, matrice des vecteurs produits)."""
    meta = {k.decode(): v.decode() for k, v in r.hgetall(META_KEY).items()}
    if not meta:
        raise RuntimeError("Aucun modèle ALS dans Redis : lancer spark_jobs/export_als_to_redis.py")
    v, rank = meta["version"], int(meta["rank"])
    ids = np.frombuffer(r.get(f"als:v{v}:item_ids"), dtype=np.int64)
    factors = np.frombuffer(r.get(f"als:v{v}:item_factors"), dtype=np.float32).reshape(-1, rank)
    return meta, ids, factors


def top_k(r: redis.Redis, meta: dict, item_ids: np.ndarray, item_factors: np.ndarray,
          user_id: int | str, k: int = 100) -> list[tuple[int, float]] | None:
    """Les k meilleurs produits d'une personne (None si elle est inconnue du modèle : cold start)."""
    raw = r.get(user_key(meta["version"], user_id))
    if raw is None:
        return None
    x = np.frombuffer(raw, dtype=np.float32)
    scores = item_factors @ x                        # un score par produit : x · y
    best = np.argpartition(-scores, k)[:k]           # les k plus grands, sans tout trier
    best = best[np.argsort(-scores[best])]           # puis on trie seulement ces k-là
    return [(int(item_ids[i]), float(scores[i])) for i in best]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True)
    p.add_argument("--k", type=int, default=10)
    args = p.parse_args()

    r = connect()
    t0 = time.perf_counter()
    meta, ids, factors = load_items(r)
    t1 = time.perf_counter()
    recs = top_k(r, meta, ids, factors, args.user, args.k)
    t2 = time.perf_counter()

    n = lambda v: f"{int(v):,}".replace(",", " ")        # 105 000 plutôt que 105,000
    print(f"Modèle : {meta['model_name']} v{meta['version']} (rank {meta['rank']}, "
          f"{n(meta['n_items'])} produits, {n(meta['n_users'])} personnes)")
    print(f"Chargement des produits : {(t1 - t0) * 1000:.0f} ms (une seule fois au démarrage de l'API)")
    if recs is None:
        print(f"Personne {args.user} inconnue du modèle (cold start) -> repli popularité / session.")
        return
    print(f"Top {args.k} de la personne {args.user}, calculé en {(t2 - t1) * 1000:.1f} ms :")
    for rank_, (pid, score) in enumerate(recs, 1):
        print(f"  {rank_:>3}. produit {pid:<10} score {score:.3f}")


if __name__ == "__main__":
    main()