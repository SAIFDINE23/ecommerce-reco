"""Lire les fiches de session dans Redis (ce que l'API lira en semaine 6).

Exemples (dans le conteneur spark-client) :
  python streaming/show_session.py --top 5            # les 5 sessions les plus actives
  python streaming/show_session.py --user 512345678   # la fiche complète d'un visiteur
  python streaming/show_session.py --stats            # taille de Redis
"""
import argparse
import os
import time

import redis


def show_user(r: redis.Redis, uid: str) -> None:
    t0 = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    pipe.hgetall(f"user:{uid}:session")
    pipe.lrange(f"user:{uid}:recent", 0, 9)
    pipe.zrevrange(f"user:{uid}:cats", 0, 2, withscores=True)
    pipe.ttl(f"user:{uid}:session")
    session, recent, cats, ttl = pipe.execute()
    read_ms = (time.perf_counter() - t0) * 1000
    if not session:
        print(f"Aucune fiche pour le visiteur {uid} (inconnu ou expiré).")
        return
    print(f"Visiteur {uid}   (lu en {read_ms:.1f} ms, expire dans {ttl} s)")
    print(f"  session        : {session.get('session_id')}")
    print(f"  dernier clic   : {session.get('last_event_time')}  ({session.get('last_category') or '-'})")
    print(f"  vues / paniers / achats : {session.get('views', 0)} / {session.get('carts', 0)} / "
          f"{session.get('purchases', 0)}")
    print(f"  catégories     : " + ", ".join(f"{c} ({int(n)})" for c, n in cats))
    print(f"  derniers produits (du plus récent au plus ancien) : {', '.join(recent)}")


def show_top(r: redis.Redis, n: int) -> None:
    """Parcourt les fiches (SCAN, sans bloquer Redis) et affiche les plus actives."""
    rows = []
    for key in r.scan_iter(match="user:*:session", count=5000):
        rows.append(key)
    pipe = r.pipeline(transaction=False)
    for key in rows:
        pipe.hmget(key, "views", "carts", "purchases", "last_category", "last_event_time")
    scored = []
    for key, vals in zip(rows, pipe.execute()):
        views, carts, purchases = (int(v or 0) for v in vals[:3])
        scored.append((views + 3 * carts + 5 * purchases, key.split(":")[1], views, carts, purchases,
                       vals[3], vals[4]))
    scored.sort(reverse=True)
    print(f"{len(rows):,} sessions actives dans Redis. Les {n} plus actives :".replace(",", " "))
    for s, uid, v, c, pu, cat, last in scored[:n]:
        print(f"  {uid:>10}  vues {v:>4}  paniers {c:>3}  achats {pu:>3}  dernier clic {last}  ({cat or '-'})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    p.add_argument("--user", help="user_id à afficher")
    p.add_argument("--top", type=int, help="Afficher les N sessions les plus actives")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    if args.user:
        show_user(r, args.user)
    if args.top:
        show_top(r, args.top)
    if args.stats or not (args.user or args.top):
        info = r.info("memory")
        print(f"Clés : {r.dbsize():,} | mémoire utilisée : {info['used_memory_human']} | "
              f"dernier micro-batch écrit : {r.get('stream:session_features:last_batch_id')}".replace(",", " "))


if __name__ == "__main__":
    main()