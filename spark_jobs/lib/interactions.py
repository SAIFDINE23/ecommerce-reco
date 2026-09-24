"""Construction de la table d'interactions (couche gold) : une ligne par couple (utilisateur, produit).

C'est l'entrée du modèle ALS : pour chaque couple, un score qui résume
« à quel point cet utilisateur s'est intéressé à ce produit ».
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Poids de chaque type d'événement. Un achat est un signal bien plus fort qu'une vue.
# Ces valeurs sont un point de départ : on en testera d'autres avec MLflow (semaine 3).
EVENT_WEIGHTS = {"view": 1.0, "cart": 3.0, "purchase": 5.0}


def build_interactions(events: DataFrame, weights: dict = EVENT_WEIGHTS) -> DataFrame:
    """Agrège les événements bruts en une ligne par couple (user_id, product_id).

    Colonnes produites :
      n_views, n_carts, n_purchases : nombre d'événements de chaque type
      score       : somme pondérée des événements
      confidence  : log(1 + score), pour qu'un utilisateur qui clique 200 fois
                    sur un produit ne pèse pas 200 fois plus qu'un achat unique
      first_seen, last_seen : première et dernière interaction avec ce produit
    """
    is_type = lambda t: F.when(F.col("event_type") == t, 1).otherwise(0)  # noqa: E731

    agg = (events
           .groupBy("user_id", "product_id")
           .agg(F.sum(is_type("view")).alias("n_views"),
                F.sum(is_type("cart")).alias("n_carts"),
                F.sum(is_type("purchase")).alias("n_purchases"),
                F.min("event_time").alias("first_seen"),
                F.max("event_time").alias("last_seen")))

    score = (F.col("n_views") * weights["view"]
             + F.col("n_carts") * weights["cart"]
             + F.col("n_purchases") * weights["purchase"])

    return (agg
            .withColumn("score", score.cast("double"))
            .withColumn("confidence", F.log1p("score")))