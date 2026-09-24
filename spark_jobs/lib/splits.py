"""Découpage temporel entraînement / test et filtrage des données d'entraînement."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Ce qu'on cherche à prédire : un ajout au panier ou un achat (pas une simple vue).
TARGET_EVENTS = ("cart", "purchase")


def filter_interactions(inter: DataFrame, min_user_events: int = 5,
                        min_item_events: int = 10) -> DataFrame:
    """Retire les utilisateurs et produits trop rares pour que le modèle apprenne quelque chose.

    min_user_events : un utilisateur doit avoir au moins N événements sur la période ;
    min_item_events : un produit doit avoir au moins N événements sur la période.
    """
    total = F.col("n_views") + F.col("n_carts") + F.col("n_purchases")
    users = (inter.groupBy("user_id").agg(F.sum(total).alias("u_events"))
             .where(F.col("u_events") >= min_user_events).select("user_id"))
    items = (inter.groupBy("product_id").agg(F.sum(total).alias("i_events"))
             .where(F.col("i_events") >= min_item_events).select("product_id"))
    # « left_semi » = garder les lignes qui ont une correspondance, sans ajouter de colonnes.
    return (inter.join(users, "user_id", "left_semi")
                 .join(items, "product_id", "left_semi"))


def build_ground_truth(events: DataFrame) -> DataFrame:
    """La « vérité » du test : pour chaque utilisateur, les produits réellement
    mis au panier ou achetés pendant la période de test.

    Résultat : une ligne par utilisateur -> (user_id, truth: liste de product_id, n_truth)
    """
    return (events
            .where(F.col("event_type").isin(*TARGET_EVENTS))
            .groupBy("user_id")
            .agg(F.array_sort(F.collect_set("product_id")).alias("truth"))
            .withColumn("n_truth", F.size("truth")))