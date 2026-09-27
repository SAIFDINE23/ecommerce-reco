"""Découpage temporel entraînement / test et filtrage des données d'entraînement."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Ce qu'on cherche à prédire : un ajout au panier ou un achat (pas une simple vue).
TARGET_EVENTS = ("cart", "purchase")

# Les deux découpages temporels du projet (même recette, décalée d'une semaine) :
#   val  : pour RÉGLER les paramètres  -> entraînement 1-17 oct., validation 18-24 oct.
#   test : pour la NOTE FINALE (1 fois) -> entraînement 1-24 oct., test       25-31 oct.
# pop_start / pop_end : la semaine de popularité juste avant la période évaluée.
SPLITS = {
    "val": {"train": "data/lake/interactions/val_train_filtered",
            "truth": "data/lake/eval/val_truth",
            "pop_start": "2019-10-11", "pop_end": "2019-10-17"},
    "test": {"train": "data/lake/interactions/train_filtered",
             "truth": "data/lake/eval/test_truth",
             "pop_start": "2019-10-18", "pop_end": "2019-10-24"},
}


def resolve_split(args) -> None:
    """Complète args.train / args.truth / args.pop_start / args.pop_end avec le preset de args.split
    (sauf si on les a donnés explicitement en ligne de commande)."""
    for key, value in SPLITS[args.split].items():
        if getattr(args, key) is None:
            setattr(args, key, value)


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