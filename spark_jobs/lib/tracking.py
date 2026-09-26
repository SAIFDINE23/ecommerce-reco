"""Petits outils pour envoyer les résultats dans MLflow avec des noms cohérents.

Les deux modèles (popularité et ALS) utilisent les mêmes noms de métriques,
ce qui permet de les comparer côte à côte dans l'interface MLflow :
    known_recall_at_10, all_recall_at_10, cold_recall_at_10, ...
"""
import mlflow

EXPERIMENT = "recommandation"  # une seule expérience pour tous les modèles du projet


def metric_name(prefix: str, name: str) -> str:
    """("known", "recall@10") -> "known_recall_at_10"  (MLflow refuse le caractère @)."""
    return f"{prefix}_{name.replace('@', '_at_')}"


def log_metric_groups(groups: dict) -> dict:
    """Envoie des métriques rangées par groupe d'utilisateurs.

    {"known": {"recall@10": 0.24, "users": 67862}}
        -> known_recall_at_10 = 0.24, known_users = 67862
    """
    flat = {metric_name(prefix, name): float(value)
            for prefix, metrics in groups.items()
            for name, value in metrics.items()}
    mlflow.log_metrics(flat)
    return flat