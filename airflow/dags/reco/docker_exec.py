"""Faire exécuter une commande par un conteneur du projet (comme « docker compose exec »).

Airflow ne fait PAS le calcul lui-même : il donne des ordres. Le travail tourne dans le conteneur
spark-client, exactement comme quand on tape à la main :
    docker compose exec spark-client spark-submit spark_jobs/train_als.py
Pour cela, le conteneur Airflow a accès au « socket » Docker (voir docker-compose.yml).
"""
import docker


def run_in_container(container: str, cmd: list[str], workdir: str = "/opt/project") -> None:
    """Lance `cmd` dans `container`, recopie sa sortie dans le journal Airflow, échoue si la commande échoue."""
    api = docker.from_env().api
    print(f"$ docker exec {container} {' '.join(cmd)}", flush=True)
    exec_id = api.exec_create(container, cmd, workdir=workdir)["Id"]

    buffer = ""
    for chunk in api.exec_start(exec_id, stream=True):        # la sortie arrive au fil de l'eau
        buffer += chunk.decode(errors="replace")
        *lines, buffer = buffer.split("\n")
        for line in lines:
            print(line, flush=True)
    if buffer:
        print(buffer, flush=True)

    code = api.exec_inspect(exec_id)["ExitCode"]
    if code != 0:
        raise RuntimeError(f"La commande a échoué (code {code}) : {' '.join(cmd)}")