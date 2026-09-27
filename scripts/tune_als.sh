#!/usr/bin/env bash
# Recherche de réglages ALS sur la semaine de VALIDATION (jamais sur le test !).
# Méthode « un bouton à la fois » : on part du réglage de référence (rank 32, alpha 20, regParam 0.1)
# et on ne change qu'UN paramètre par run, pour voir l'effet de chacun.
#
# À lancer DANS le conteneur spark-client (chaque run apparaît dans MLflow) :
#   docker compose exec spark-client bash scripts/tune_als.sh
# Durée : ~8 min par run sur votre machine.
set -u

#        rank  alpha  regParam
CONFIGS=(
  "32     5     0.1"
  "32    10     0.1"
  "32    40     0.1"
  "32    80     0.1"
  "32    20     0.01"
  "32    20     1.0"
  "16    20     0.1"
  "64    20     0.1"
)

mkdir -p reports/tuning
LOG="reports/tuning/tune_als_$(date -u +%Y%m%dT%H%M%SZ).log"
echo "Journal complet : $LOG"

i=0
for cfg in "${CONFIGS[@]}"; do
  i=$((i + 1))
  read -r RANK ALPHA REG <<< "$cfg"
  echo "=== [$i/${#CONFIGS[@]}] rank=$RANK alpha=$ALPHA regParam=$REG  ($(date +%H:%M)) ===" | tee -a "$LOG"
  # --no-log-model : on compare des scores, inutile de stocker 8 modèles de ~150 Mo.
  if spark-submit spark_jobs/train_als.py --split val --no-log-model \
       --rank "$RANK" --alpha "$ALPHA" --reg-param "$REG" >> "$LOG" 2>&1; then
    echo "    OK" | tee -a "$LOG"
  else
    # Un run qui échoue (ex. mémoire) n'arrête pas les suivants ; il apparaît en FAILED dans MLflow.
    echo "    ÉCHEC (voir $LOG)" | tee -a "$LOG"
  fi
done

echo "Terminé. Classement : python analysis/compare_runs.py --split val"