#!/usr/bin/env bash
# Télécharge le dataset REES46 (Kaggle) dans data/raw/.
# Prérequis : clé API Kaggle dans ~/.kaggle/kaggle.json
# Usage     : bash scripts/download_data.sh
#
# On utilise curl plutôt que l'outil `kaggle` : curl écrit les données sur le disque
# au fil de l'eau (streaming), alors que l'outil kaggle 1.7 charge tout le fichier
# en mémoire (4,3 Go) et se fait tuer par Linux faute de RAM.

set -euo pipefail

DATASET="mkechinov/ecommerce-behavior-data-from-multi-category-store"
DEST="data/raw"
ZIP="$DEST/dataset.zip"
CREDS="$HOME/.kaggle/kaggle.json"

mkdir -p "$DEST"

# Idempotent : si les fichiers sont déjà là, on ne retélécharge pas 4 Go.
if [[ -f "$DEST/2019-Oct.csv" && -f "$DEST/2019-Nov.csv" ]]; then
  echo "Les données sont déjà présentes dans $DEST, rien à faire."
  exit 0
fi

# Lecture de l'identifiant et de la clé dans kaggle.json (sans les afficher).
KAGGLE_USER=$(python3 -c "import json; print(json.load(open('$CREDS'))['username'])")
KAGGLE_KEY=$(python3 -c "import json; print(json.load(open('$CREDS'))['key'])")

echo "Téléchargement de $DATASET (~4,3 Go compressé)..."
curl --location --fail --retry 5 --continue-at - \
     --user "$KAGGLE_USER:$KAGGLE_KEY" \
     --output "$ZIP" \
     "https://www.kaggle.com/api/v1/datasets/download/$DATASET"

echo "Décompression (~14,7 Go)..."
unzip -o "$ZIP" -d "$DEST"
rm "$ZIP"   # on libère 4 Go : le zip ne sert plus

echo "Terminé :"
ls -lh "$DEST"