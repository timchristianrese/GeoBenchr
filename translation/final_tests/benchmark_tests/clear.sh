#!/usr/bin/env sh
# cleanup_tst_variants.sh
# Supprime dans le dossier courant.

set -eu

find . -maxdepth 1 -type f \
  \( -name 'tst_[0-9][0-9]_sedona.txt' -o \
     -name 'tst_[0-9][0-9]_spacetime.txt' -o \
     -name 'tst_[0-9][0-9]_postgis.txt' \) \
  -exec rm -f {} +