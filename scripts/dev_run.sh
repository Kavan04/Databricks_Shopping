#!/bin/sh
# Quick local run (no resource caps). dev_run.sh DATA OUT ; env: IMAGE, PLATFORM (e.g. linux/amd64), NCPU
set -eu
DATA=$(cd "$1" && pwd); mkdir -p "$2"; OUT=$(cd "$2" && pwd); chmod 777 "$OUT"
PLAT=""; [ -n "${PLATFORM:-}" ] && PLAT="--platform $PLATFORM"
docker run --rm $PLAT --network none --user 1000:1000 -v "$DATA":/data:ro -v "$OUT":/output -e NCPU="${NCPU:-8}" "${IMAGE:-shopping-sessions:latest}"
