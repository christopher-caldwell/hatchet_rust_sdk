#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
OUT="$ROOT/examples/multi-node/target/railway/build-context"

rm -rf "$OUT"
mkdir -p "$OUT/crates" "$OUT/examples"
rsync -a --delete --exclude target "$ROOT/crates/hatchet-core" "$OUT/crates/"
rsync -a --delete --exclude target "$ROOT/crates/hatchet-macros" "$OUT/crates/"
rsync -a --delete --exclude target --exclude tests --exclude benches \
  "$ROOT/crates/hatchet-sdk" "$OUT/crates/"
rsync -a --delete --exclude target \
  "$ROOT/examples/multi-node" "$OUT/examples/"
rsync -a "$ROOT/Cargo.toml" "$OUT/"
rsync -a \
  "$ROOT/examples/multi-node/railway.toml" \
  "$ROOT/examples/multi-node/.railwayignore" \
  "$OUT/"

test -f "$OUT/crates/hatchet-sdk/proto/v1/dispatcher.proto"
test -f "$OUT/railway.toml"
test -f "$OUT/.railwayignore"
printf 'railway_build_context=%s\n' "$OUT"
