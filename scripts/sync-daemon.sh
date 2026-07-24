#!/bin/sh
# Sincroniza daemon/ a partir do repo de desenvolvimento (xneog-code-bridge).
set -e
SRC="$HOME/Projects/xneog-code-bridge"
DST="$(dirname "$0")/../daemon"
for f in xneog-code-bridge.mjs mcp-approval.mjs grok-jail.sb PROTOCOL.md; do
  cp "$SRC/$f" "$DST/$f" && echo "sync: $f"
done
