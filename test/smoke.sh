#!/bin/sh
# Smoke test de contrato do protocolo v1 — sobe o daemon em porta efêmera com estado
# isolado e valida o contrato SEM depender de Claude Code instalado (nível protocolo).
set -e
PKG="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d /tmp/agentd-smoke.XXXXXX)"
PORT=$((20000 + $$ % 10000))
KEY="smoke-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
trap 'kill $PID 2>/dev/null; rm -rf "$TMP"' EXIT

cp "$PKG/daemon/grok-jail.sb" "$TMP/"
XNEOG_AGENTD_DIR="$TMP" PORT="$PORT" NATIVE_API_KEY="$KEY" \
  node "$PKG/daemon/xneog-code-bridge.mjs" > "$TMP/boot.log" 2>&1 &
PID=$!

for i in $(seq 1 30); do
  curl -s -o /dev/null "http://127.0.0.1:$PORT/meta" 2>/dev/null && break
  sleep 0.3
done

fail() { echo "FAIL: $1"; cat "$TMP/boot.log"; exit 1; }
J() { curl -s -m 5 -H "Authorization: Bearer $KEY" "$@"; }
CODE() { curl -s -m 5 -o /dev/null -w '%{http_code}' "$@"; }

# 1. sem key → 401 (fail-closed)
[ "$(CODE "http://127.0.0.1:$PORT/meta")" = "401" ] || fail "sem key deveria dar 401"
# 2. /meta com key → protocol 1 + capabilities obrigatórias
META=$(J "http://127.0.0.1:$PORT/meta")
echo "$META" | grep -q '"protocol":1' || fail "/meta sem protocol:1 → $META"
for cap in sessions approval-queue sse-replay revive transcripts; do
  echo "$META" | grep -q "\"$cap\"" || fail "capability ausente: $cap"
done
# 3. /sessions começa vazio
J "http://127.0.0.1:$PORT/sessions" | grep -q '"sessions":\[\]' || fail "/sessions deveria começar vazio"
# 4. /models responde com engines
J "http://127.0.0.1:$PORT/models" | grep -q '"engines"' || fail "/models sem engines"
# 5. stream de sessão inexistente → 404
[ "$(CODE -H "Authorization: Bearer $KEY" "http://127.0.0.1:$PORT/sessions/nao-existe/stream?from=0")" = "404" ] || fail "stream de sessão inexistente deveria dar 404"
# 6. key errada → 401 mesmo com header
[ "$(CODE -H "Authorization: Bearer errada" "http://127.0.0.1:$PORT/sessions")" = "401" ] || fail "key errada deveria dar 401"

echo "OK: smoke de contrato v1 passou (porta $PORT, estado $TMP)"
