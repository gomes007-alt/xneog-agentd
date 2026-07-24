#!/bin/sh
# xneog-agentd installer — self-host BYOK.
set -e
command -v node >/dev/null 2>&1 || { echo "Node.js >= 20 é necessário: https://nodejs.org"; exit 1; }
node -e 'process.exit(Number(process.versions.node.split(".")[0]) >= 20 ? 0 : 1)' || { echo "Node.js >= 20 é necessário (atual: $(node -v))"; exit 1; }
npm install -g github:gomes007-alt/xneog-agentd
xneog-agentd init
echo
echo "Pronto. Suba o daemon com:  xneog-agentd run   (ou: xneog-agentd install)"
echo "Depois instale o cliente:   curl -fsSL https://cli.xneog.com/install.sh | sh"
