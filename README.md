# xneog-agentd

Daemon self-host de sessões agentic. Envelopa o **seu** Claude Code (BYOK — sua conta, seu plano)
com sessões persistentes, replay por cursor e fila de aprovação **fail-closed**: toda ação com
efeito colateral vira um card aprovável no terminal ou no app iOS; sem resposta em 120s → deny.

Protocolo v1 documentado em [`daemon/PROTOCOL.md`](daemon/PROTOCOL.md). Clientes: [xneog-cli](https://github.com/gomes007-alt/xneog-cli), app iOS xNeog.

## Instalar (macOS, Node ≥ 20)

```sh
curl -fsSL https://cli.xneog.com/agentd-install.sh | sh
```

ou manualmente:

```sh
npm install -g github:gomes007-alt/xneog-agentd
xneog-agentd init       # gera key + estado em ~/.xneog
xneog-agentd run        # foreground; ou `xneog-agentd install` (launchd)
xneog-agentd status
```

## Requisitos

- macOS (Seatbelt é usado na jaula do engine grok; launchd no `install`)
- [Claude Code](https://claude.com/claude-code) instalado e logado (engine `claude`)
- Node.js ≥ 20

## Estado e segredos

- `~/.xneog/env` — segredos (0600); `NATIVE_API_KEY` é o bearer do daemon
- `~/.xneog/agentd/` — sessions.json, transcripts/, devices.json, logs/
- O daemon escuta **somente** em 127.0.0.1:8802; nunca vê `ANTHROPIC_API_KEY`

## Atualizar o daemon vendorado

`daemon/` é vendorado do repo de desenvolvimento (`xneog-code-bridge`). Para sincronizar:

```sh
./scripts/sync-daemon.sh
```
