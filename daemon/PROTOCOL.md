# Protocolo xneog-agentd — v1 (24-jul-2026)

Spec do protocolo entre o daemon (`xneog-code-bridge.mjs`, :8802) e os clientes
(app iOS/macOS via proxy `/code/*` do BFF :8801 · TUI `xneog-code` · futuros).
Fonte da verdade é o código; esta spec congela o CONTRATO — o que um cliente pode
assumir sem ler o daemon.

## 1. Identidade e versionamento

`GET /meta` → `{ name: "xneog-agentd", protocol: 1, capabilities: [...] }`

- `protocol` só sobe em QUEBRA de contrato (campo removido/renomeado, semântica mudada).
- Capacidade nova = append em `capabilities`, protocol não sobe. Cliente que não conhece
  uma capability ignora.
- Campo novo em payload existente = sempre aditivo e opcional. Clientes decodificam com
  campos opcionais (regra Swift: `let novo: Tipo?`).
- Capabilities atuais: `sessions, cli, tasks, commands, approval-queue, sse-replay,
  inject-tty, queue, revive, adopt, engines, transcripts, import`.

## 2. Transporte e auth

- HTTP/1.1 em `127.0.0.1:8802` (bind local; o device chega via proxy `/code/*` do BFF).
- Auth: `Authorization: Bearer <NATIVE_API_KEY>` OU token por-device
  `v2.<deviceId>.<expiry>.<hmac>` (TTL 10min, emitido pelo BFF — só p/ `/code/*`).
- Pairing (capability `pair`): `xneog pair` → `POST /pair/start` (master key) gera código
  single-use de 5min; o device troca via `POST /pair/claim` (sem auth — o código é a
  credencial) por `{ deviceId, secret }` e passa a cunhar tokens v2 localmente
  (`hmac = HMAC-SHA256(secret, "deviceId.expiry")`). 10 claims errados → pendências limpas.
- Respostas JSON `{...}` ou erro `{ "error": "..." }` com status HTTP correspondente.
- SSE: `text/event-stream`, frames `data: {...}\n\n`, heartbeat `: hb` a cada 15s.

## 3. Sessões

Sessão = processo dirigível no Mac. Duas classes (`engine`):

| engine   | processo                                     | modos | aprovação | resume |
|----------|----------------------------------------------|-------|-----------|--------|
| `claude` | `claude -p` stream-json persistente (1/sessão) | default/acceptEdits/plan/**auto** | fila do daemon | `--resume <claudeSession>` |
| `grok`   | 1 spawn POR TURNO sob Seatbelt (`grok-jail.sb`), cwd presa em `~/GrokWork/<id>` | NÃO (400) — a jaula é a parede | `--always-approve` | `-r <grokSession>` |
| `api`    | NENHUM — loop agentic do próprio daemon (F4): Messages API via chat-api, ≤20 iterações/turno | default/acceptEdits (plan → 400) | mesma fila do daemon (Bash/Write) | `transcripts/<id>.messages.json` |

Estados: `idle` (pronta) · `running` (turno em andamento) · `dead` (processo caiu;
`reviveable: true` se há `claudeSession` p/ `--resume`). Grok restaurada nasce `idle`
(turn-based, não tem processo pra morrer).

Persistência (F3): `sessions.json` = só metadata; eventos curados persistem em
`transcripts/<id>.jsonl` (append por evento, deltas fora). Restart re-hidrata o tail
(attach mostra histórico sem reviver); `?from=N` anterior ao buffer completa do arquivo
(replay profundo). O arquivo sobrevive ao DELETE da sessão. Processo não persiste —
revive sob demanda (`--resume`).

Import (rampa de migração): `POST /sessions/import { claudeSession }` cria sessão
dead/reviveável apontando pro transcript nativo do Claude Code, com o tail curado
hidratado (janela de 512KB em transcript gigante). Duplicata → 409 com o id existente.

### Payload da lista (`GET /sessions`)

```
{ sessions: [{ id, title, cwd, status, lastTs, turns, count, createdAt,
   needsInput,            // nº de aprovações pendentes
   engine,                // "claude" | "grok"
   aiTitle,               // título de IA do transcript ("" se não há; grok = "")
   connected,             // utilizável agora (grok: status != dead; claude: child vivo)
   reviveable, lastPrompt, model, permissionMode, archived,
   always: [tool...],     // grants "sempre permitir" ativos (revogáveis)
   queued, seq, readyForReview }] }
```

## 4. Eventos curados

Todo evento tem `i` (sequência monotônica POR SESSÃO — id e cursor, NUNCA assumir
contiguidade), `ts` (epoch ms) e `kind`. Buffer por sessão: 4000 eventos (splice dos
antigos; `i` segue crescendo).

| kind | payload extra | emitido quando |
|------|---------------|----------------|
| `user` | `text, images?, via?` | turno do usuário entrou (`via`: "terminal"\|"app" — cliente de origem) |
| `delta` | `text` | streaming do modelo — **EFÊMERO**: só ao vivo, fora do buffer/replay |
| `presence` | `terminal, app` | **EFÊMERO** — quem está com o stream aberto (muda ao entrar/sair) |
| `text` | `text` | bloco de texto consolidado (fim do bloco) |
| `thinking` | `text` | raciocínio (cliente mostra opt-in) |
| `tool_use` | `toolId, tool, input` | modelo chamou tool (grok: `tool` = `grok:<type>`) |
| `tool_result` | `toolId, output, isError?, runId?` | resultado (runId = Workflow) |
| `task` | `toolId, tool, name, agentType?, desc?` | Task/Agent/Workflow disparado |
| `command` | `name, output?` | slash command local (chip, não bolha de user) |
| `turn_end` | `ok, queued, next` | turno terminou (`next`: fila vai emendar outro) |
| `init` | `model, cwd, claudeSession` | primeiro init do stream-json |
| `queued` | `text, images?, depth` | mensagem entrou na fila (turno ocupado) |
| `queue_removed` | `removedId, depth` | item removido da fila |
| `queue_cleared` | `dropped, reason` | fila descartada (interrupt/morte) |
| `permission_request` | `requestId, tool, input` | tool aguardando decisão |
| `permission_resolved` | `requestId, approve, always?` | decisão tomada (por QUALQUER cliente) |
| `bulk_resolved` | `tools, approve, always, resolved, seeded` | lote (aceitar edições) |
| `mode_changed` / `model_changed` | `mode` / `model` | troca a quente (respawn c/ histórico) |
| `session_revived` | — | revive concluído |
| `session_end` | `code` | processo morreu (fila descartada, aprovações negadas) |

Contrato de replay: `GET /sessions/:id/stream?from=N` entrega `events[i >= N]` do buffer
e segue ao vivo. Cliente guarda `i+1` do último visto e reconecta com ele — perde só
deltas (o `text` consolidado cobre).

## 5. Streams SSE

- **Sessão**: `GET /sessions/:id/stream?from=N&client=terminal|app` — replay + ao vivo,
  heartbeat 15s, teto 8 assinantes/sessão (429 acima). `client` identifica o assinante
  para presença (lista expõe `clients: {terminal, app}`; mudanças viram evento `presence`).
  `POST /sessions/:id/message` aceita `via` com o mesmo vocabulário.
- **Lista**: `GET /events` — 1º frame `{hello:true}`; depois avisos de mudança da lista
  (sessão criada/morta/aprovação pendente…) → cliente refaz `GET /sessions`. Teto de
  assinantes global; o app usa 1.
- **Espelho CLI**: `GET /sessions/cli/:pid/stream?from=N` — frames
  `{ events, total, status, connected, driveVia, tasksRunning, aiTitle }` (transcript
  re-lido incremental; `from` é índice no array curado).

## 6. Aprovação (permission)

1. Daemon segura a tool e emite `permission_request` (+ push APNs).
2. Qualquer cliente resolve: `POST /sessions/:id/permission`
   `{ requestId, approve, always? }` — `always` cria grant persistente por tool
   (revogável: `DELETE /sessions/:id/always/:tool`). Bash/KillShell & cia NUNCA
   entram no always (nem por lote) — a razão da ponte existir é a fila segurar Bash.
3. Lote: `POST /sessions/:id/permission/bulk` `{ approve, always }` — resolve pendentes
   de escrita e semeia a família (Edit/Write/MultiEdit…).
4. Sem resposta em 120s = negado. `session_end` nega tudo pendente.
5. `bypassPermissions` do CLI NÃO é exposto. O modo **`auto`** (opt-in POR SESSÃO, 24-jul)
   é diferente: a fila continua no daemon, que aprova na hora COM trilha de auditoria
   (`by:"auto"` no audit.jsonl) — revogável a quente via `POST /mode`. Engines claude e api;
   grok segue sem modos (jaula).

## 7. Fila de turnos

Turno ocupado + mensagem nova = entra na fila (`queued`), emenda no `turn_end`
(`next:true`). `DELETE /sessions/:id/queue/:qid` remove item; interrupt/morte limpa
(`queue_cleared`). Escrever no stdin do claude a meio turno ABORTA o turno — por isso
a serialização é do daemon, nunca do cliente.

## 8. Tasks (subagentes/workflows)

`GET /sessions/:id/tasks` (bridge, via claudeSession) · `GET /sessions/cli/:pid/tasks`
(espelho). Fontes: `<sessionDir>/subagents/` (agent-*.jsonl + journal.jsonl started/result)
+ `workflows/wf_<runId>.json` rico (workflowProgress: label real, fase, model, tokens
por agente — pode nascer segundos depois do run; até lá o run aparece como runId cru).
Cache 3s.

```
{ runs: [{ runId, name, description, phases: [titulo...],
   phaseProgress?: [{ title, total, done }],     // só com wf.json rico
   agents: [{ id, label, agentType, model, tokens, startedAt, lastTs, running,
              phase? }],
   done, total, startedAt, lastTs }],
  running }
```

Heurística conhecida: agente AVULSO (sem journal) conta como `running` até 2min após o
último write (mtime). Workflow tem journal e conclui na hora.

## 9. Commands

`GET /commands` → `{ commands: [custom...], menu: [{ cmd, desc, args?, scope }] }`.
`menu` = custom + built-ins curados do CLI (one-shot apenas; nada que abra picker de TUI).
`scope: "both"` = vai como mensagem pro `-p`; comandos locais do cliente (ex.: `/model`
na TUI, menu "/" no app) são responsabilidade do cliente.

## 10. Engines e modelos

`GET /models` → `{ engines: { claude: { label, models, trusted, default },
grok: { label, models, trusted:false, jail, available, notes }, ... } }`.
Registry = builtin + overlay `engines.json` (editável sem deploy, cache por mtime).
Cliente: engine com `available:false` (ou ausente) não é oferecida.

Semântica grok: criação ignora `cwd` (jaula própria) · `mode` → 400 · `model` aplica no
PRÓXIMO turno (sem respawn) · interrupt = SIGTERM no turno · sem fila de aprovação.

Semântica api (F4 — loop agentic próprio, fallback existencial + caminho multi-tenant):
tools Bash/Read/Write/Glob (mesmo vocabulário do app — verbos e diff prontos); Bash e
Write passam pela MESMA fila de aprovação (`permission_request` idêntico; acceptEdits
auto-aprova Write; Bash pede sempre — NEVER_ALWAYS); histórico Messages API persiste em
`transcripts/<id>.messages.json` (janela deslizante de 60 sem quebrar par tool_use/result);
`model` aceita atalho (sonnet/haiku/opus/fable) ou id `claude-*`, aplica no próximo turno;
interrupt aborta o fetch, nega pendentes e mata Bash em voo. A ANTHROPIC_API_KEY vive SÓ
no chat-api (:3848, `POST /v1/agent/messages`, auth `AGENT_SERVICE_KEY` compartilhado,
parser 4mb próprio, metering sqlite sub "agentd" + logs/agentd-usage.jsonl); sem key
configurada lá, o turno falha limpo (`⚠️ chat-api 500` + turn_end ok:false).

## 11. Rotas (v1 completa)

```
GET  /meta                              identidade + protocol + capabilities
GET  /health                            { ok, sessions, pending, listSubs, streamSubs }
GET  /events                            SSE da lista
GET  /models                            registry de engines
GET  /projects                          ~/Projects por mtime (menu "+")
GET  /commands                          menu de comandos
GET  /sessions                          lista (§3)
POST /sessions                          { cwd?, title?, model?, engine?, permissionMode? } → { id, ... }
POST /sessions/import                   { claudeSession, title? } → { id, events, reviveable }
GET  /sessions/:id/stream?from=N        SSE da sessão (§5)
POST /sessions/:id/message              { text, images? } → turno (ou fila)
POST /sessions/:id/interrupt            cancela o turno
POST /sessions/:id/permission           { requestId, approve, always? }
POST /sessions/:id/permission/bulk      { approve, always }
DELETE /sessions/:id/always/:tool       revoga grant "sempre permitir"
POST /sessions/:id/mode                 { mode } (claude only; grok → 400)
POST /sessions/:id/model                { model }
POST /sessions/:id/rename               { title }
POST /sessions/:id/archive              { archived }
POST /sessions/:id/revive               respawn --resume (dead + reviveable)
GET  /sessions/:id/transcript           texto consolidado p/ compartilhar
GET  /sessions/:id/tasks                subagentes/workflows (§8)
DELETE /sessions/:id                    encerra processo (histórico fica)
DELETE /sessions/:id/queue/:qid         remove da fila
GET  /sessions/cli                      espelho read-only das sessões do CLI no Mac
GET  /sessions/cli/:pid/stream?from=N   SSE do espelho (alias: /transcript/stream)
GET  /sessions/cli/:pid/transcript      transcript curado do espelho
GET  /sessions/cli/:pid/tasks           tasks do espelho
POST /sessions/cli/:pid/interrupt       SIGINT no processo do CLI
POST /sessions/cli/:pid/adopt           adota sessão do CLI (vira dirigível)
POST /sessions/cli/:pid/inject          injeta texto no tty (gate: pid+start-time)
POST /internal/approval                 (interno) MCP de aprovação → daemon
POST /pair/start                        { name? } → { code, deviceId, expiresInSec } (só master key)
POST /pair/claim                        SEM auth: { code, name? } → { deviceId, secret, tokenFormat, maxTtlMs }
```

## 12. Regras de compatibilidade para clientes

1. Ignorar `kind` desconhecido (evento novo não pode quebrar render).
2. Ignorar campo desconhecido em qualquer payload.
3. `i` é cursor/id opaco — sem aritmética além de `from = max(i)+1`.
4. Reconexão SSE: backoff exponencial (1s → teto 15s), replay via `from`.
5. Feature-gate por `capabilities` (§1), nunca por versão de app.
