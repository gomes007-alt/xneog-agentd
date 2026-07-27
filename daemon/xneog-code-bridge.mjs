#!/usr/bin/env node
/**
 * xneog-code-bridge — motor de sessões Claude Code dirigíveis pelo app iOS nativo (F0).
 * Sessões = processos `claude -p` persistentes (stream-json in/out) com streaming SSE em tempo real.
 * GET  /sessions                 → lista [{id,title,cwd,status,lastTs,turns}]
 * POST /sessions                 → cria {cwd?,title?} → {id}
 * POST /sessions/:id/message     → {text} envia turno do usuário
 * POST /sessions/:id/interrupt   → SIGINT no processo (para o turno atual)
 * DELETE /sessions/:id           → encerra processo
 * GET  /sessions/:id/stream?from=N → SSE: replay events[N..] + ao vivo (heartbeat 15s)
 * Auth: Bearer NATIVE_API_KEY (mesmo do BFF). Porta 8802, bind 127.0.0.1 (device chega via proxy /code do :8801).
 */
import { createServer } from "node:http";
import { spawn, spawnSync } from "node:child_process";
import { connect as netConnect } from "node:net";
import { readFileSync, writeFileSync, existsSync, mkdirSync, appendFileSync, unlinkSync, statSync, readdirSync, openSync, readSync, closeSync, chmodSync, watch as fsWatch } from "node:fs";
import { homedir } from "node:os";
import { randomUUID, timingSafeEqual, createHmac, randomBytes } from "node:crypto";
import { createInterface } from "node:readline";

const HOME = homedir();
const PORT = Number(process.env.PORT || 8802);
const DIR  = process.env.XNEOG_AGENTD_DIR || `${HOME}/Projects/xneog-code-bridge`;
const LOGDIR = `${DIR}/logs`; try { if (!existsSync(LOGDIR)) mkdirSync(LOGDIR, { recursive: true }); } catch {}
const AUDIT = `${LOGDIR}/audit.jsonl`;
// rotação-lite no boot (mesmo padrão do native-api): sem isto os logs crescem para sempre.
// 0600: os logs guardam trechos de sessão (prompts, I/O do PTY) → nunca world-readable.
for (const lp of [`${LOGDIR}/bridge.log`, AUDIT]) {
  try { if (existsSync(lp) && statSync(lp).size > 5 * 1024 * 1024) writeFileSync(lp, "", { mode: 0o600 }); } catch {}
  try { if (!existsSync(lp)) writeFileSync(lp, "", { mode: 0o600 }); else chmodSync(lp, 0o600); } catch {}
}

function loadEnv(p){ const o={}; if(existsSync(p)) for(const l of readFileSync(p,"utf8").split("\n")){ const m=l.match(/^\s*([A-Z_]+)\s*=\s*(.*)\s*$/); if(m) o[m[1]]=m[2].replace(/^["']|["']$/g,""); } return o; }
const env = { ...loadEnv(`${HOME}/.xneog/env`), ...process.env };
const KEY = env.NATIVE_API_KEY || "";
if (!KEY) { console.error("FATAL: NATIVE_API_KEY ausente no .env"); process.exit(1); }

// Blindagem de processo: um throw solto (spawn falho, JSON podre) NÃO pode derrubar o bridge
// inteiro — ele segura todas as sessões vivas do Gomes. Mesmo padrão do native-api.
process.on("uncaughtException", (e) => { try { log(`UNCAUGHT: ${String(e?.stack || e).slice(0, 300)}`); } catch {} });
process.on("unhandledRejection", (e) => { try { log(`UNHANDLED: ${String(e?.stack || e).slice(0, 300)}`); } catch {} });

const WIN = process.platform === "win32";
const CLAUDE = process.env.XNEOG_CLAUDE_BIN || `${HOME}/.local/bin/claude`;
const PATHENV = WIN ? (process.env.PATH || "")
  : `${HOME}/.local/bin:${HOME}/.local/node/bin:${process.env.PATH || ""}:/usr/bin:/bin`;
// F4: loop agentic próprio via chat-api (:3848) — a ANTHROPIC_API_KEY vive SÓ lá; o bridge fala
// com o passthrough /v1/agent/messages usando um service key compartilhado (metering central).
const CHAT_API = env.CHAT_API_BASE || "http://127.0.0.1:3848";
const AGENT_KEY = env.AGENT_SERVICE_KEY || "";
// F1 = leitura auto-aprovada; TODO o resto (Bash/Edit/Write/...) passa pela FILA DE APROVAÇÃO no app.
// --permission-mode default força o prompt (vence o "auto" global); --permission-prompt-tool roteia o prompt
// pro mcp-approval.mjs, que segura no /internal/approval até o owner decidir (120s → deny fail-closed).
const ALLOWED = "Read,Glob,Grep,WebFetch,WebSearch,TodoWrite";
const APPROVAL_TIMEOUT_MS = 120000;
const CFGDIR = `${DIR}/.mcp`; try { if (!existsSync(CFGDIR)) mkdirSync(CFGDIR, { recursive: true, mode: 0o700 }); } catch {}
// Segredo INTERNO do canal de aprovação: gerado a cada boot, entregue só ao mcp-approval (filho do
// bridge, via env do mcp-config). /internal/approval passa pelo auth() global (que um device também
// satisfaz por token v2), então SEM esta 2ª parede um device podia chamar /code/internal/approval e
// floodar a fila (cards forjados + sockets/timers acumulados). O device via proxy nunca tem este header.
const APPROVAL_SECRET = randomBytes(24).toString("hex");
// grava o mcp-config (que contém BRIDGE_KEY) em arquivo 0600 e passa o CAMINHO no argv —
// o segredo deixa de aparecer no `ps aux` (antes ia inline via --mcp-config).
function writeMcpConfig(id){
  const p = `${CFGDIR}/${id}.json`;
  writeFileSync(p, JSON.stringify({ mcpServers: { approver: {
    command: process.execPath,
    args: [`${DIR}/mcp-approval.mjs`],
    env: { BRIDGE_SESSION: id, BRIDGE_PORT: String(PORT), BRIDGE_KEY: KEY, APPROVAL_SECRET },
  } } }), { mode: 0o600 });
  return p;
}
const MAX_PENDING = 64;            // teto global de aprovações pendentes (anti-exhaustion / flood de cards)
const MAX_LIST_SUBS = 16;          // teto de SSE global (/events)
const MAX_SESSION_SUBS = 8;        // teto de SSE por sessão (/sessions/:id/stream)
const MAX_TRANSCRIPT_STREAMS = 16; // teto global de streams de transcript CLI
let transcriptStreams = 0;
const MAX_SESSIONS = 6;
const MAX_EVENTS = 4000;          // buffer de replay por sessão
const TOOL_RESULT_CAP = 4000;     // trunca payloads gigantes pro app
// PROSA não é payload. O cap de 4000 servia pra saída de ferramenta (log de build, arquivo lido),
// mas era aplicado também ao texto do agente: uma resposta longa chegava cortada no meio da frase,
// SEM marcador — no app parecia que o agente tinha parado de escrever. Prosa ganha cap próprio, e
// truncar (raro: a janela de leitura do transcript já limita o volume) passa a deixar rastro.
const TEXT_CAP = 200_000;
const capText = s => s.length > TEXT_CAP ? s.slice(0, TEXT_CAP) + `\n\n… [+${s.length - TEXT_CAP} caracteres truncados]` : s;

// Serialização do input de tool PRESERVANDO O JSON VÁLIDO. Antes: JSON.stringify(...).slice(cap) —
// um Write de 10KB virava JSON cortado no meio, o app não conseguia parsear e o cartão de aprovação
// perdia diff, caminho do arquivo e virava um blob ilegível. Justo no cartão onde o dono decide.
// Agora corta o VALOR de cada campo longo (marcando o corte) e mantém a estrutura intacta.
function capInput(obj, cap = TOOL_RESULT_CAP){
  try {
    if (!obj || typeof obj !== "object") return JSON.stringify(obj ?? {});
    const campos = Object.keys(obj).length || 1;
    const porCampo = Math.max(400, Math.floor(cap / campos));
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v === "string" && v.length > porCampo) out[k] = v.slice(0, porCampo) + `\n… [+${v.length - porCampo} chars]`;
      else if (Array.isArray(v)) out[k] = v.slice(0, 20).map(x => {   // ex.: edits[] do MultiEdit
        if (x && typeof x === "object") {
          const o2 = {};
          for (const [k2, v2] of Object.entries(x)) o2[k2] = (typeof v2 === "string" && v2.length > porCampo) ? v2.slice(0, porCampo) + `\n… [+${v2.length - porCampo} chars]` : v2;
          return o2;
        }
        return (typeof x === "string" && x.length > porCampo) ? x.slice(0, porCampo) + "…" : x;
      });
      else out[k] = v;
    }
    const s = JSON.stringify(out);
    return s.length > cap * 2 ? s.slice(0, cap * 2) : s;   // rede de segurança, raríssimo
  } catch { return JSON.stringify({ erro: "input não serializável" }); }
}

const sessions = new Map();       // id → S
const pending = new Map();        // requestId → {res, sid, tool, timer}  (aprovações aguardando o owner)
let reqSeq = 0;
const JSONH = { "Content-Type": "application/json" };

// ── SSE global da LISTA de sessões ───────────────────────────────────────────
// Antes o app pollava /sessions a cada 12s: uma aprovação podia levar 12s pra acender o badge
// com o app aberto. Aqui só avisamos "mudou" (ping) — o app refaz o GET, que é leve (~2KB).
// Enviar a lista inteira duplicaria a serialização e o cálculo de needsInput.
const listSubs = new Set();
let _notifyTimer = null;
// kinds que mudam algo VISÍVEL na lista (status, turns, needsInput, fila). `delta` fica de fora.
const LIST_KINDS = new Set([
  "user", "turn_end", "permission_request", "permission_resolved",
  "session_end", "session_revived", "queued", "queue_cleared", "queue_removed", "mode_changed", "model_changed",
]);
function notifySessions(){
  clearTimeout(_notifyTimer);
  _notifyTimer = setTimeout(() => {
    const frame = `data: ${JSON.stringify({ t: Date.now() })}\n\n`;
    for (const res of listSubs) { try { res.write(frame); } catch { try { listSubs.delete(res); } catch {} } }
  }, 250);   // coalesce: um turno dispara vários kinds em rajada
}

// ── Persistência (histórico entre restarts) ──────────────────────────────────
// Antes o Map era só memória: derrubar o serviço = "Sem sessões" na tela. Agora a METADATA
// sobrevive (events não — são pesados e o --resume do claude reconstrói o contexto).
// Sessão restaurada nasce status:"dead" e reanima via POST /sessions/:id/revive (--resume claudeSession).
const SESS_FILE = `${DIR}/sessions.json`;
const MAX_PERSIST = 30;
let _saveTimer = null;

// ── F3: transcript próprio — eventos curados em JSONL por sessão ─────────────
// sessions.json guarda só METADATA; os eventos persistem em transcripts/<id>.jsonl (append por
// evento; deltas fora — são efêmeros por contrato). Restart re-hidrata o tail (attach mostra
// histórico sem reviver); replay ?from=N anterior ao buffer completa do arquivo. O arquivo fica
// mesmo após DELETE da sessão (é texto curado e capado — histórico barato). Strings já chegam
// capadas (TOOL_RESULT_CAP), então ler o arquivo inteiro no replay profundo é aceitável; se um
// dia passar de ~10MB, janelar aqui igual ao readCliEvents.
const TRANS_DIR = `${DIR}/transcripts`;
try { mkdirSync(TRANS_DIR, { recursive: true, mode: 0o700 }); } catch {}
function transAppend(id, item){
  try { appendFileSync(`${TRANS_DIR}/${id}.jsonl`, JSON.stringify(item) + "\n", { mode: 0o600 }); } catch {}
}
function transRead(id, from = 0, cap = MAX_EVENTS){
  let text = ""; try { text = readFileSync(`${TRANS_DIR}/${id}.jsonl`, "utf8"); } catch { return []; }
  const out = [];
  for (const line of text.split("\n")) {
    if (!line) continue;
    let j; try { j = JSON.parse(line); } catch { continue; }
    if (typeof j.i === "number" && j.i >= from) out.push(j);
  }
  return out.length > cap ? out.slice(-cap) : out;
}

function sessMeta(S){
  return { id: S.id, title: S.title, cwd: S.cwd, turns: S.turns, lastTs: S.lastTs, createdAt: S.createdAt,
           claudeSession: S.claudeSession, model: S.model, permissionMode: S.permissionMode || "default",
           archived: !!S.archived, lastPrompt: S.lastPrompt || "", lastTurnEndTs: S.lastTurnEndTs || 0, seq: S.seq || 0,
           engine: S.engine || "claude", grokSession: S.grokSession || "" };   // F2: multi-engine
}
function saveSessions(){
  clearTimeout(_saveTimer);
  _saveTimer = setTimeout(() => {
    try {
      const arr = [...sessions.values()].map(sessMeta).sort((a, b) => b.lastTs - a.lastTs).slice(0, MAX_PERSIST);
      writeFileSync(SESS_FILE, JSON.stringify(arr, null, 2), { mode: 0o600 });
    } catch (e) { log(`save sessions err ${e.message}`); }
  }, 500);
}
function loadSessions(){
  let arr = []; try { arr = JSON.parse(readFileSync(SESS_FILE, "utf8")); } catch { return; }
  if (!Array.isArray(arr)) return;
  for (const m of arr) {
    if (!m || typeof m.id !== "string") continue;
    // grok é turn-based (spawn por turno): restaurada volta IDLE e já aceita mensagem — não há
    // processo persistente pra reviver. claude restaurada nasce dead (revive = --resume).
    const status = (m.engine === "grok" || m.engine === "api") ? "idle" : "dead";   // turn-based não tem processo pra morrer
    // F3: re-hidrata o tail do transcript próprio — attach/app mostram histórico sem reviver
    const past = transRead(m.id, 0, 1000);
    const seq = Math.max(m.seq || 0, past.length ? past[past.length - 1].i + 1 : 0);
    sessions.set(m.id, { ...m, child: null, status, events: past, subs: new Set(), always: new Set(),
                         queue: [], seq, lastNotify: {}, restored: true });   // fila não persiste (segura base64)
  }
  log(`restauradas ${sessions.size} sessões (claude=dead/reviváveis · grok=idle · eventos re-hidratados)`);
}

const liveCount = () => [...sessions.values()].filter(S => S.child && S.status !== "dead").length;

// presença: quantos clientes de cada tipo estão com o stream desta sessão aberto AGORA
function presenceOf(S){
  let terminal = 0, app = 0;
  for (const r of S.subs) { if (r.clientTag === "terminal") terminal++; else if (r.clientTag === "app") app++; }
  return { terminal, app };
}

// ── Sessões do Claude Code CLI rodando no Mac (read-only) ────────────────────
// O app OFICIAL lista estas (o CLI se registra em ~/.claude/sessions/<pid>.json e fala com o relay
// da Anthropic). O bridge não as DIRIGE — só controla processos que ele mesmo spawna com stream-json.
// Expor read-only dá paridade visual sem mentir sobre o que dá pra fazer.
const CLI_SESS_DIR = `${HOME}/.claude/sessions`;
const CLI_PROJECTS = `${HOME}/.claude/projects`;
const pidAlive = pid => { try { process.kill(pid, 0); return true; } catch { return false; } };

// Apelidos dados pelo usuário às sessões do terminal. Overlay do BRIDGE, não do CLI: escrever no
// ~/.claude/sessions/<pid>.json não adianta — o próprio Claude Code reescreve aquele arquivo
// (status/updatedAt/name) e comeria o apelido. Chave = sessionId quando existe (estável entre
// restarts do bridge), senão `pid:<pid>` (pid recicla, por isso a poda ao salvar).
const CLI_NAMES_FILE = `${DIR}/cli-names.json`;
const cliNameKey = j => (j.sessionId ? `sid:${j.sessionId}` : `pid:${j.pid}`);
let cliNames = {};
try { const o = JSON.parse(readFileSync(CLI_NAMES_FILE, "utf8")); if (o && typeof o === "object") cliNames = o; } catch {}
function saveCliNames(){
  // poda: apelido preso a um pid que não existe mais é lixo (o sid sobrevive, o pid não)
  for (const k of Object.keys(cliNames)) {
    if (k.startsWith("pid:") && !pidAlive(Number(k.slice(4)))) delete cliNames[k];
  }
  try { writeFileSync(CLI_NAMES_FILE, JSON.stringify(cliNames, null, 2), { mode: 0o600 }); }
  catch (e) { log(`save cli-names err ${e.message}`); }
}


// Snapshot pid→{tty,startMs} de UMA chamada `ps -axo` (TTL 1s). tty e start-time são imutáveis na vida
// do processo, então um snapshot recente serve pra todas as sessões de um request. Antes cada sessão
// disparava 2 spawnSync (lstart + tty) por poll de /sessions/cli → 8-12 forks bloqueavam a thread única
// e engasgavam os writes de SSE do streaming. Agora: 1 fork por janela de 1s, 0 em cache hit.
let _psCacheAt = 0, _psCache = null;
function procTable(){
  const now = Date.now();
  if (_psCache && now - _psCacheAt < 1000) return _psCache;
  const map = new Map();
  try {
    const out = spawnSync("ps", ["-axo", "pid=,tty=,lstart="], { encoding: "utf8" }).stdout || "";
    for (const line of out.split("\n")) {
      const m = line.match(/^\s*(\d+)\s+(\S+)\s+(.+)$/);   // pid · tty(ou ??) · lstart(com espaços)
      if (!m) continue;
      const ms = Date.parse(m[3].trim());
      map.set(Number(m[1]), { tty: m[2], startMs: Number.isFinite(ms) ? ms : 0 });
    }
  } catch {}
  _psCache = map; _psCacheAt = now;
  return map;
}
// epoch (ms) de início do processo vivo, do snapshot. Comparo por EPOCH (não string) porque o procStart
// do json está em UTC e o ps em hora local → strings nunca casariam.
const procStartMs = pid => procTable().get(pid)?.startMs || 0;
// o processo vivo é o MESMO que registrou a sessão? (ps trunca sub-segundo → tolerância de 3s)
const startMatches = (pid, startedAt) => { if (!startedAt) return true; const live = procStartMs(pid); return live > 0 && Math.abs(live - startedAt) < 3000; };
// GATE ANTI-RCE: só injeta em pid que é uma sessão Claude Code REGISTRADA (${pid}.json parseável), viva,
// E cujo processo é o MESMO (start-time bate). Sem isso, um pid reusado por um shell viraria alvo de
// `do script`/socket = execução de comando fora da fila de aprovação (RCE).
function validCliPid(pid){
  if (!pid || !Number.isInteger(pid)) return null;
  let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { return null; }
  if (!meta || meta.pid !== pid) return null;
  if (!pidAlive(pid) || !startMatches(pid, meta.startedAt)) return null;
  return meta;
}
// input do usuário → tira TODO controle (\r \n \v \f \t e C0/DEL): um \r isolado submeteria a linha
// no meio e viraria multi-comando. O Enter de submissão é adicionado 1x pelo injetor (nunca aqui).
const sanitizeInput = s => String(s).replace(/[\x00-\x1f\x7f]+/g, " ").trim();

// ── Injeção no terminal (dirigir a sessão do CLI SEM tocar a Anthropic) ───────
// A sessão `claude` interativa não escuta socket algum; o único jeito de mandar
// input é digitar no TTY dela. `xneog-inject` faz isso via Terminal.app, mirando
// pelo TTY EXATO (nunca a janela da frente). O bridge resolve pid→tty e delega.
const INJECT_BIN = `${HOME}/.local/bin/xneog-inject`;
function sessionTty(pid){
  const t = procTable().get(pid)?.tty;               // do snapshot batelado (sem fork por chamada)
  if (!t || t === "??" || t === "?") return null;     // job de fundo / sem terminal
  const dev = "/dev/" + t;
  return /^\/dev\/ttys[0-9]{1,4}$/.test(dev) ? dev : null;
}
function injectToTty(tty, text){
  return new Promise(resolve => {
    const p = spawn(INJECT_BIN, ["--tty", tty], { stdio: ["pipe", "ignore", "pipe"] });
    let err = "";
    p.stderr.on("data", d => { err += d; });
    p.on("error", e => resolve({ ok: false, err: e.message }));
    p.on("close", code => resolve({ ok: code === 0, code, err: err.trim() }));
    p.stdin.write(text); p.stdin.end();
  });
}
// Stop: xneog-inject --interrupt manda ESC na aba (cancela o turno, não mata). Sem stdin.
function interruptTty(tty){
  return new Promise(resolve => {
    const p = spawn(INJECT_BIN, ["--tty", tty, "--interrupt"], { stdio: ["ignore", "ignore", "pipe"] });
    let err = "";
    p.stderr.on("data", d => { err += d; });
    p.on("error", e => resolve({ ok: false, err: e.message }));
    p.on("close", code => resolve({ ok: code === 0, code, err: err.trim() }));
  });
}

// ── Injeção em bg job (PTY socket) ────────────────────────────────────────────
// Job de fundo não tem TTY (`tty ??`): o input entra pelo socket PTY que o `--bg-pty-host` serve.
// Só o HELLO é framed [len:uint32 BE][0x01][json]; depois disso o canal é BYTES CRUS nos dois
// sentidos (ver injectToSocket). O PTY socket aceita input sem auth OAuth (auth é do control socket).
function sockForJob(jobId){
  if (!jobId || !/^[a-f0-9]{6,40}$/i.test(jobId)) return null;
  if (!process.getuid) return null; // win32: sem PTY socket Unix — injeção indisponível
  const base = `/tmp/cc-daemon-${process.getuid()}`;
  let dirs = []; try { dirs = readdirSync(base); } catch { return null; }
  for (const d of dirs) { const p = `${base}/${d}/pty/${jobId}.sock`; if (existsSync(p)) return p; }
  return null;
}
function sessionSocket(pid){
  let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { return null; }
  return sockForJob(meta.jobId);
}
// Depois do hello (frame [len][01][json]), o pty.sock é um canal de BYTES CRUS nos dois sentidos
// (igual um TTY). Input = os bytes que você "digita" + Enter (\r). Nada de frame JSON no input —
// enframar faz o JSON literal ser digitado na sessão.
// ENTREGA CONFIRMADA: escreve bytes crus + Enter, coleta o eco, e só reporta ok se o TEXTO reaparece
// no eco (o TUI renderiza o que foi digitado). Sem isso o composer da bg mentiria sucesso. NUNCA loga
// o conteúdo do eco (é I/O da sessão) — só o veredito.
function injectToSocket(sockPath, text){
  return new Promise(resolve => {
    let done = false, wrote = false, echo = "";
    const finish = (ok, err) => {
      if (done) return; done = true; try { sock.destroy(); } catch {}
      log(`inject-socket ok=${ok}${err ? " err=" + err : ""}`);
      resolve({ ok, err });
    };
    const sock = netConnect(sockPath);
    sock.on("error", e => finish(false, e.message));
    sock.on("data", d => {
      if (!wrote) {
        wrote = true;
        try { sock.write(Buffer.from(text + "\r", "utf8")); } catch (e) { return finish(false, e.message); }
        setTimeout(() => {
          const clean = echo.replace(/\x1b\[[0-9;?]*[A-Za-z]/g, "").replace(/[\x00-\x1f\x7f]+/g, "");
          const probe = text.replace(/\s+/g, "").slice(0, 24);
          const delivered = probe.length > 0 && clean.replace(/\s+/g, "").includes(probe);
          finish(delivered, delivered ? null : "input não apareceu no eco da sessão");
        }, 900);
      } else if (echo.length < 262144) { echo += d.toString("utf8"); }   // teto: não acumular a tela toda
    });
    setTimeout(() => finish(false, "timeout no socket"), 3000);
  });
}

// ── Transcript das sessões do CLI (read-only) ────────────────────────────────
// O CLI grava tudo em ~/.claude/projects/<slug>/<sessionId>.jsonl. Achar por varredura
// é mais robusto que derivar o slug do cwd (o slug troca "/" e "." por "-").
function findTranscript(sessionId){
  if (!/^[0-9a-f-]{16,40}$/i.test(sessionId)) return "";
  let dirs = []; try { dirs = readdirSync(CLI_PROJECTS); } catch { return ""; }
  for (const d of dirs) {
    const p = `${CLI_PROJECTS}/${d}/${sessionId}.jsonl`;
    if (existsSync(p)) return p;
  }
  return "";
}

// Título de IA de uma sessão do BRIDGE (linha {"type":"ai-title"} no transcript que o claude -p
// grava). O CLI REESCREVE o título ao longo da sessão — o último vale. Ler o arquivo inteiro por
// poll da lista seria caro (transcripts passam de 8MB): tail de 256KB pega o mais recente; se a
// sessão é longa e o título parou de mudar cedo, fallback na cabeça de 64KB. Cache TTL 30s.
const titleCache = new Map();   // claudeSession → { ts, title }
function aiTitleFor(sessionId){
  if (!sessionId) return "";
  const c = titleCache.get(sessionId);
  if (c && Date.now() - c.ts < 30_000) return c.title;
  let title = "";
  try {
    const p = findTranscript(sessionId);
    if (p) {
      const scan = (buf) => {
        for (const line of buf.split("\n")) {
          if (!line.includes(`"ai-title"`)) continue;
          try { const j = JSON.parse(line); if (j.type === "ai-title" && j.aiTitle) title = String(j.aiTitle).slice(0, 120); } catch {}
        }
      };
      const size = statSync(p).size, fd = openSync(p, "r");
      try {
        const tail = Math.min(size, 256 * 1024), b = Buffer.alloc(tail);
        readSync(fd, b, 0, tail, size - tail);
        scan(b.toString("utf8"));
        if (!title && size > tail) {
          const head = Buffer.alloc(64 * 1024);
          const n = readSync(fd, head, 0, head.length, 0);
          scan(head.toString("utf8", 0, n));
        }
      } finally { closeSync(fd); }
    }
  } catch {}
  titleCache.delete(sessionId); titleCache.set(sessionId, { ts: Date.now(), title });   // toca: LRU
  while (titleCache.size > 64) titleCache.delete(titleCache.keys().next().value);
  return title;
}

// Leitura INCREMENTAL: o transcript desta sessão passa de 8MB. Reparsear a cada poll queimaria
// CPU à toa — lê só o delta a partir do offset e guarda a linha parcial (`carry`).
// Cada transcript aberto vira um array de eventos em memória. O bridge é processo longevo:
// sem teto, abrir várias sessões do CLI vaza memória pra sempre. LRU simples de 4 (o Map do JS
// preserva ordem de inserção → o primeiro é o menos recente).
const CLI_CACHE_MAX = 4;
const cliCache = new Map();   // path → { size, events, carry, windowStart }
const CLI_READ_WINDOW = 512 * 1024;
// Curação compartilhada de 1 linha do transcript (.jsonl) → eventos p/ a UI. F0 23-jul: além de user/text/tool_use,
// emite tool_result (do array-form user content) + `id` no tool_use → o app casa tool_use↔result e mostra output/diff.
// F1 23-jul: emite thinking (raciocínio) — o app mostra opt-in atrás de um toggle (default oculto).
function curateCliLine(j, out, sink){
  const t = j.type;
  // Título de IA da sessão (o app oficial RC usa como header) — vai no sink, não como evento.
  if (t === "ai-title" && j.aiTitle) { if (sink) sink.aiTitle = String(j.aiTitle).slice(0, 120); return; }
  if (t === "user" && !j.isMeta) {
    const content = j.message?.content;
    if (typeof content === "string") {
      // Slash command local (/effort, /status…): o transcript grava <command-name> e o stdout em
      // <local-command-stdout> — vira evento "command" (chip + saída mono no app), não bolha de user.
      if (content.includes("<command-name>") || content.includes("<local-command-stdout>")) {
        const name = content.match(/<command-name>([^<]*)<\/command-name>/)?.[1]?.trim() || "";
        // stdout de comando traz códigos ANSI de estilo ([1m…[22m) — inúteis no app, fora
        const stdout = (content.match(/<local-command-stdout>([\s\S]*?)<\/local-command-stdout>/)?.[1] || "")
          .replace(/\x1b?\[[0-9;]*m/g, "").trim();
        if (name || stdout) out.push({ kind: "command", name, output: stdout.slice(0, TOOL_RESULT_CAP) });
      } else if (content.trim()) out.push({ kind: "user", text: capText(content) });
    } else if (Array.isArray(content)) {
      for (const b of content) {
        if (b?.type === "tool_result") {
          const txt = typeof b.content === "string" ? b.content
            : Array.isArray(b.content) ? b.content.map(x => typeof x === "string" ? x : (x?.text || "")).join("\n") : "";
          // resultado do Workflow traz o Run ID → elo do card com GET /tasks (progresso vivo)
          const run = txt.match(/Run ID: (wf_[a-z0-9-]{6,})/)?.[1];
          out.push({ kind: "tool_result", id: b.tool_use_id, output: txt.slice(0, TOOL_RESULT_CAP), isError: !!b.is_error, ...(run ? { runId: run } : {}) });
        } else if (b?.type === "text" && b.text?.trim()) {
          out.push({ kind: "user", text: capText(b.text) });
        }
      }
    }
  } else if (t === "assistant") {
    for (const b of (j.message?.content || [])) {
      if (b.type === "text" && b.text) out.push({ kind: "text", text: capText(b.text) });
      else if (b.type === "thinking" && b.thinking) out.push({ kind: "thinking", text: capText(b.thinking) });
      else if (b.type === "tool_use") {
        const inp = b.input ?? {};
        // Subagentes/workflows viram card nomeado (padrão do app oficial RC), não tool genérico.
        if (b.name === "Task" || b.name === "Agent") {
          out.push({ kind: "task", id: b.id, tool: b.name,
                     name: String(inp.description || inp.subagent_type || "subagente").slice(0, 80),
                     agentType: String(inp.subagent_type || ""), input: "" });
        } else if (b.name === "Workflow") {
          const script = String(inp.script || "");
          out.push({ kind: "task", id: b.id, tool: "Workflow",
                     name: (script.match(/name:\s*['"]([^'"]+)['"]/)?.[1] || String(inp.name || "workflow")).slice(0, 80),
                     desc: (script.match(/description:\s*['"]([^'"]+)['"]/)?.[1] || "").slice(0, 160), input: "" });
        } else out.push({ kind: "tool_use", id: b.id, tool: b.name, input: JSON.stringify(inp).slice(0, TOOL_RESULT_CAP) });
      }
    }
  }
}
function readCliEvents(path){
  let st; try { st = statSync(path); } catch { return []; }
  let c = cliCache.get(path);
  if (!c || st.size < c.size) { c = { size: 0, events: [], carry: "", windowStart: 0 }; cliCache.set(path, c); }
  else { cliCache.delete(path); cliCache.set(path, c); }   // toca: vira o mais recente
  while (cliCache.size > CLI_CACHE_MAX) cliCache.delete(cliCache.keys().next().value);
  if (st.size === c.size) return c.events;

  // COLD-CACHE em transcript grande: NUNCA materializar o arquivo inteiro (chega a 100MB+ e trava a
  // thread única do node por segundos, derrubando heartbeats SSE). Lê só uma janela do FIM e descarta
  // a 1ª linha (parcial) — o app pede tail de qualquer jeito. Reads incrementais depois pegam o delta.
  let from = c.size, dropPartial = false;
  if (c.size === 0 && st.size > CLI_READ_WINDOW) { from = st.size - CLI_READ_WINDOW; dropPartial = true; }
  if (c.size === 0) c.windowStart = from;   // byte onde a janela do tail começou (>0 = há histórico anterior p/ back-paginar)

  const len = st.size - from;
  let text = "";
  let fd = null;
  try {
    fd = openSync(path, "r");
    const buf = Buffer.alloc(len);
    readSync(fd, buf, 0, len, from);
    text = c.carry + buf.toString("utf8");
  } catch { return c.events; }
  finally { if (fd !== null) { try { closeSync(fd); } catch {} } }   // sem finally, exceção vaza o fd

  const lines = text.split("\n");
  c.carry = lines.pop() ?? "";
  if (dropPartial && lines.length) lines.shift();   // começamos no meio de um evento → 1ª linha é lixo
  for (const line of lines) {
    if (!line.trim()) continue;
    let j; try { j = JSON.parse(line); } catch { continue; }
    curateCliLine(j, c.events, c);   // c recebe aiTitle (título de IA da sessão)
  }
  c.size = st.size;
  return c.events;
}
// Back-pagination (F0): lê uma janela de 512KB TERMINANDO em `before` (byte), cura e devolve os eventos +
// onde a janela começa (`windowStart`). One-shot — histórico é imutável, não cacheia. O app faz prepend.
function readCliWindow(path, before){
  let st; try { st = statSync(path); } catch { return { events: [], windowStart: 0, fileSize: 0 }; }
  const end = Math.max(0, Math.min(before, st.size));
  const from = Math.max(0, end - CLI_READ_WINDOW);
  const dropPartial = from > 0;
  const len = end - from;
  let text = "", fd = null;
  try { fd = openSync(path, "r"); const buf = Buffer.alloc(len); readSync(fd, buf, 0, len, from); text = buf.toString("utf8"); }
  catch { return { events: [], windowStart: from, fileSize: st.size }; }
  finally { if (fd !== null) { try { closeSync(fd); } catch {} } }
  const lines = text.split("\n");
  lines.pop();   // última linha cortada em `before` = parcial
  if (dropPartial && lines.length) lines.shift();   // 1ª linha cortada no começo da janela = parcial
  const out = [];
  for (const line of lines) { if (!line.trim()) continue; let j; try { j = JSON.parse(line); } catch { continue; } curateCliLine(j, out); }
  return { events: out, windowStart: from, fileSize: st.size };
}
// ── Subagentes/workflows da sessão (padrão do app oficial RC, ver reference_claude_rc_ui_patterns) ──
// O CLI grava em <sessionDir>/subagents/: agent-<id>.jsonl (transcript por agente, usage por linha
// assistant) + workflows/<runId>/{journal.jsonl, agent-*.jsonl, *.meta.json}. journal: "started" sem
// "result" = agente rodando. Script persistido em <sessionDir>/workflows/scripts/<name>-<runId>.js
// dá nome/descrição/fases. Leitura INCREMENTAL (delta por size) — nunca materializar de novo.
const taskCache = new Map();   // sessionId → { at, data } · TTL 3s (o SSE pusha a cada write do transcript)
const agentTok = new Map();    // path → { size, tokens, model, label } · soma incremental de usage
function shortModel(m){ return String(m || "").replace(/^claude-/, "").replace(/-\d{8}$/, ""); }
function agentStats(path){
  let st; try { st = statSync(path); } catch { return null; }
  let c = agentTok.get(path);
  if (!c || st.size < c.size) c = { size: 0, tokens: 0, model: "", label: "" };
  if (st.size > c.size) {
    let from = c.size;
    if (from === 0 && st.size > 4 * 1024 * 1024) from = st.size - CLI_READ_WINDOW;   // gigante: só tail
    const len = st.size - from;
    let fd = null, text = "";
    try { fd = openSync(path, "r"); const buf = Buffer.alloc(len); readSync(fd, buf, 0, len, from); text = buf.toString("utf8"); }
    catch { return { ...c, mtime: st.mtimeMs, birth: st.birthtimeMs || st.mtimeMs }; }
    finally { if (fd !== null) { try { closeSync(fd); } catch {} } }
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      let j; try { j = JSON.parse(line); } catch { continue; }
      const u = j.message?.usage;
      if (u) { c.tokens += (u.output_tokens || 0); if (j.message?.model) c.model = j.message.model; }
      if (!c.label && j.type === "user") {
        const mc = j.message?.content;
        const txt = typeof mc === "string" ? mc : Array.isArray(mc) ? (mc.find(x => x?.type === "text")?.text || "") : "";
        if (txt.trim()) {
          // label = 1ª linha SUBSTANTIVA do prompt: pula preâmbulos de contexto ("Contexto (…", "Você é…")
          const lines = txt.split("\n").map(l => l.trim()).filter(Boolean);
          const pick = lines.find(l => !/^contexto\b|^context\b|^você é|^voce e|^you are/i.test(l)) || lines[0] || "";
          c.label = pick.replace(/\s+/g, " ").slice(0, 64);
        }
      }
    }
    c.size = st.size;
    agentTok.set(path, c);
    while (agentTok.size > 64) agentTok.delete(agentTok.keys().next().value);
  }
  return { ...c, mtime: st.mtimeMs, birth: st.birthtimeMs || st.mtimeMs };
}
function readAgents(dir, journal){
  let files = []; try { files = readdirSync(dir).filter(f => /^agent-[0-9a-f]+\.jsonl$/.test(f)); } catch { return []; }
  const out = [];
  for (const f of files) {
    const id = f.slice(6, -6);
    const s = agentStats(`${dir}/${f}`);
    if (!s) continue;
    let agentType = "";
    try { agentType = JSON.parse(readFileSync(`${dir}/agent-${id}.meta.json`, "utf8")).agentType || ""; } catch {}
    // sem journal (agente avulso do Agent tool): rodando = escreveu há <2min
    const done = journal ? journal.done.has(id) : (Date.now() - s.mtime > 120000);
    out.push({ id, label: s.label || agentType || id.slice(0, 8), agentType, model: shortModel(s.model),
               tokens: s.tokens, startedAt: Math.round(s.birth), lastTs: Math.round(s.mtime), running: !done });
  }
  return out.sort((a, b) => a.startedAt - b.startedAt);
}
function sessionTasks(sessionId){
  const now = Date.now();
  const hit = taskCache.get(sessionId);
  if (hit && now - hit.at < 3000) return hit.data;
  const data = { runs: [], running: 0 };
  const t = sessionId ? findTranscript(sessionId) : "";
  if (t) {
    const base = t.slice(0, -6);   // dir de artefatos da sessão (mesmo nome sem .jsonl)
    const scripts = {};
    try { for (const f of readdirSync(`${base}/workflows/scripts`)) { const m = f.match(/^(.+)-(wf_[a-z0-9-]+)\.js$/); if (m) scripts[m[2]] = { name: m[1], file: `${base}/workflows/scripts/${f}` }; } } catch {}
    let runIds = []; try { runIds = readdirSync(`${base}/subagents/workflows`); } catch {}
    for (const runId of runIds) {
      const dir = `${base}/subagents/workflows/${runId}`;
      const journal = { started: new Set(), done: new Set() };
      try { for (const line of readFileSync(`${dir}/journal.jsonl`, "utf8").split("\n")) {
        if (!line.trim()) continue;
        let j; try { j = JSON.parse(line); } catch { continue; }
        if (j.type === "started") journal.started.add(j.agentId); else if (j.type === "result") journal.done.add(j.agentId);
      } } catch {}
      const agents = readAgents(dir, journal);
      if (!agents.length) continue;
      const sc = scripts[runId];
      // F1: fonte RICA quando existe — workflows/wf_<runId>.json (workflowProgress: label real do
      // agent(), fase por agente, model, tokens). O journal continua mandando no "vivo" (started sem
      // result = rodando): o wf.json pode estar defasado num run em andamento — merge, não substituição.
      let wf = null;
      try { wf = JSON.parse(readFileSync(`${base}/workflows/${runId}.json`, "utf8")); } catch {}
      const wfAgents = new Map();
      for (const it of wf?.workflowProgress || [])
        if (it.type === "workflow_agent" && it.agentId) wfAgents.set(it.agentId, it);
      for (const a of agents) {
        const w = wfAgents.get(a.id);
        if (!w) continue;
        if (w.label) a.label = String(w.label).slice(0, 80);
        if (w.phaseTitle) a.phase = String(w.phaseTitle).slice(0, 60);
        if (w.model) a.model = shortModel(String(w.model));
        const wt = Number(w.tokens) || 0;
        if (wt > a.tokens) a.tokens = wt;
      }
      let description = "", phases = [];
      if (Array.isArray(wf?.phases)) phases = wf.phases.map(p => String(p?.title || "")).filter(Boolean).slice(0, 12);
      const src0 = sc ? (() => { try { return readFileSync(sc.file, "utf8").slice(0, 4000); } catch { return ""; } })()
                      : String(wf?.script || "").slice(0, 4000);
      if (src0) {
        description = src0.match(/description:\s*['"]([^'"]+)['"]/)?.[1] || "";
        if (!phases.length) phases = [...src0.matchAll(/title:\s*['"]([^'"]+)['"]/g)].map(m => m[1]).slice(0, 12);
      }
      // progresso por fase (só quando o wf.json deu fase por agente — senão o app segue nos chips simples)
      const phaseProgress = phases.map(title => {
        const list = agents.filter(a => a.phase === title);
        return { title, total: list.length, done: list.filter(a => !a.running).length };
      }).filter(p => p.total > 0);
      const running = agents.filter(a => a.running).length;
      data.runs.push({ runId, name: sc?.name || wf?.workflowName || runId, description: description.slice(0, 160), phases,
                       phaseProgress: phaseProgress.length ? phaseProgress : undefined,
                       agents, done: agents.length - running, total: agents.length,
                       startedAt: Math.min(...agents.map(a => a.startedAt)), lastTs: Math.max(...agents.map(a => a.lastTs)) });
    }
    const solo = readAgents(`${base}/subagents`, null);
    if (solo.length) {
      const running = solo.filter(a => a.running).length;
      data.runs.push({ runId: "", name: "Agentes", description: "", phases: [], agents: solo,
                       done: solo.length - running, total: solo.length,
                       startedAt: Math.min(...solo.map(a => a.startedAt)), lastTs: Math.max(...solo.map(a => a.lastTs)) });
    }
    data.runs.sort((a, b) => b.startedAt - a.startedAt);
    data.running = data.runs.reduce((n, r) => n + (r.total - r.done), 0);
  }
  taskCache.set(sessionId, { at: now, data });
  while (taskCache.size > 16) taskCache.delete(taskCache.keys().next().value);
  return data;
}

// status honesto p/ a UI: um "busy" cujo transcript E json não mudam há 2min está TRAVADO/velho, não
// trabalhando de verdade → reporta "idle" (senão o app fica com o spinner "Claude trabalhando…" preso).
function displayStatus(meta, alive){
  if (!alive) return "dead";
  let s = meta.status || "idle";
  if (s === "busy") {
    let fresh = meta.updatedAt || 0;
    const p = findTranscript(meta.sessionId || "");
    if (p) { try { fresh = Math.max(fresh, statSync(p).mtimeMs); } catch {} }
    if (Date.now() - fresh > 120000) s = "idle";   // 2min sem escrever nada = não está trabalhando
  }
  return s;
}
function cliSessions(){
  let files = []; try { files = readdirSync(CLI_SESS_DIR).filter(f => f.endsWith(".json")); } catch { return []; }
  // children do PRÓPRIO bridge (claude -p das sessões) também se registram em ~/.claude/sessions
  // e apareciam DUPLICADOS na lista ("xneog-cli-f3" ao lado da sessão xneog-cli) — a sessão já
  // está na lista principal como dirigível; o espelho é só pra processos de FORA.
  const own = new Set([...sessions.values()].map(S => S.child?.pid).filter(Boolean));
  const out = [];
  for (const f of files) {
    let j; try { j = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${f}`, "utf8")); } catch { continue; }
    if (!j || !j.pid) continue;
    if (own.has(j.pid)) continue;
    // headless dos loops/crons (workspace em ~/.config/xneog-*) — transiente e não-dirigível: só polui a lista
    if (String(j.cwd || "").includes("/.config/xneog-")) continue;
    const alive = pidAlive(j.pid);
    // injetável só se o processo vivo é O MESMO da sessão registrada (start-time bate) — mesmo gate anti-RCE
    const valid = alive && startMatches(j.pid, j.startedAt);
    const tty = valid ? sessionTty(j.pid) : null;                 // sessão interativa num terminal → TTY
    const sock = valid && !tty ? sockForJob(j.jobId) : null;      // bg job → pty.sock (input = bytes crus)
    const driveVia = tty ? "tty" : sock ? "socket" : null;        // o app decide a UI por aqui
    // apelido do usuário ganha do nome derivado (toda sessão aberta no HOME nasce com o mesmo nome)
    const alias = cliNames[cliNameKey(j)];
    out.push({ pid: j.pid, sessionId: j.sessionId || "", name: alias || j.name || `pid ${j.pid}`, cwd: j.cwd || "",
               alias: !!alias,
               kind: j.kind || "", status: displayStatus(j, alive), connected: alive,
               startedAt: j.startedAt || 0, lastTs: j.updatedAt || j.startedAt || 0,
               remote: !!j.bridgeSessionId, driveable: false, injectable: !!driveVia, driveVia });
  }
  return out.sort((a, b) => b.lastTs - a.lastTs);
}

// push curado → feed Atualizações do app (que já notifica briefs novos) · throttle por sessão+tipo
function notify(S, type, title, summary, body, extra = {}){
  const now = Date.now();
  S.lastNotify = S.lastNotify || {};
  // APROVAÇÃO NUNCA É ENGOLIDA: o throttle de 30s/tipo existe pra não spammar "done", mas dois pedidos
  // seguidos são o caso NORMAL de um turno — o 2º sumia e morria no timeout de 120s sem ninguém ver.
  if (type !== "approval" && now - (S.lastNotify[type] || 0) < 30000) return;
  S.lastNotify[type] = now;
  fetch("http://127.0.0.1:8801/brief", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${KEY}` },
    // sid/requestId viajam junto: sem eles a notificação não tinha como abrir A sessão certa
    body: JSON.stringify({ title, summary, body, silent: false, sid: S.id, kind: type, ...extra }),
    signal: AbortSignal.timeout(3000),   // BFF pendurado não acumula socket/promise por 5min (default undici)
  }).catch(() => {});
}

// ── Item 7 (23-jul): notificação de "turno completo" p/ sessão CLI injetada do celular ──────────
// O bridge só OBSERVA sessões CLI (não as possui), então rastreia por pid: ao injetar, arma o watch;
// um loop de fundo detecta a transição busy→idle (turno terminou) e empurra um /brief (o BFF vira
// push/APNs + feed). Sem isso, você injeta do celular, fecha o app, e não sabe quando o Claude terminou.
const cliWatch = new Map();   // pid → { armedAt, sawBusy, name, lastNotify }
function armCliWatch(pid){
  let name = `pid ${pid}`;
  try { const m = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); name = m.name || (m.cwd || "").split("/").pop() || name; } catch {}
  const cur = cliWatch.get(pid) || {};
  cliWatch.set(pid, { armedAt: Date.now(), sawBusy: false, name, lastNotify: cur.lastNotify || 0 });
}
function notifyCli(name){
  fetch("http://127.0.0.1:8801/brief", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${KEY}` },
    body: JSON.stringify({
      title: `✅ Turno concluído · ${name}`,
      summary: `A sessão do terminal "${name}" terminou o turno.`,
      body: `Abra Código → ${name} pra revisar o resultado.`,
      silent: false,
    }),
    signal: AbortSignal.timeout(3000),
  }).catch(() => {});
}
// loop de fundo (3s): p/ cada pid armado, lê status. busy→sawBusy; sawBusy+idle (após >4s do inject,
// pra não notificar eco/turno instantâneo) → notifica e desarma (re-arma no próximo inject). Timeout 20min.
setInterval(() => {
  if (cliWatch.size === 0) return;
  const now = Date.now();
  for (const [pid, w] of cliWatch) {
    if (!pidAlive(pid)) { cliWatch.delete(pid); continue; }                         // sessão morreu
    if (now - w.armedAt > 20 * 60 * 1000) { cliWatch.delete(pid); continue; }       // timeout: nunca completou
    let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { continue; }
    const st = displayStatus(meta, true);
    if (st === "busy" || st === "running") { w.sawBusy = true; }
    else if (w.sawBusy && st === "idle" && now - w.armedAt > 4000) {
      if (now - (w.lastNotify || 0) > 30000) { notifyCli(w.name); w.lastNotify = now; }
      cliWatch.delete(pid);
    }
  }
}, 3000).unref();

// ── Live Activity (Ilha Dinâmica) ────────────────────────────────────────────
// O app inicia a atividade e registra o pushToken dela no BFF. Quando o app SUSPENDE, só o push
// atualiza a Ilha — por isso o bridge empurra as mudanças de FASE. O cronômetro NÃO vem por push
// (o widget conta sozinho com Text(timerInterval:)), então um throttle de 1.5s não a deixa atrasada.
const LA_THROTTLE_MS = 1500;
function laPush(S, { fase, tool = null, requerEntrada = false, event = "update" }){
  const agora = Date.now();
  if (event !== "end" && agora - (S.laLast || 0) < LA_THROTTLE_MS) return;
  S.laLast = agora;
  if (!S.laInicio) S.laInicio = new Date().toISOString();
  const state = { fase, tool, queued: S.queue ? S.queue.length : 0, inicio: S.laInicio, requerEntrada };
  fetch("http://127.0.0.1:8801/apns/liveactivity/update", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${KEY}` },
    body: JSON.stringify({ sid: S.id, state, event }),
    signal: AbortSignal.timeout(3000),
  }).catch(() => {});   // best-effort: a Ilha nunca pode derrubar o turno
}

function resolveApproval(requestId, approved, by, always = false, reason = ""){
  const P = pending.get(requestId); if (!P) return false;
  pending.delete(requestId); clearTimeout(P.timer);
  const S = sessions.get(P.sid);
  // `always` guarda só o NOME da tool → aprovar UM `Bash: ls` com "sempre" auto-aprovaria todo Bash
  // seguinte, inclusive `rm -rf` que o owner nunca viu. Tools de alto risco NUNCA entram em always
  // (enforce server-side, não só no app): a fila continua pedindo comando a comando. Mesma regra do bulk.
  if (approved && always && S && !NEVER_ALWAYS.has(P.tool)) S.always.add(P.tool);
  // deny com motivo: o `message` volta pro claude como resultado da tool — sem isso o agente segue cego.
  const denyMsg = (reason && reason.trim()) ? `Negado pelo owner. Motivo: ${reason.trim().slice(0, 500)}` : `negado pelo owner no app (${by})`;
  const decision = approved ? { behavior: "allow", updatedInput: P.input } : { behavior: "deny", message: denyMsg };
  if (P.cb) { try { P.cb(decision); } catch {} }   // F4: engine api espera numa Promise, não num res HTTP
  else try { P.res.writeHead(200, JSONH); P.res.end(JSON.stringify(decision)); } catch {}
  if (S) push(S, { kind: "permission_resolved", requestId, approved, by });
  audit({ act: "approval", requestId, sid: P.sid, tool: P.tool, approved, by, always });
  return true;
}

function log(m){ try { appendFileSync(`${LOGDIR}/bridge.log`, `${new Date().toISOString()} ${m}\n`); } catch {} }
function audit(o){ try { appendFileSync(AUDIT, JSON.stringify({ ts: new Date().toISOString(), ...o }) + "\n"); } catch {} }
const MAX_TTL_MS = 10 * 60 * 1000;   // TTL prometido = 10min; o app cunha 10min. (era 20 — o servidor não impunha o limite)
const CLOCK_SKEW_MS = 60 * 1000;     // tolera relógio do device adiantado até 1min (senão rejeitaria token legítimo no limite)
const DEVICES_FILE = `${DIR}/devices.json`;
let _devCache = { mtime: 0, map: {} };
function devices(){
  try { const st = statSync(DEVICES_FILE); if (st.mtimeMs !== _devCache.mtime) { _devCache = { mtime: st.mtimeMs, map: JSON.parse(readFileSync(DEVICES_FILE, "utf8")) }; } } catch { _devCache = { mtime: 0, map: {} }; }
  return _devCache.map;
}
// token por-device: "v2.<deviceId>.<expiryMs>.<hmacHex>" · hmac = HMAC(deviceSecret, "deviceId.expiry").
// Secret vive só no Mac (devices.json) e no device. Revogar = flag revoked (instantâneo, sem rebuild).
function validDevice(tok){
  const m = /^v2\.([a-z0-9-]{1,40})\.(\d+)\.([0-9a-f]{64})$/.exec(tok || ""); if (!m) return false;
  const [, deviceId, expStr, hmac] = m;
  const exp = Number(expStr), now = Date.now();
  if (exp < now || exp > now + MAX_TTL_MS + CLOCK_SKEW_MS) return false;   // CF Access é a defesa de borda; aqui o TTL curto limita replay a ≤10min
  const dev = devices()[deviceId]; if (!dev || dev.revoked || !dev.secret) return false;
  const want = createHmac("sha256", dev.secret).update(`${deviceId}.${expStr}`).digest("hex");
  try { return timingSafeEqual(Buffer.from(want), Buffer.from(hmac)); } catch { return false; }
}
function eq(tok, secret){ try { const a = Buffer.from(tok), b = Buffer.from(secret); return a.length === b.length && timingSafeEqual(a, b); } catch { return false; } }
function auth(req){
  const tok = (req.headers["authorization"] || "").replace(/^Bearer\s+/i, "");
  return eq(tok, KEY) || validDevice(tok);   // Bearer estático (curl/simulador) OU token por-device
}
// `max` por rota: /message carrega anexos (imagem em base64) e precisa de folga.
// Excedeu → resolve null (o caller responde 413). Antes destruía o socket: o cliente via
// "connection reset" sem saber o motivo.
function readBody(req, max = 1 << 20){
  return new Promise(r => {
    let d = "", over = false;
    req.on("data", c => { if (over) return; d += c; if (d.length > max) { over = true; d = ""; } });
    req.on("end", () => r(over ? null : d));
    req.on("error", () => r(null));
  });
}
const MAX_IMAGES = 4;
const MAX_IMAGE_B64 = 7 * 1024 * 1024;   // ~5MB de imagem depois do decode
const OK_MEDIA = new Set(["image/jpeg", "image/png", "image/webp", "image/gif"]);
const MAX_QUEUE = 8;
// MAX_QUEUE limita a CONTAGEM, não os bytes: 8 mensagens com 4 imagens (~28MB base64) cada = ~224MB em
// RAM por sessão até o turn_end drenar, × várias sessões = >1GB. Teto cumulativo de bytes de anexo na fila.
const MAX_QUEUE_BYTES = 40 << 20;
const itemBytes = it => (it.content || []).reduce((n, c) => n + (c.source?.data?.length || c.text?.length || 0), 0);
// "Aceitar edições": libera a família inteira de tools que escrevem arquivo, de uma vez.
// Bash e o resto continuam passando pela fila de aprovação — essa é a fronteira da ponte.
const EDIT_TOOLS = ["Edit", "MultiEdit", "Write", "NotebookEdit"];
// Nunca entram no allowlist "always" (nem por lote): a razão da ponte existir é a fila segurar Bash & cia.
const NEVER_ALWAYS = new Set(["Bash", "KillShell", "KillBash", "BashOutput"]);

// ── Bash de LEITURA (24-jul, decisão baseada em dados) ───────────────────────
// Telemetria de 121 aprovações reais: 87 eram Bash e 53% delas pura inspeção (grep/ls/cat/wc/head),
// com 2,5% de negativas e mediana de 4s de espera — a fila cobrava pedágio em quem só olhava.
// No modo acceptEdits (que JÁ auto-aprova escrita de arquivo) leitura passar é mais coerente, não
// menos seguro. Allowlist ESTRITA: verbo desconhecido, redirecionamento, subshell, background,
// sudo/eval/xargs ou flag destrutiva → cai na fila como antes. Auditado como by:"auto-read".
const READ_VERBS = new Set(["ls", "cat", "head", "tail", "wc", "grep", "rg", "find", "stat", "file",
  "du", "df", "pwd", "echo", "which", "type", "date", "uname", "hostname", "whoami", "ps", "lsof",
  "sort", "uniq", "cut", "basename", "dirname", "realpath", "printenv", "tree", "column", "diff",
  "cmp", "shasum", "md5", "jq", "sips", "plutil", "defaults"]);
const GIT_READ = new Set(["status", "log", "diff", "show", "branch", "rev-parse", "ls-files", "blame", "describe"]);
function isReadOnlyBash(cmd){
  const s = String(cmd || "").trim();
  if (!s || s.length > 400) return false;
  const t = s
    .replace(/\s*&>\s*\/dev\/null/g, "")            // descartar saída não é escrever em lugar nenhum
    .replace(/\s*\d?>\s*\/dev\/null/g, "")
    .replace(/\s*2>&1/g, "")
    .replace(/&&/g, ";");                           // encadeamento sequencial é ok; background não
  if (/&/.test(t)) return false;                    // `cmd &` roda solto, fora do timeout
  if (/[>`]|\$\(|<\(/.test(t)) return false;        // redireciona (escreve) ou executa em subshell
  if (/\b(sudo|eval|exec|xargs|source)\b/.test(t)) return false;
  const segs = t.split(/[;|]/).map(x => x.trim()).filter(Boolean);
  if (!segs.length || segs.length > 6) return false;
  for (const seg of segs) {
    const parts = seg.split(/\s+/);
    const verb = (parts[0] || "").split("/").pop();
    if (verb === "cd") continue;                    // prefixo comum: `cd dir; ls`
    if (verb === "git") {                           // `git -C <path> log` é o formato real do dia a dia
      const args = parts.slice(1);
      let i = 0;
      while (i < args.length && args[i].startsWith("-")) i += (args[i] === "-C" || args[i] === "-c") ? 2 : 1;
      if (!GIT_READ.has(args[i])) return false;
      continue;
    }
    if (!READ_VERBS.has(verb)) return false;
    if (verb === "find" && /\s-(delete|exec|execdir|ok|okdir)\b/.test(seg)) return false;
    if (verb === "defaults" && !/\s(read|read-type|domains|find)\b/.test(seg)) return false;
    if (verb === "plutil" && !/\s-(p|lint|convert)\b/.test(seg)) return false;
  }
  return true;
}
// leitura auto-aprovada SÓ em acceptEdits (opt-in explícito do dono); default segue pedindo tudo
function autoReadOK(S, tool, input){
  return S.permissionMode === "acceptEdits" && tool === "Bash" && isReadOnlyBash(input?.command);
}

// ── FILA DE MENSAGENS (serialização obrigatória) ─────────────────────────────
// Verificado 09-jul: escrever um `user` no stdin do claude DURANTE um turno **aborta o turno**.
// Prova: turno com tool_use (Glob) + 1 mensagem no meio → um único turn_end, respondendo só
// a mensagem nova; a resposta original SUMIU. (Um probe anterior deu falso positivo porque a
// mensagem chegou 391ms antes do fim natural do turno — corrida, não fila.)
//
// Logo: o bridge é o ÚNICO lugar onde a serialização pode ser garantida. Mensagem que chega com
// a sessão `running` entra em S.queue e só vai pro stdin no turn_end seguinte.
function dispatch(S, item){
  if (S.engine === "grok") return grokTurn(S, item);   // F2: turn-based, spawn por turno na jaula
  if (S.engine === "api") return apiTurn(S, item);     // F4: loop agentic próprio via chat-api
  if (!S.child || !S.child.stdin?.writable) {   // processo morreu entre o enqueue e o dispatch
    log(`[${S.id}] dispatch sem processo — mensagem descartada`);
    S.status = "dead";
    return;
  }
  S.turns += 1;
  S.lastTs = Date.now();
  S.laInicio = new Date().toISOString();   // cronômetro da Ilha começa aqui
  S.laLast = 0;
  laPush(S, { fase: "trabalhando" });
  push(S, { kind: "user", text: item.label, images: item.nImg, via: item.via || "" });
  try {
    S.child.stdin.write(JSON.stringify({ type: "user", message: { role: "user", content: item.content } }) + "\n");
  } catch (e) { log(`[${S.id}] stdin write falhou: ${e.message}`); }
}

/// Descarta a fila (interrupt / respawn / revive) e avisa os clientes.
function clearQueue(S, reason){
  if (!S.queue?.length) return;
  const n = S.queue.length;
  S.queue = [];
  push(S, { kind: "queue_cleared", dropped: n, reason });
  log(`[${S.id}] fila descartada (${n}) · ${reason}`);
}
// Nega toda aprovação pendente da sessão. Sem isto, na morte/respawn do child as pendentes ficavam no
// Map até o timeout de 120s: o badge "requer entrada" seguia aceso numa sessão morta/reiniciada, a Ilha
// travava em "aprovação", e cada entry retinha o `res` de um socket já fechado (o mcp-approval morreu
// junto). O /interrupt já fazia isso; agora exit/spawn_error/respawn/DELETE também.
function denyPending(S, reason){
  for (const [rid, P] of [...pending]) if (P.sid === S.id) resolveApproval(rid, false, reason);
}
// Slash-commands do Gomes (~/.claude/commands/*.md) p/ o menu do composer — adicionar um comando deixa
// de exigir rebuild do app. Só os comandos PRÓPRIOS (não as ~10 skills de plugin = ruído no telefone).
// Cache 30s (o menu abre raramente). O claude -p do bridge resolve "/update" como texto; a injeção TTY
// o entrega ao TUI que o interpreta nativamente — cobertura nos dois canais sem nada novo.
const COMMANDS_DIR = `${HOME}/.claude/commands`;
let _cmdCache = { at: 0, list: [] };
function listCommands(){
  const now = Date.now();
  if (now - _cmdCache.at < 30000) return _cmdCache.list;
  let list = [];
  try { list = readdirSync(COMMANDS_DIR).filter(f => f.endsWith(".md")).map(f => "/" + f.slice(0, -3)).sort(); } catch {}
  _cmdCache = { at: now, list };
  return list;
}
// Menu estilo bot do Telegram (digitar "/" no app abre lista nome+descrição): built-ins do Claude Code
// CURADOS pro celular — só one-shot, nada que abra picker interativo do TUI (/config, /resume, /rewind…).
// scope "cli" = injeta no TTY (o TUI resolve nativo); custom = "both" (o -p também resolve como prompt).
// Curadoria 24-jul sobre a lista do bundle v2.1.219 + estudo claude-code-guide (programmatic por comando).
const BUILTIN_MENU = [
  { cmd: "/compact", desc: "Compacta o contexto e libera espaço" },
  { cmd: "/clear",   desc: "Zera a conversa (memória e CLAUDE.md ficam)" },
  { cmd: "/model",   desc: "Troca o modelo da sessão", args: ["sonnet", "opus", "fable", "haiku"] },
  { cmd: "/effort",  desc: "Nível de raciocínio", args: ["low", "medium", "high", "xhigh", "max"] },
  { cmd: "/status",  desc: "Conta, modelo e estado da sessão" },
  { cmd: "/context", desc: "Uso do contexto (tokens)" },
  { cmd: "/cost",    desc: "Custo e duração da sessão" },
  { cmd: "/usage",   desc: "Limites do plano" },
  { cmd: "/todos",   desc: "Tarefas do turno atual" },
  { cmd: "/tasks",   desc: "Trabalho em background e subagentes" },
];
function commandMenu(){
  return [
    ...listCommands().map(c => ({ cmd: c, desc: "Comando do Gomes", scope: "both" })),
    ...BUILTIN_MENU.map(m => ({ ...m, scope: "cli" })),
  ];
}

// Projetos pro menu "+" do app — ~/Projects (nível 1) + Vorcaro-LiquidGlass, ordenados por
// atividade (mtime desc, top 12). Antes era lista FIXA no app, que envelhecia a cada projeto novo.
let _projCache = { at: 0, list: [] };
function listProjects(){
  const now = Date.now();
  if (now - _projCache.at < 60000) return _projCache.list;
  const home = homedir();
  const roots = [];
  try {
    const base = home + "/Projects";
    for (const name of readdirSync(base)) {
      if (name.startsWith(".") || name === "node_modules") continue;
      const p = base + "/" + name;
      try { const st = statSync(p); if (st.isDirectory()) roots.push({ name, path: p, mtime: st.mtimeMs }); } catch {}
    }
  } catch {}
  try {
    const p = home + "/Vorcaro-LiquidGlass";
    roots.push({ name: "Vorcaro-LiquidGlass", path: p, mtime: statSync(p).mtimeMs });
  } catch {}
  roots.sort((a, b) => b.mtime - a.mtime);
  const list = roots.slice(0, 12).map(({ name, path }) => ({ name, path }));
  list.push({ name: "Projects (raiz)", path: home + "/Projects" });
  _projCache = { at: now, list };
  return list;
}

// ── eventos curados pro app (o stdout cru do claude é ruidoso demais) ──
function curate(ev){
  const t = ev.type;
  if (t === "system" && ev.subtype === "init") return { kind: "init", model: ev.model, cwd: ev.cwd, claudeSession: ev.session_id };
  if (t === "stream_event") {
    const e = ev.event || {};
    if (e.type === "content_block_delta" && e.delta?.type === "text_delta") return { kind: "delta", text: e.delta.text };
    if (e.type === "content_block_delta" && e.delta?.type === "thinking_delta") return null;   // pensamento não vai pro app no F0
    return null;
  }
  if (t === "assistant") {
    const out = [];
    for (const c of ev.message?.content || []) {
      if (c.type === "tool_use") {
        const inp = c.input ?? {};
        // Subagentes/workflows = card nomeado no app (paridade com a view CLI, F1-subagentes 24-jul)
        if (c.name === "Task" || c.name === "Agent") {
          out.push({ kind: "task", toolId: c.id, tool: c.name,
                     name: String(inp.description || inp.subagent_type || "subagente").slice(0, 80),
                     agentType: String(inp.subagent_type || "") });
        } else if (c.name === "Workflow") {
          const script = String(inp.script || "");
          out.push({ kind: "task", toolId: c.id, tool: "Workflow",
                     name: (script.match(/name:\s*['"]([^'"]+)['"]/)?.[1] || String(inp.name || "workflow")).slice(0, 80),
                     desc: (script.match(/description:\s*['"]([^'"]+)['"]/)?.[1] || "").slice(0, 160) });
        } else out.push({ kind: "tool_use", toolId: c.id, tool: c.name, input: JSON.stringify(inp).slice(0, TOOL_RESULT_CAP) });
      }
      if (c.type === "text" && c.text) out.push({ kind: "text", text: c.text });
    }
    return out.length ? out : null;
  }
  if (t === "user") {
    const out = [];
    for (const c of ev.message?.content || []) {
      if (c.type === "tool_result") {
        let body = typeof c.content === "string" ? c.content : (c.content || []).map(x => x.text || "").join("\n");
        out.push({ kind: "tool_result", toolId: c.tool_use_id, isError: !!c.is_error, output: String(body).slice(0, TOOL_RESULT_CAP) });
      }
    }
    return out.length ? out : null;
  }
  if (t === "result") return { kind: "turn_end", ok: ev.subtype === "success", costUsd: ev.total_cost_usd ?? null, durationMs: ev.duration_ms ?? null };
  return null;
}

function push(S, obj){
  if (LIST_KINDS.has(obj.kind)) notifySessions();
  // `i` é uma SEQUÊNCIA monotônica, nunca events.length: depois do primeiro splice (MAX_EVENTS) o
  // length trava em 4000 e todo evento novo nasceria com i=4000 — replay `?from=N` nunca mais casa
  // e o ForEach do SwiftUI recebe ids duplicados.
  const item = { i: S.seq++, ts: Date.now(), ...obj };
  // deltas são EFÊMEROS: streamados ao vivo mas NÃO persistidos no buffer. Um turno verboso gera milhares
  // — persistir evictava user/tool_use/turn_end (o replay perdia turnos inteiros) e dobrava memória (o
  // delta + o kind:"text" consolidado que chega no fim do bloco). O seq segue monotônico (cursor ao vivo
  // intacto); o replay entrega o texto consolidado, com buracos de seq — ok, o cliente usa `i` só como
  // cursor/id, nunca assume contiguidade. (O /transcript já ignorava deltas.)
  // efêmeros: broadcast ao vivo, fora do buffer/replay/transcript (delta = streaming; presence = quem está olhando)
  if (obj.kind !== "delta" && obj.kind !== "presence") {
    S.events.push(item);
    transAppend(S.id, item);   // F3: persistência por evento (replay sobrevive a restart)
    if (S.events.length > MAX_EVENTS) S.events.splice(0, S.events.length - MAX_EVENTS);
  }
  const line = `id: ${item.i}\ndata: ${JSON.stringify(item)}\n\n`;
  // Item 6 (backpressure, 23-jul): num link lento (4G) o buffer do socket enche; escrever milhares de
  // deltas de um turno verboso sem olhar o retorno de write() bufferiza SEM TETO no Node. Delta é
  // efêmero → DROPA pra socket atrasado (o kind:"text" consolidado chega no fim do bloco, sem perda);
  // evento importante (user/tool_use/tool_result/turn_end, replayável) SEMPRE vai. Retoma delta no drain.
  const isDelta = obj.kind === "delta";
  for (const res of S.subs) {
    if (isDelta && res._xnBehind) continue;
    let ok = false;
    try { ok = res.write(line); } catch { continue; }
    if (!ok && !res._xnBehind) { res._xnBehind = true; res.once("drain", () => { res._xnBehind = false; }); }
  }
}

const MODELS = new Set(["sonnet", "opus", "haiku", "fable"]);
// Modos expostos ao app. `bypassPermissions` NÃO entra: ele auto-aprova Bash e mata a fila de
// aprovação no telefone, que é a razão de existir desta ponte. "Modo automático" do app oficial
// não tem equivalente aqui, de propósito.
//   default     → toda tool com efeito colateral cai na fila (aprovar/negar no iPhone)
//   acceptEdits → Edit/Write auto-aprovados; Bash e o resto seguem na fila
//   plan        → Claude planeja e não executa nada
const MODES = new Set(["default", "acceptEdits", "plan"]);

// ── F2: engines/modelos — registry declarativo (padrão Kimi: editar engines.json, sem tocar código) ──
// Engine "grok" = 2ª classe de sessão: turn-based (1 spawn por turno, resume via -r <sessionId>),
// SEMPRE dentro da jaula Seatbelt grok-jail.sb + cwd em ~/GrokWork/<id> (exfiltração provada jul-26:
// o processo não enxerga NADA do usuário fora da jail — validado adversarialmente 24-jul, "Permission
// denied" ao tentar ler repo). Sem fila de aprovação: --always-approve, a parede é a jaula.
const GROK_BIN = `${HOME}/.grok/bin/grok`;
const GROK_JAIL_ROOT = `${HOME}/GrokWork`;
const GROK_SB = `${DIR}/grok-jail.sb`;
const ENGINES_FILE = `${DIR}/engines.json`;
let _engCache = { mtime: -1, data: null };
function engineRegistry(){
  const builtin = {
    claude: { label: "Claude Code", models: [...MODELS], trusted: true, default: true },
    grok:   { label: "Grok (jaula)", models: ["default"], trusted: false, jail: GROK_JAIL_ROOT,
              available: process.platform === "darwin" && existsSync(GROK_BIN) && existsSync(GROK_SB),
              notes: "pesquisa/2ª opinião · enxerga SÓ ~/GrokWork/<sessão> (Seatbelt) · sem aprovação (a parede é a jaula)" },
    api:    { label: "Claude API (key)", models: ["sonnet", "haiku", "opus", "fable"], trusted: true, metered: true,
              available: !!AGENT_KEY,
              notes: "F4: loop agentic próprio via chat-api :3848 — key nunca sai do chat-api · metering sqlite · fila de aprovação igual claude · caminho multi-tenant" },
  };
  try {
    const st = statSync(ENGINES_FILE);
    if (_engCache.mtime === st.mtimeMs && _engCache.data) return _engCache.data;
    const j = JSON.parse(readFileSync(ENGINES_FILE, "utf8"));
    const merged = { ...builtin };
    for (const [k, v] of Object.entries(j)) if (v && typeof v === "object") merged[k] = { ...builtin[k], ...v };
    _engCache = { mtime: st.mtimeMs, data: merged };
    return merged;
  } catch { return builtin; }
}

// Um turno grok: spawn sob sandbox-exec, prompt por ARQUIVO 0600 na jail (argv vaza no ps), NDJSON
// {thought|text|end} → eventos curados do bridge. `end.sessionId` vira o -r do próximo turno.
function grokTurn(S, item){
  S.turns += 1; S.lastTs = Date.now();
  S.laInicio = new Date().toISOString(); S.laLast = 0;
  laPush(S, { fase: "trabalhando" });
  push(S, { kind: "user", text: item.label, images: item.nImg, via: item.via || "" });
  const text = item.content?.find?.(c => c.type === "text")?.text || item.label || "";
  const pf = `${S.cwd}/.prompt-${Date.now()}.txt`;
  try { writeFileSync(pf, text, { mode: 0o600 }); }
  catch (e) {
    log(`[${S.id}] grok prompt-file err ${e.message}`);
    S.status = "idle";
    return push(S, { kind: "turn_end", ok: false, queued: S.queue.length, next: false });
  }
  const args = ["-D", `JAIL=${S.cwd}`, "-f", GROK_SB, GROK_BIN,
                "--prompt-file", pf, "--output-format", "streaming-json", "--cwd", S.cwd, "--always-approve"];
  if (S.model && S.model !== "default") args.push("-m", S.model);
  if (S.grokSession) args.push("-r", S.grokSession);
  const child = spawn("/usr/bin/sandbox-exec", args, { cwd: S.cwd, env: { ...process.env, PATH: PATHENV } });
  S.child = child;
  let acc = "";
  createInterface({ input: child.stdout }).on("line", (l) => {
    let j; try { j = JSON.parse(l); } catch { return; }
    if (j.type === "text" && j.data) { acc += j.data; push(S, { kind: "delta", text: j.data }); }
    else if (j.type === "end") { if (j.sessionId) S.grokSession = j.sessionId; }
    else if (j.type && j.type !== "thought") {
      // tool_call e afins: melhor esforço — vira linha de tool no app (vocabulário do grok evolui)
      push(S, { kind: "tool_use", toolId: String(j.id || `g${Date.now()}`), tool: `grok:${j.name || j.tool || j.type}`,
                input: JSON.stringify(j).slice(0, TOOL_RESULT_CAP) });
    }
  });
  const finish = (ok) => {
    try { unlinkSync(pf); } catch {}
    if (S.child === child) S.child = null;
    if (acc) push(S, { kind: "text", text: acc });
    S.status = "idle"; S.lastTurnEndTs = Date.now(); S.lastTs = Date.now();
    const nxt = S.queue.length > 0;
    push(S, { kind: "turn_end", ok, queued: S.queue.length, next: nxt });
    laPush(S, { fase: "fim", event: "end" });
    if (!nxt && ok) notify(S, "done", `✅ Grok respondeu · ${S.title}`, "Turno concluído.", `A sessão grok "${S.title}" terminou. Abra Código pra revisar.`);
    saveSessions();
    if (nxt) { S.status = "running"; grokTurn(S, S.queue.shift()); }
  };
  child.on("exit", (code) => finish(code === 0));
  child.on("error", (e) => { log(`[${S.id}] grok spawn err ${e.message}`); finish(false); });
}

// ── F4: engine "api" — loop agentic MÍNIMO próprio (fallback existencial + caminho vendável) ──
// Tool use direto na Messages API via chat-api (a key nunca chega aqui). Mesmos eventos curados,
// MESMA fila de aprovação do engine claude (waitApproval resolve Promise em vez de res HTTP).
// Nomes de tool = vocabulário do app (Bash/Read/Write/Glob → verbos e diff prontos na UI).

// aprovação interna: mesma fila/eventos/timeout do MCP, resolvendo uma Promise
function waitApproval(S, tool, input){
  return new Promise((resolve) => {
    // modo AUTO (opt-in POR SESSÃO, decisão do Gomes 24-jul): a fila não pendura — o daemon aprova
    // na hora, com trilha de auditoria. Diferente de bypass: por sessão, auditado, revogável a quente.
    if (S.permissionMode === "auto") { audit({ act: "approval", sid: S.id, tool, approved: true, by: "auto" }); return resolve({ behavior: "allow", updatedInput: input }); }
    if (autoReadOK(S, tool, input)) { audit({ act: "approval", sid: S.id, tool, approved: true, by: "auto-read", cmd: String(input?.command || "").slice(0, 120) }); return resolve({ behavior: "allow", updatedInput: input }); }
    if (S.always.has(tool)) { audit({ act: "approval", sid: S.id, tool, approved: true, by: "always" }); return resolve({ behavior: "allow", updatedInput: input }); }
    if (pending.size >= MAX_PENDING) return resolve({ behavior: "deny", message: "fila de aprovação cheia" });
    const requestId = randomUUID();
    const timer = setTimeout(() => resolveApproval(requestId, false, "timeout"), APPROVAL_TIMEOUT_MS);
    pending.set(requestId, { cb: resolve, sid: S.id, tool, input, timer });
    S.laLast = 0;
    laPush(S, { fase: "aprovação", tool, requerEntrada: true });
    push(S, { kind: "permission_request", requestId, tool, input: capInput(input) });
    const cmd = (input && (input.command || input.file_path)) || "";
    notify(S, "approval", `⌘ Requer aprovação · ${S.title}`, `${tool}: ${String(cmd).slice(0, 120)}`, `Sessão "${S.title}" aguardando sua decisão (${tool}). Abra Código → ${S.title}. Sem resposta em 120s = negado.`, { requestId, tool });
  });
}

const API_MAX_ITERS = 20;
const API_MODEL_MAP = { "": "claude-sonnet-5", sonnet: "claude-sonnet-5", opus: "claude-opus-5", haiku: "claude-haiku-4-5-20251001", fable: "claude-fable-5" };
const apiModel = (S) => /^claude-/.test(S.model || "") ? S.model : (API_MODEL_MAP[S.model || ""] || "claude-sonnet-5");
const API_TOOLS = [
  { name: "Bash", description: "Executa um comando de shell no Mac (zsh, cwd da sessão). Timeout 120s.",
    input_schema: { type: "object", properties: { command: { type: "string" } }, required: ["command"] } },
  { name: "Read", description: "Lê um arquivo de texto (até 50KB por chamada; use offset pra continuar).",
    input_schema: { type: "object", properties: { file_path: { type: "string" }, offset: { type: "number" } }, required: ["file_path"] } },
  { name: "Write", description: "Escreve conteúdo num arquivo (cria ou sobrescreve; cria diretórios).",
    input_schema: { type: "object", properties: { file_path: { type: "string" }, content: { type: "string" } }, required: ["file_path", "content"] } },
  { name: "Glob", description: "Lista arquivos que casam um padrão (ex.: **/*.js) a partir de um diretório.",
    input_schema: { type: "object", properties: { pattern: { type: "string" }, path: { type: "string" } }, required: ["pattern"] } },
];
function apiSystem(S){
  return `Você é o agente de código da xNeog rodando no Mac do Gomes (macOS, zsh). Diretório de trabalho: ${S.cwd}. Responda em pt-BR, direto e conciso, sem emojis. Use as ferramentas para AGIR (Bash/Read/Write/Glob) em vez de descrever o que faria. Leia antes de escrever. Comandos e escritas passam pela aprovação do dono — se negado, explique e proponha alternativa.`;
}
function loadApiMessages(id){ try { return JSON.parse(readFileSync(`${TRANS_DIR}/${id}.messages.json`, "utf8")); } catch { return []; } }
function saveApiMessages(id, msgs){ try { writeFileSync(`${TRANS_DIR}/${id}.messages.json`, JSON.stringify(msgs), { mode: 0o600 }); } catch {} }
// contexto deslizante SEM quebrar par tool_use/tool_result: corta só em user de texto puro
function trimApiMessages(msgs){
  if (msgs.length <= 60) return msgs;
  let cut = msgs.length - 60;
  while (cut < msgs.length && !(msgs[cut].role === "user" && typeof msgs[cut].content === "string")) cut++;
  return cut < msgs.length ? msgs.slice(cut) : msgs.slice(-2);
}

async function runApiTool(S, name, input){
  const rel = (p) => { const s = String(p || ""); return s.startsWith("/") ? s : `${S.cwd}/${s}`; };
  if (name === "Read") {
    try {
      const p = rel(input.file_path);
      const st = statSync(p);
      const off = Math.max(0, Number(input.offset) || 0);
      const len = Math.min(50 * 1024, Math.max(0, st.size - off));
      const fd = openSync(p, "r"); const buf = Buffer.alloc(len);
      readSync(fd, buf, 0, len, off); closeSync(fd);
      const rest = st.size - off - len;
      return { text: buf.toString("utf8") + (rest > 0 ? `\n[... +${rest} bytes — continue com offset=${off + len}]` : "") };
    } catch (e) { return { text: `erro: ${e.message}`, isError: true }; }
  }
  if (name === "Glob") {
    try {
      const root = rel(input.path || ".");
      const pat = String(input.pattern || "*");
      const rx = new RegExp("^" + pat
        .replace(/[.+^${}()|[\]\\]/g, "\\$&")
        .replace(/\*\*\//g, "\u0001")   // **/ → qualquer prefixo de diretórios
        .replace(/\*\*/g, "\u0002")      // **  → qualquer coisa
        .replace(/\*/g, "[^/]*")
        .replace(/\?/g, ".")
        .replace(/\u0001/g, "(?:.*\/)?")
        .replace(/\u0002/g, ".*") + "$");
      const out = [];
      const walk = (d, depth) => {
        if (depth > 6 || out.length >= 200) return;
        let es = []; try { es = readdirSync(d, { withFileTypes: true }); } catch { return; }
        for (const e of es) {
          if (e.name.startsWith(".") || e.name === "node_modules") continue;
          const full = `${d}/${e.name}`;
          if (e.isDirectory()) { walk(full, depth + 1); continue; }
          const relp = full.slice(root.length + 1);
          if (rx.test(relp) || rx.test(e.name)) { out.push(relp); if (out.length >= 200) return; }
        }
      };
      walk(root, 0);
      return { text: out.length ? out.join("\n") : "(nenhum arquivo casou)" };
    } catch (e) { return { text: `erro: ${e.message}`, isError: true }; }
  }
  if (name === "Write") {
    const p = rel(input.file_path);
    const content = String(input.content ?? "");
    if (S.permissionMode !== "acceptEdits" && !S.always.has("Write")) {
      const d = await waitApproval(S, "Write", { file_path: p, content });
      if (d.behavior !== "allow") return { text: d.message || "negado", isError: true };
    }
    try {
      mkdirSync(p.split("/").slice(0, -1).join("/") || "/", { recursive: true });
      writeFileSync(p, content);
      return { text: `escrito: ${p} (${content.length} chars)` };
    } catch (e) { return { text: `erro: ${e.message}`, isError: true }; }
  }
  if (name === "Bash") {
    const cmd = String(input.command || "");
    const d = await waitApproval(S, "Bash", { command: cmd });   // Bash NUNCA entra em always (NEVER_ALWAYS)
    if (d.behavior !== "allow") return { text: d.message || "negado", isError: true };
    return await new Promise((resolve) => {
      const child = WIN
        ? spawn("cmd.exe", ["/d", "/s", "/c", cmd], { cwd: S.cwd, env: { ...process.env, PATH: PATHENV }, windowsVerbatimArguments: true })
        : spawn("/bin/zsh", ["-lc", cmd], { cwd: S.cwd, env: { ...process.env, PATH: PATHENV } });
      S.apiChild = child;   // interrupt do turno mata o Bash em voo junto
      let out = "";
      const cap = (dd) => { if (out.length < 200 * 1024) out += dd; };
      child.stdout.on("data", cap); child.stderr.on("data", cap);
      const to = setTimeout(() => { try { child.kill("SIGKILL"); } catch {} }, 120000);
      child.on("close", (code) => { clearTimeout(to); if (S.apiChild === child) S.apiChild = null; resolve({ text: (out || "(sem output)").slice(0, 30000) + (code ? `\n[exit ${code}]` : ""), isError: code !== 0 }); });
      child.on("error", (e) => { clearTimeout(to); if (S.apiChild === child) S.apiChild = null; resolve({ text: `erro: ${e.message}`, isError: true }); });
    });
  }
  return { text: `tool desconhecida: ${name}`, isError: true };
}

async function apiTurn(S, item){
  S.turns += 1; S.lastTs = Date.now();
  S.laInicio = new Date().toISOString(); S.laLast = 0;
  laPush(S, { fase: "trabalhando" });
  push(S, { kind: "user", text: item.label, images: item.nImg, via: item.via || "" });
  const userText = item.content?.find?.(c => c.type === "text")?.text || item.label || "";
  if (!S.apiMessages) S.apiMessages = loadApiMessages(S.id);
  S.apiMessages.push({ role: "user", content: userText });
  const ctl = new AbortController(); S.apiAbort = ctl; S.apiStop = false;
  let ok = true;
  try {
    for (let iter = 0; iter < API_MAX_ITERS && !S.apiStop; iter++) {
      S.apiMessages = trimApiMessages(S.apiMessages);
      const r = await fetch(`${CHAT_API}/v1/agent/messages`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${AGENT_KEY}` },
        body: JSON.stringify({ model: apiModel(S), max_tokens: 8192, system: apiSystem(S),
                               messages: S.apiMessages, tools: API_TOOLS, session: S.id }),
        signal: ctl.signal,
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j.error || j.type === "error") {
        push(S, { kind: "text", text: `⚠️ chat-api ${r.status}: ${(j.error && (j.error.message || j.error)) || "erro"}` });
        ok = false; break;
      }
      S.apiMessages.push({ role: "assistant", content: j.content || [] });
      const results = [];
      for (const b of j.content || []) {
        if (b.type === "text" && b.text) push(S, { kind: "text", text: b.text });
        else if (b.type === "tool_use") {
          push(S, { kind: "tool_use", toolId: b.id, tool: b.name, input: capInput(b.input) });
          const out = S.apiStop ? { text: "turno interrompido pelo owner", isError: true } : await runApiTool(S, b.name, b.input || {});
          push(S, { kind: "tool_result", toolId: b.id, output: String(out.text).slice(0, TOOL_RESULT_CAP), isError: !!out.isError });
          results.push({ type: "tool_result", tool_use_id: b.id, content: String(out.text).slice(0, 30000), is_error: !!out.isError });
        }
      }
      if (j.stop_reason !== "tool_use" || !results.length) break;
      S.apiMessages.push({ role: "user", content: results });
    }
  } catch (e) {
    if (e.name !== "AbortError") { push(S, { kind: "text", text: `⚠️ ${e.message}` }); }
    ok = false;
  }
  S.apiAbort = null; S.apiStop = false;
  saveApiMessages(S.id, S.apiMessages);
  S.status = "idle"; S.lastTurnEndTs = Date.now(); S.lastTs = Date.now();
  const nxt = S.queue.length > 0;
  push(S, { kind: "turn_end", ok, queued: S.queue.length, next: nxt });
  laPush(S, { fase: "fim", event: "end" });
  if (!nxt && ok) notify(S, "done", `✅ API respondeu · ${S.title}`, "Turno concluído.", `A sessão "${S.title}" (Claude API) terminou o turno. Abra Código pra revisar.`);
  saveSessions();
  if (nxt) { S.status = "running"; apiTurn(S, S.queue.shift()); }
}

function createSession({ cwd, title, model, permissionMode, engine }){
  if (liveCount() >= MAX_SESSIONS) return null;   // o cap é de PROCESSOS vivos; mortas ficam no histórico
  const id = randomUUID().slice(0, 8);
  if (engine === "grok") {
    if (!engineRegistry().grok.available) return null;
    const jail = `${GROK_JAIL_ROOT}/${id}`;
    try { mkdirSync(jail, { recursive: true, mode: 0o700 }); } catch { return null; }
    const S = { id, engine: "grok", child: null, cwd: jail, title: (title || "grok").slice(0, 60), status: "idle",
                events: [], subs: new Set(), turns: 0, lastTs: Date.now(), createdAt: Date.now(), always: new Set(),
                model: typeof model === "string" ? model.slice(0, 40) : "", permissionMode: "jail", archived: false,
                claudeSession: "", grokSession: "", lastPrompt: "", lastTurnEndTs: 0, queue: [], seq: 0 };
    sessions.set(id, S);
    audit({ act: "create", id, engine: "grok", cwd: jail });
    log(`create ${id} engine=grok jail=${jail}`);
    saveSessions(); notifySessions();
    return S;
  }
  if (engine === "api") {   // F4: loop agentic próprio — sem processo persistente, turn-based
    if (!AGENT_KEY) return null;
    const dir2 = cwd && existsSync(cwd) ? cwd : `${HOME}/Projects`;
    const S = { id, engine: "api", child: null, cwd: dir2, title: (title || "api").slice(0, 60), status: "idle",
                events: [], subs: new Set(), turns: 0, lastTs: Date.now(), createdAt: Date.now(), always: new Set(),
                model: typeof model === "string" ? model.slice(0, 60) : "",
                permissionMode: (permissionMode === "acceptEdits" || permissionMode === "auto") ? permissionMode : "default",
                archived: false, claudeSession: "", grokSession: "", lastPrompt: "", lastTurnEndTs: 0, queue: [], seq: 0, apiMessages: null };
    sessions.set(id, S);
    audit({ act: "create", id, engine: "api", cwd: dir2 });
    log(`create ${id} engine=api model=${apiModel(S)}`);
    saveSessions(); notifySessions();
    return S;
  }
  const dir = cwd && existsSync(cwd) ? cwd : `${HOME}/Projects`;
  const S = { id, child: null, cwd: dir, title: (title || dir.split("/").pop() || "sessão").slice(0, 60), status: "idle", events: [], subs: new Set(), turns: 0, lastTs: Date.now(), createdAt: Date.now(), always: new Set(), model: MODELS.has(model) ? model : "", permissionMode: permissionMode === "auto" ? "auto" : (MODES.has(permissionMode) ? permissionMode : "default"), archived: false, claudeSession: "", lastPrompt: "", lastTurnEndTs: 0, queue: [], seq: 0 };
  wireChild(S, baseArgs(S));
  sessions.set(id, S);
  audit({ act: "create", id, cwd: dir });
  log(`create ${id} cwd=${dir}`);
  saveSessions(); notifySessions();
  return S;
}

function baseArgs(S){
  const args = [
    "-p", "--input-format", "stream-json", "--output-format", "stream-json",
    "--include-partial-messages", "--verbose",
    "--permission-mode", S.permissionMode === "auto" ? "acceptEdits" : (S.permissionMode || "default"),   // "auto" é do DAEMON, não do CLI
    "--allowedTools", ALLOWED,
    "--permission-prompt-tool", "mcp__approver__approve",
    "--mcp-config", writeMcpConfig(S.id),
  ];
  if (S.model) args.push("--model", S.model);
  return args;
}

// "Continuar aqui": adota uma sessão do CLI (terminal) como sessão dirigível do bridge, via
// `--resume <sessionId>` — o histórico inteiro vem junto.
//
// GATE: só se o processo do terminal estiver MORTO. Duas instâncias do claude escrevendo o mesmo
// <sessionId>.jsonl é receita de transcript corrompido. Processo vivo → 409, sem exceção.
function adoptCliSession(pid){
  let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { return { err: "notfound" }; }
  if (!meta?.sessionId) return { err: "notfound" };
  if (pidAlive(pid)) return { err: "alive" };                       // gate
  if (liveCount() >= MAX_SESSIONS) return { err: "limit" };
  if (!findTranscript(meta.sessionId)) return { err: "notranscript" };

  const id = randomUUID().slice(0, 8);
  const dir = meta.cwd && existsSync(meta.cwd) ? meta.cwd : `${HOME}/Projects`;
  const S = { id, child: null, cwd: dir, title: (meta.name || "sessão do terminal").slice(0, 60),
              status: "idle", events: [], subs: new Set(), turns: 0, lastTs: Date.now(), createdAt: Date.now(),
              always: new Set(), model: "", permissionMode: "default", archived: false,
              claudeSession: meta.sessionId, lastPrompt: "", lastTurnEndTs: 0, queue: [], seq: 0, adoptedFromPid: pid };
  wireChild(S, [...baseArgs(S), "--resume", meta.sessionId]);
  sessions.set(id, S);
  audit({ act: "adopt", id, pid, claudeSession: meta.sessionId, cwd: dir });
  log(`adopt ${id} ← pid ${pid} (${meta.sessionId})`);
  saveSessions(); notifySessions();
  return { S };
}

// Trocar modelo/modo = trocar argv → exige respawn. `--resume <claudeSession>` preserva o histórico
// (mesmo mecanismo do /revive). Sem claudeSession (sessão que nunca teve um init) o processo nasce limpo.
function respawn(S){
  // Trocar modelo/modo no meio de um turno mata o child sem `result`: sem isto o app ficaria
  // com running=true pra sempre (spinner infinito, botão Stop no lugar do enviar).
  if (S.status === "running") push(S, { kind: "turn_end", ok: false, queued: 0, next: false, aborted: "respawn" });
  clearTimeout(S.interruptTimer); S.interruptTimer = null;
  try { S.child?.kill(); } catch {}
  clearQueue(S, "respawn");   // processo novo: a fila do antigo nunca seria drenada
  denyPending(S, "respawn");  // mcp-approval morreu junto: as pendentes nunca resolveriam
  S.child = null;
  const args = baseArgs(S);
  if (S.claudeSession) args.push("--resume", S.claudeSession);
  wireChild(S, args);
  S.status = "idle";
  S.lastTs = Date.now();
  saveSessions();
}

function wireChild(S, args){
  const child = spawn(CLAUDE, args, { cwd: S.cwd, env: { ...process.env, PATH: PATHENV } });
  S.child = child;
  createInterface({ input: child.stdout }).on("line", (l) => {
    let ev; try { ev = JSON.parse(l); } catch { return; }
    const cur = curate(ev); if (!cur) return;
    for (const c of Array.isArray(cur) ? cur : [cur]) {
      if (c.kind === "init" && c.claudeSession) S.claudeSession = c.claudeSession;   // pro --resume pós-morte
      if (c.kind === "turn_end") {
        clearTimeout(S.interruptTimer); S.interruptTimer = null;   // o turno terminou: fallback SIGINT não é mais devido
        S.lastTurnEndTs = Date.now();   // alimenta o filtro "Pronto para revisão"
        const next = (S.queue && S.queue.length) ? S.queue.shift() : null;
        c.queued = S.queue ? S.queue.length : 0;   // quantas ficam ESPERANDO (fora a que vai rodar já)
        c.next = !!next;                            // outra entra agora → o app não pisca "ocioso"
        S.status = next ? "running" : "idle";      // "idle" só quando a fila secou — senão é mentira
        // "Pronto pra revisão" só faz sentido quando não há mais nada engatilhado
        if (!next && c.ok !== false && (c.durationMs || 0) > 20000) notify(S, "done", `✅ Pronto pra revisão · ${S.title}`, `Turno concluído em ${Math.round((c.durationMs || 0) / 1000)}s.`, `A sessão "${S.title}" terminou o turno. Abra Código pra revisar o resultado.`);
        S.lastTs = Date.now();
        push(S, c);
        // Ilha: fila secou → encerra a atividade; senão o dispatch abaixo já reinicia o cronômetro
        if (!next) { S.laLast = 0; laPush(S, { fase: "concluído", event: "end" }); S.laInicio = null; }
        if (next) dispatch(S, next);   // só agora é seguro escrever no stdin
        saveSessions();
        continue;
      }
      if (c.kind === "tool_use") laPush(S, { fase: "trabalhando", tool: c.tool });
      S.lastTs = Date.now();
      push(S, c);
    }
  });
  child.on("error", (e) => {   // spawn falhou (binário sumiu, EMFILE): emite 'error', NÃO 'exit'
    log(`[${S.id}] spawn error: ${e.message}`);
    if (S.child === child) { S.status = "dead"; S.child = null; clearQueue(S, "spawn_error"); denyPending(S, "spawn_error"); push(S, { kind: "session_end", code: -1 }); saveSessions(); }
  });
  child.stdin.on("error", (e) => log(`[${S.id}] stdin error: ${e.message}`));   // EPIPE se o filho morrer
  child.stderr.on("data", d => log(`[${S.id}] stderr: ${String(d).slice(0, 200)}`));
  child.on("exit", (code) => { if (S.child === child) { S.status = "dead"; S.child = null; clearQueue(S, "session_end"); denyPending(S, "session_end"); push(S, { kind: "session_end", code }); log(`[${S.id}] exit ${code}`); saveSessions(); } });
}

// ── pairing (F6): `xneog pair` gera código de uso único; o app troca o código por
// {deviceId, secret} e passa a cunhar tokens v2 sozinho. O código É a credencial do claim:
// 10 chars base32 (~50 bits), 5min, single-use, 10 tentativas erradas → pending morre.
const pairPending = new Map();   // code -> { deviceId, secret, name, exp, attempts }
function pairGC(){ const now = Date.now(); for (const [c, p] of pairPending) if (p.exp < now) pairPending.delete(c); }

const server = createServer(async (req, res) => {
  // alias /code/* → /* : o app iOS fala o contrato do proxy do native-api; aqui é o mesmo protocolo
  if (req.url.startsWith("/code/")) req.url = req.url.slice(5);
  const urlEarly = new URL(req.url, "http://x");

  // SEM auth: o device ainda não tem credencial — o código de pairing é a credencial.
  if (urlEarly.pathname === "/pair/claim" && req.method === "POST") {
    pairGC();
    const body = JSON.parse((await readBody(req, 4096)) || "{}");
    const code = String(body.code || "").toUpperCase().replace(/[^A-Z2-9]/g, "");
    const P = pairPending.get(code);
    if (!P) {
      // tentativa errada queima TODAS as pendentes após 10 erros acumulados (anti brute-force)
      for (const p of pairPending.values()) if (++p.attempts >= 10) { pairPending.clear(); break; }
      res.writeHead(404, JSONH); return res.end(`{"error":"código inválido ou expirado"}`);
    }
    pairPending.delete(code);   // single-use
    const map = devices();
    map[P.deviceId] = { secret: P.secret, name: String(body.name || P.name || "device").slice(0, 60), created: new Date().toISOString() };
    writeFileSync(DEVICES_FILE, JSON.stringify(map, null, 2) + "\n", { mode: 0o600 });
    _devCache = { mtime: 0, map: {} };   // invalida cache (mtime pode colidir no mesmo ms)
    log(`[pair] device ${P.deviceId} pareado (${map[P.deviceId].name})`);
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ deviceId: P.deviceId, secret: P.secret, tokenFormat: "v2.<deviceId>.<expiryMs>.<hmacHex>", maxTtlMs: MAX_TTL_MS }));
  }

  if (!auth(req)) { res.writeHead(401, JSONH); return res.end(`{"error":"unauthorized"}`); }
  const url = new URL(req.url, "http://x");
  const parts = url.pathname.split("/").filter(Boolean);   // ["sessions", id?, action?]

  // início do pairing: SÓ com a master key (device não cunha device)
  if (url.pathname === "/pair/start" && req.method === "POST") {
    const tok = (req.headers["authorization"] || "").replace(/^Bearer\s+/i, "");
    if (!eq(tok, KEY)) { res.writeHead(403, JSONH); return res.end(`{"error":"pairing exige a master key"}`); }
    pairGC();
    if (pairPending.size >= 5) { res.writeHead(429, JSONH); return res.end(`{"error":"pareamentos pendentes demais"}`); }
    const body = JSON.parse((await readBody(req, 4096)) || "{}");
    const ALPHA = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";   // sem 0/O/1/I/L
    const code = [...randomBytes(10)].map(b => ALPHA[b % ALPHA.length]).join("");
    const P = { deviceId: `dev-${randomBytes(6).toString("hex")}`, secret: randomBytes(32).toString("hex"),
                name: String(body.name || "").slice(0, 60), exp: Date.now() + 5 * 60 * 1000, attempts: 0 };
    pairPending.set(code, P);
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ code, deviceId: P.deviceId, expiresInSec: 300 }));
  }

  // SSE global: "a lista mudou". Heartbeat mantém o túnel do cloudflared vivo.
  if (url.pathname === "/events" && req.method === "GET") {
    // teto de assinantes: um token válido (ou replayado no TTL) podia abrir SSE sem limite — cada um é
    // um setInterval + fd. 429 acima do teto (o app usa 1). Cleanup só ocorre no close do cliente.
    if (listSubs.size >= MAX_LIST_SUBS) { res.writeHead(429, JSONH); return res.end(`{"error":"limite de streams"}`); }
    res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache, no-store", "connection": "keep-alive", "x-accel-buffering": "no" });
    res.write("retry: 3000\n\n");
    res.write(`data: ${JSON.stringify({ hello: true, t: Date.now() })}\n\n`);
    listSubs.add(res);
    const hb = setInterval(() => { try { res.write(": hb\n\n"); } catch {} }, 15000);
    const close = () => { clearInterval(hb); listSubs.delete(res); };
    req.on("close", close); req.on("error", close);
    return;
  }

  if (url.pathname === "/health") {
    const subs = [...sessions.values()].reduce((a, S) => a + S.subs.size, 0);
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ ok: true, sessions: sessions.size, pending: pending.size, listSubs: listSubs.size, streamSubs: subs }));
  }

  // F1 xneog-agentd: protocolo VERSIONADO — clientes (TUI/app/futuros) checam capacidades em vez de
  // adivinhar rotas. Bump de `protocol` só em mudança incompatível; capacidade nova = append.
  if (url.pathname === "/meta" && req.method === "GET") {
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ name: "xneog-agentd", protocol: 1,
      capabilities: ["sessions", "cli", "tasks", "commands", "approval-queue", "sse-replay", "inject-tty", "queue", "revive", "adopt", "engines", "transcripts", "import", "pair"] }));
  }

  // F2: registry declarativo de engines/modelos (builtin + overlay engines.json, editável sem deploy)
  if (url.pathname === "/models" && req.method === "GET") {
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ engines: engineRegistry() }));
  }

  if (url.pathname === "/commands" && req.method === "GET") {
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ commands: listCommands(), menu: commandMenu() }));
  }

  if (url.pathname === "/projects" && req.method === "GET") {
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ projects: listProjects() }));
  }

  // chamado pelo mcp-approval.mjs (localhost) — segura a resposta até o owner decidir no app
  if (url.pathname === "/internal/approval" && req.method === "POST") {
    // 2ª parede: só o mcp-approval (filho do bridge) tem o APPROVAL_SECRET. Um device (mesmo com token
    // v2 válido, que satisfaz o auth() global) chegando por /code/internal/approval NÃO tem este header
    // → 403. Fecha o flood de cards forjados/exhaustion. Comparação timing-safe.
    if (!eq(String(req.headers["x-approval-secret"] || ""), APPROVAL_SECRET)) {
      res.writeHead(403, JSONH); return res.end(`{"error":"forbidden"}`);
    }
    // O body carrega o `input` INTEIRO da tool (Write/MultiEdit podem passar de 1MB). Com o cap default
    // o readBody devolvia null → b={} → sessão "" → deny "sessão desconhecida": a tool morria fora da
    // fila e o erro mentia a causa. Cap 16MB (tráfego localhost do mcp-approval) + deny com motivo real.
    const raw = await readBody(req, 16 << 20);
    if (raw === null) { res.writeHead(200, JSONH); return res.end(JSON.stringify({ behavior: "deny", message: "input grande demais para a fila de aprovação" })); }
    let b; try { b = JSON.parse(raw || "{}"); } catch { b = {}; }
    const S = sessions.get(String(b.session || ""));
    const tool = String(b.tool_name || "?");
    if (!S) { res.writeHead(200, JSONH); return res.end(JSON.stringify({ behavior: "deny", message: "sessão desconhecida" })); }
    if (S.permissionMode === "auto") { res.writeHead(200, JSONH); audit({ act: "approval", sid: S.id, tool, approved: true, by: "auto" }); return res.end(JSON.stringify({ behavior: "allow", updatedInput: b.input ?? {} })); }
    if (autoReadOK(S, tool, b.input)) { res.writeHead(200, JSONH); audit({ act: "approval", sid: S.id, tool, approved: true, by: "auto-read", cmd: String(b.input?.command || "").slice(0, 120) }); return res.end(JSON.stringify({ behavior: "allow", updatedInput: b.input ?? {} })); }
    if (S.always.has(tool)) { res.writeHead(200, JSONH); audit({ act: "approval", sid: S.id, tool, approved: true, by: "always" }); return res.end(JSON.stringify({ behavior: "allow", updatedInput: b.input ?? {} })); }
    // teto de pendentes: fail-closed (nega, não acumula) — cada pendente segura um socket + timer 120s
    if (pending.size >= MAX_PENDING) { res.writeHead(200, JSONH); return res.end(JSON.stringify({ behavior: "deny", message: "fila de aprovação cheia" })); }
    const requestId = `pr_${++reqSeq}_${Date.now()}`;
    const timer = setTimeout(() => resolveApproval(requestId, false, "timeout"), APPROVAL_TIMEOUT_MS);
    pending.set(requestId, { res, sid: S.id, tool, input: b.input ?? {}, timer });
    S.laLast = 0;   // zera ANTES: aprovação é raro e importante, o throttle não pode engolir
    laPush(S, { fase: "aprovação", tool, requerEntrada: true });
    push(S, { kind: "permission_request", requestId, tool, input: capInput(b.input) });
    const cmd = (b.input && (b.input.command || b.input.file_path)) || "";
    notify(S, "approval", `⌘ Requer aprovação · ${S.title}`, `${tool}: ${String(cmd).slice(0, 120)}`, `Sessão "${S.title}" aguardando sua decisão (${tool}). Abra Código → ${S.title}. Sem resposta em 120s = negado.`, { requestId, tool });
    return;   // resposta fica pendurada até resolveApproval
  }

  if (parts[0] !== "sessions") { res.writeHead(404, JSONH); return res.end(`{"error":"not found"}`); }

  if (parts.length === 1 && req.method === "GET") {
    const list = [...sessions.values()].map(S => {
      const needsInput = [...pending.values()].filter(P => P.sid === S.id).length;
      return { id: S.id, title: S.title, cwd: S.cwd, status: S.status, lastTs: S.lastTs, turns: S.turns, count: S.events.length,
               createdAt: S.createdAt, needsInput,
               engine: S.engine || "claude",                             // F2: badge/seletor no cliente
               aiTitle: S.engine === "grok" ? "" : aiTitleFor(S.claudeSession),   // título de IA (padrão RC)
               clients: presenceOf(S),                                  // presença: terminal/app ao vivo
               // grok é turn-based: sem child persistente, "conectada" = utilizável (não-morta)
               connected: (S.engine === "grok" || S.engine === "api") ? S.status !== "dead" : (!!S.child && S.status !== "dead"),
               reviveable: S.status === "dead" && !!S.claudeSession,     // dead COM --resume disponível
               lastPrompt: S.lastPrompt || "",                           // preview no card
               model: S.model || "", permissionMode: S.permissionMode || "default", archived: !!S.archived,
               always: [...(S.always || [])],                            // grants "sempre permitir" ativos (revogáveis no app)
               queued: S.queue ? S.queue.length : 0, seq: S.seq || 0,
               // "Pronto para revisão": terminou o turno, não está ocupado e não pede nada
               readyForReview: S.status === "idle" && !needsInput && !!S.lastTurnEndTs };
    });
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ sessions: list.sort((a, b) => b.lastTs - a.lastTs) }));
  }

  // Inventário READ-ONLY das sessões do CLI no Mac (as que o app oficial dirige via relay Anthropic).
  // Aparecem na lista como contexto; não são dirigíveis pelo bridge (driveable:false) — ele só fala
  // com processos que ele mesmo spawnou em stream-json.
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 2 && req.method === "GET") {
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ sessions: cliSessions() }));
  }

  // Elimina a sessão do terminal: encerra o processo claude (se vivo) e tira o registro da lista.
  // SIGTERM primeiro (o CLI sai limpo e o hook SessionEnd remove o json sozinho); só recorre a
  // SIGKILL se ainda estiver de pé em 1.5s — e aí o json é removido aqui, porque hook morto não roda.
  // A ABA do terminal continua aberta: matamos o claude, não o shell.
  // Mesmo gate anti-RCE do /inject: pid tem que ser sessão registrada COM start-time batendo (senão
  // um pid reciclado viraria "mate qualquer processo do meu usuário"). Sessão já morta: só desregistra.
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 3 && req.method === "DELETE") {
    const pid = Number(parts[2]);
    let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { meta = null; }
    if (!meta?.pid) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão CLI desconhecida"}`); }
    const vivo = !!validCliPid(pid);
    let killed = false;
    if (vivo) {
      try { process.kill(pid, "SIGTERM"); killed = true; } catch {}
      await new Promise(r => setTimeout(r, 1500));
      if (pidAlive(pid) && startMatches(pid, meta.startedAt)) { try { process.kill(pid, "SIGKILL"); } catch {} }
    }
    try { unlinkSync(`${CLI_SESS_DIR}/${pid}.json`); } catch {}
    delete cliNames[cliNameKey(meta)]; saveCliNames();
    audit({ act: "cli_kill", pid, alive: vivo });
    log(`cli kill pid ${pid} (vivo=${vivo})`);
    notifySessions();
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, killed }));
  }

  // Apelida a sessão do terminal (overlay do bridge — ver CLI_NAMES_FILE). Título vazio = volta ao
  // nome derivado. Não exige processo vivo: renomear é metadado, não controle (sem gate anti-RCE).
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 4 && parts[3] === "rename" && req.method === "POST") {
    const pid = Number(parts[2]);
    let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { meta = null; }
    if (!meta?.pid) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão CLI desconhecida"}`); }
    let t = ""; try { t = String(JSON.parse((await readBody(req)) || "{}").title || ""); } catch { res.writeHead(400, JSONH); return res.end(`{"error":"json inválido"}`); }
    t = t.replace(/[\r\n\t]/g, " ").trim().slice(0, 60);
    const k = cliNameKey(meta);
    if (t) cliNames[k] = t; else delete cliNames[k];
    saveCliNames();
    audit({ act: "cli_rename", pid, title: t });
    notifySessions();   // o app repuxa a lista (o ping é genérico "mudou")
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, name: t || meta.name || `pid ${pid}` }));
  }

  // "Continuar aqui" — adota a sessão do terminal. 409 se o processo dela ainda estiver vivo.
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 4 && parts[3] === "adopt" && req.method === "POST") {
    const r = adoptCliSession(Number(parts[2]));
    if (r.err === "alive") { res.writeHead(409, JSONH); return res.end(`{"error":"a sessão ainda roda no terminal — encerre-a antes de continuar aqui"}`); }
    if (r.err === "limit") { res.writeHead(429, JSONH); return res.end(`{"error":"limite de sessões vivas"}`); }
    if (r.err) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão do CLI não encontrada"}`); }
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ id: r.S.id, title: r.S.title, cwd: r.S.cwd }));
  }

  // Dirige a sessão do terminal SEM tocar a Anthropic: injeta o texto no TTY da sessão viva
  // (via xneog-inject → Terminal.app). A RESPOSTA volta pelo /transcript (o .jsonl que já lemos).
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 4 && parts[3] === "inject" && req.method === "POST") {
    const pid = Number(parts[2]);
    // GATE ANTI-RCE: pid tem que ser uma sessão Claude Code registrada E o processo o mesmo (start-time).
    if (!validCliPid(pid)) { res.writeHead(409, JSONH); return res.end(`{"error":"pid não é uma sessão Claude Code válida (ou não está mais rodando)"}`); }
    const raw = await readBody(req, 64 * 1024);
    if (raw === null) { res.writeHead(413, JSONH); return res.end(`{"error":"mensagem grande demais"}`); }
    let text = ""; try { text = String(JSON.parse(raw).text || ""); } catch { res.writeHead(400, JSONH); return res.end(`{"error":"json inválido"}`); }
    text = sanitizeInput(text);
    if (!text) { res.writeHead(400, JSONH); return res.end(`{"error":"texto vazio"}`); }
    const tty = sessionTty(pid);
    const sock = tty ? null : sessionSocket(pid);   // interativa → TTY · bg → socket PTY
    const via = tty ? "tty" : sock ? "socket" : null;
    if (!via) { res.writeHead(400, JSONH); return res.end(`{"error":"sessão sem terminal nem socket controlável"}`); }
    const out = tty ? await injectToTty(tty, text) : await injectToSocket(sock, text);
    audit({ act: "inject", pid, via, ok: out.ok, chars: text.length });   // sem 'target'/texto no audit
    if (!out.ok) { res.writeHead(502, JSONH); return res.end(JSON.stringify({ error: "injeção falhou", detail: out.err || "" })); }
    log(`inject pid ${pid} via ${via} (${text.length} chars)`);
    armCliWatch(pid);   // item 7: notifica quando o turno desta injeção terminar (busy→idle) mesmo com o app fechado
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, via }));
  }

  // STOP da sessão do terminal: cancela o TURNO em andamento sem matar o processo. Injeta ESC na aba
  // (xneog-inject --interrupt) — o par de enviar+parar da dirigibilidade. Mesmo gate anti-RCE do /inject.
  // Só TTY (interativa): bg/socket é read-only e o Stop dele é do app oficial. Nunca SIGINT (mataria).
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 4 && parts[3] === "interrupt" && req.method === "POST") {
    const pid = Number(parts[2]);
    if (!validCliPid(pid)) { res.writeHead(409, JSONH); return res.end(`{"error":"pid não é uma sessão Claude Code válida (ou não está mais rodando)"}`); }
    const tty = sessionTty(pid);
    if (!tty) { res.writeHead(400, JSONH); return res.end(`{"error":"sessão sem terminal controlável (bg/socket = read-only)"}`); }
    const out = await interruptTty(tty);
    audit({ act: "cli_interrupt", pid, ok: out.ok });
    if (!out.ok) { res.writeHead(502, JSONH); return res.end(JSON.stringify({ error: "stop falhou", detail: out.err || "" })); }
    log(`cli interrupt pid ${pid} (ESC)`);
    res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
  }

  // SSE AO VIVO do transcript CLI: empurra o delta assim que o .jsonl muda (fs.watch, debounce 120ms)
  // + heartbeat 5s. Uma conexão persistente no lugar do poll de 2s → atualização sub-segundo. Gate anti-RCE.
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 5 && parts[3] === "transcript" && parts[4] === "stream" && req.method === "GET") {
    const pid = Number(parts[2]);
    const meta = validCliPid(pid);
    if (!meta?.sessionId) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão CLI inválida"}`); }
    const path = findTranscript(meta.sessionId);
    if (!path) { res.writeHead(404, JSONH); return res.end(`{"error":"transcript não encontrado"}`); }
    // teto global de watchers de transcript (cada um = fsWatch + heartbeat 5s); sem teto um token abre sem limite
    if (transcriptStreams >= MAX_TRANSCRIPT_STREAMS) { res.writeHead(429, JSONH); return res.end(`{"error":"limite de streams de transcript"}`); }
    transcriptStreams++;
    res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache, no-store", "connection": "keep-alive", "x-accel-buffering": "no" });
    let sent = Number(url.searchParams.get("from") || 0);
    let dead = false, timer = null, watcher = null, hb = null;
    const cleanup = () => { if (dead) return; dead = true; transcriptStreams--; if (hb) clearInterval(hb); if (timer) clearTimeout(timer); try { watcher?.close(); } catch {} };
    const push = () => {
      if (dead || res._xnBehind) return;   // item 6: socket atrás (4G lenta) → pula; o próximo push coalesce
      let all; try { all = readCliEvents(path); } catch { return; }
      const slice = all.length > sent ? all.slice(sent).map((e, k) => ({ i: sent + k, ...e })) : [];
      // relê o <pid>.json FRESCO (não o meta congelado do connect): idle→busy do terminal só aparece
      // assim. E manda `connected`(vivo) + `driveVia` no frame → o app fecha o composer-fantasma na
      // hora (sessão morta/aba fechada) em vez de esperar o poll de 6-30s da lista.
      const alive = pidAlive(pid);
      let m = meta; try { m = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")) || meta; } catch {}
      const tty = alive ? sessionTty(pid) : null;
      const driveVia = tty ? "tty" : (alive && sessionSocket(pid)) ? "socket" : null;
      let ok = false;
      const tasks = sessionTasks(m.sessionId || meta.sessionId);   // cache 3s — barato por push
      try { ok = res.write(`data: ${JSON.stringify({ events: slice, total: all.length, status: displayStatus(m, alive), connected: alive, driveVia, tasksRunning: tasks.running, aiTitle: cliCache.get(path)?.aiTitle || "" })}\n\n`); } catch { cleanup(); return; }
      sent = all.length;   // só avança após escrever (write=false ainda bufferizou, sem perda); enquanto behind não re-avança
      if (!ok) { res._xnBehind = true; res.once("drain", () => { res._xnBehind = false; }); }
    };
    const schedule = () => { if (timer || dead) return; timer = setTimeout(() => { timer = null; push(); }, 120); };
    req.on("close", cleanup); res.on("close", cleanup); res.on("error", cleanup);
    push();   // delta inicial (se from < total)
    try { watcher = fsWatch(path, () => schedule()); } catch {}
    hb = setInterval(push, 5000);
    return;
  }

  // Transcript AO VIVO de uma sessão do CLI (read-only). O app polla com ?from=<total anterior>
  // e recebe só o delta — mesmo contrato do /stream?from=N do bridge.
  // ?tail=N na 1ª carga: devolve os últimos N eventos (o transcript desta sessão passa de 8MB).
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 4 && parts[3] === "transcript" && req.method === "GET") {
    const pid = Number(parts[2]);
    let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { meta = null; }
    if (!meta?.sessionId) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão CLI desconhecida"}`); }
    const path = findTranscript(meta.sessionId);
    if (!path) { res.writeHead(404, JSONH); return res.end(`{"error":"transcript não encontrado"}`); }

    const q = url.searchParams;
    // Back-pagination (F0): ?before=<byte> devolve a janela ANTERIOR do transcript (histórico), o app prepend.
    const before = q.get("before");
    if (before != null) {
      const w = readCliWindow(path, Number(before) || 0);
      res.writeHead(200, JSONH);
      return res.end(JSON.stringify({ events: w.events, windowStart: w.windowStart, fileSize: w.fileSize, history: true }));
    }

    const all = readCliEvents(path);
    const c = cliCache.get(path);
    let fileSize = 0; try { fileSize = statSync(path).size; } catch {}
    const tail = Number(q.get("tail") || 0);
    let from = Number(q.get("from") || 0);
    if (tail > 0 && !q.get("from")) from = Math.max(0, all.length - tail);
    if (!Number.isFinite(from) || from < 0) from = 0;

    const slice = all.slice(from).map((e, k) => ({ i: from + k, ...e }));
    const tasks = sessionTasks(meta.sessionId);
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({
      total: all.length, from, events: slice,
      windowStart: c?.windowStart || 0, fileSize,   // F0: windowStart>0 = há histórico anterior; fileSize = tamanho real do .jsonl
      title: c?.aiTitle || meta.name || `pid ${pid}`, cwd: meta.cwd || "",   // aiTitle = padrão do app oficial RC
      status: displayStatus(meta, pidAlive(pid)),
      connected: pidAlive(pid),
      tasksRunning: tasks.running,
    }));
  }

  // Subagentes/workflows da sessão CLI (cards + sheet "Tarefas em segundo plano" no app).
  if (parts[0] === "sessions" && parts[1] === "cli" && parts.length === 4 && parts[3] === "tasks" && req.method === "GET") {
    const pid = Number(parts[2]);
    let meta; try { meta = JSON.parse(readFileSync(`${CLI_SESS_DIR}/${pid}.json`, "utf8")); } catch { meta = null; }
    if (!meta?.sessionId) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão CLI desconhecida"}`); }
    res.writeHead(200, JSONH); return res.end(JSON.stringify(sessionTasks(meta.sessionId)));
  }

  if (parts.length === 1 && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const engine = b.engine === "grok" ? "grok" : b.engine === "api" ? "api" : "claude";
    // F5: perfis nomeados do cliente (safe/edit) viram permissionMode aqui; bypass NÃO existe (doutrina)
    const S = createSession({ cwd: typeof b.cwd === "string" ? b.cwd : "", title: typeof b.title === "string" ? b.title : "", model: typeof b.model === "string" ? b.model : "", engine,
                              permissionMode: typeof b.permissionMode === "string" ? b.permissionMode : "" });
    if (!S) { res.writeHead(429, JSONH); return res.end(`{"error":"limite de sessões (ou engine indisponível)"}`); }
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ id: S.id, title: S.title, cwd: S.cwd, engine: S.engine || "claude" }));
  }

  // F3: importar sessão do Claude Code CLI (rampa de migração, padrão grok-build). Cria sessão do
  // bridge dead/reviveável APONTANDO pra claudeSession (revive = --resume com contexto pleno) e
  // hidrata o histórico curado do transcript nativo no transcript próprio → replay imediato no app/TUI.
  if (parts.length === 2 && parts[1] === "import" && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const cs = typeof b.claudeSession === "string" ? b.claudeSession.trim() : "";
    const p = findTranscript(cs);
    if (!p) { res.writeHead(404, JSONH); return res.end(`{"error":"transcript não encontrado"}`); }
    const dup = [...sessions.values()].find(x => x.claudeSession === cs);
    if (dup) { res.writeHead(409, JSONH); return res.end(JSON.stringify({ error: "já importada", id: dup.id })); }
    const evs = readCliEvents(p);   // janela do fim em transcript gigante — suficiente pra rampa
    let cwd = "";
    try { for (const line of readFileSync(p, "utf8").slice(0, 16384).split("\n")) {
      let j; try { j = JSON.parse(line); } catch { continue; }
      if (j.cwd) { cwd = j.cwd; break; }
    } } catch {}
    const id = randomUUID().slice(0, 8);
    const title = String(cliCache.get(p)?.aiTitle || b.title || `import ${cs.slice(0, 8)}`).slice(0, 60);
    const S2 = { id, engine: "claude", child: null, cwd, title, status: "dead", events: [], subs: new Set(),
                 turns: 0, lastTs: Date.now(), createdAt: Date.now(), always: new Set(), model: "",
                 permissionMode: "default", archived: false, claudeSession: cs, grokSession: "",
                 lastPrompt: "", lastTurnEndTs: 0, queue: [], seq: 0, lastNotify: {}, imported: true };
    for (const ev of evs.slice(-MAX_EVENTS)) {
      const item = { ...ev, i: S2.seq++, ts: Date.now() };   // i/ts NOSSOS vencem (seq da sessão nova)
      S2.events.push(item); transAppend(id, item);
      if (ev.kind === "user" && ev.text) { S2.lastPrompt = String(ev.text).slice(0, 200); S2.turns++; }
    }
    sessions.set(id, S2);
    saveSessions(); notifySessions();
    audit({ act: "import", id, claudeSession: cs, events: S2.events.length });
    log(`import ${id} ← ${cs} (${S2.events.length} eventos)`);
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ id, title: S2.title, cwd, events: S2.events.length, reviveable: true }));
  }

  const S = sessions.get(parts[1]);
  if (!S) { res.writeHead(404, JSONH); return res.end(`{"error":"sessão não existe"}`); }

  // Subagentes/workflows da sessão do bridge (mesmo layout: o claude -p grava em ~/.claude/projects).
  if (parts[2] === "tasks" && req.method === "GET") {
    res.writeHead(200, JSONH); return res.end(JSON.stringify(sessionTasks(S.claudeSession)));
  }

  if (parts[2] === "message" && req.method === "POST") {
    const raw = await readBody(req, 32 << 20);   // anexos: 4 imagens em base64
    if (raw === null) { res.writeHead(413, JSONH); return res.end(`{"error":"payload grande demais"}`); }
    let b; try { b = JSON.parse(raw || "{}"); } catch { b = {}; }
    const text = typeof b.text === "string" ? b.text.trim() : "";

    // Anexos: o CLI aceita bloco de imagem no stdin stream-json (verificado 09-jul).
    // Ordem importa — imagem ANTES do texto, senão o modelo responde antes de "ver".
    const content = [];
    for (const im of (Array.isArray(b.images) ? b.images.slice(0, MAX_IMAGES) : [])) {
      if (typeof im?.data !== "string" || im.data.length > MAX_IMAGE_B64) continue;
      const mt = OK_MEDIA.has(im.media_type) ? im.media_type : "image/jpeg";
      content.push({ type: "image", source: { type: "base64", media_type: mt, data: im.data } });
    }
    const nImg = content.length;
    if (text) content.push({ type: "text", text });

    if (!content.length) { res.writeHead(400, JSONH); return res.end(`{"error":"mensagem vazia"}`); }
    // AUTO-REVIVE: sessão claude morta COM --resume renasce ao receber mensagem (paridade com os
    // engines turn-based, que nunca "morrem"). Antes: 400 "sessão morta" e o usuário tinha que ir
    // ao app reviver na mão — atrito real visto no terminal do Gomes 24-jul.
    if (S.status === "dead") {
      if (S.engine === "grok" || S.engine === "api" || !S.claudeSession) { res.writeHead(400, JSONH); return res.end(`{"error":"sessão morta sem resume disponível"}`); }
      if (liveCount() >= MAX_SESSIONS) { res.writeHead(429, JSONH); return res.end(`{"error":"limite de sessões vivas — encerre alguma"}`); }
      respawn(S);   // wireChild com --resume <claudeSession> → histórico pleno, status idle
      push(S, { kind: "session_revived" });
      audit({ act: "revive", id: S.id, resume: S.claudeSession, by: "auto-message" });
      log(`[${S.id}] auto-revive via message`);
    }

    const tag = nImg ? `[${nImg} ${nImg === 1 ? "imagem" : "imagens"}]` : "";
    // presença/origem: qual cliente mandou este turno ("terminal" | "app") — vai no evento user
    const via = b.via === "terminal" ? "terminal" : b.via === "app" ? "app" : "";
    const item = { content, label: [tag, text].filter(Boolean).join(" ").trim(), nImg, via };
    S.lastPrompt = (text || tag).slice(0, 200);   // preview no card da lista (igual ao app oficial)
    // audit = registro de QUE ação ocorreu, não do conteúdo. O resto do arquivo já evita logar I/O de
    // sessão (inject audita sem texto); o prompt podia trazer segredo colado → só metadados aqui.
    audit({ act: "message", id: S.id, len: text.length, images: nImg, queued: S.status === "running" });

    // Turno em andamento → FILA. Escrever no stdin agora abortaria o turno (ver nota em dispatch).
    if (S.status === "running") {
      if (!S.queue) S.queue = [];
      if (S.queue.length >= MAX_QUEUE) { res.writeHead(429, JSONH); return res.end(`{"error":"fila cheia"}`); }
      if (S.queue.reduce((n, it) => n + itemBytes(it), 0) + itemBytes(item) > MAX_QUEUE_BYTES) {
        res.writeHead(429, JSONH); return res.end(`{"error":"fila cheia (anexos ocupam a memória — aguarde o turno atual)"}`);
      }
      S.queue.push(item);
      const qid = S.seq;   // id que push() vai atribuir a ESTE evento queued → handle estável pro item na fila
      push(S, { kind: "queued", text: item.label, images: nImg, depth: S.queue.length });
      item.qid = qid;      // casa a row .queued(id:) do cliente com a entrada em S.queue (remoção granular)
      saveSessions();
      res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, queued: true, depth: S.queue.length }));
    }

    S.status = "running";
    dispatch(S, item);
    saveSessions();
    res.writeHead(200, JSONH); return res.end(`{"ok":true,"queued":false}`);
  }

  // reabre sessão morta COM histórico (claude --resume <uuid>) — sobrevive a restart do processo/Mac
  if (parts[2] === "revive" && req.method === "POST") {
    if (S.status !== "dead") { res.writeHead(400, JSONH); return res.end(`{"error":"sessão não está morta"}`); }
    if (!S.claudeSession) { res.writeHead(400, JSONH); return res.end(`{"error":"sem session-id pra retomar"}`); }
    clearQueue(S, "revive");   // itens de antes da morte não podem disparar sozinhos depois
    wireChild(S, [...baseArgs(S), "--resume", S.claudeSession]);
    S.status = "idle"; S.lastTs = Date.now(); S.restored = false;
    push(S, { kind: "session_revived" });
    audit({ act: "revive", id: S.id, resume: S.claudeSession });
    saveSessions();
    res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
  }

  if (parts[2] === "rename" && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const t = typeof b.title === "string" ? b.title.trim().slice(0, 60) : "";
    if (!t) { res.writeHead(400, JSONH); return res.end(`{"error":"título vazio"}`); }
    S.title = t; audit({ act: "rename", id: S.id, title: t });
    saveSessions(); notifySessions();
    res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
  }

  // Troca de modo de permissão a quente (paridade com o seletor do app oficial).
  // Sessão viva respawna com --resume; morta só grava a preferência p/ o próximo revive.
  if (parts[2] === "mode" && req.method === "POST") {
    if (S.engine === "grok") { res.writeHead(400, JSONH); return res.end(`{"error":"sessão grok não tem modos — a jaula é a parede"}`); }
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const m = typeof b.mode === "string" ? b.mode : "";
    if (S.engine === "api") {   // F4: sem processo → sem respawn; plan não existe no loop próprio
      if (m !== "default" && m !== "acceptEdits" && m !== "auto") { res.writeHead(400, JSONH); return res.end(`{"error":"engine api aceita default|acceptEdits|auto"}`); }
      S.permissionMode = m;
      audit({ act: "mode", id: S.id, engine: "api", mode: m });
      push(S, { kind: "mode_changed", mode: m }); saveSessions();
      res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, mode: m }));
    }
    if (m !== "auto" && !MODES.has(m)) { res.writeHead(400, JSONH); return res.end(`{"error":"modo inválido"}`); }
    S.permissionMode = m;
    audit({ act: "mode", id: S.id, mode: m });
    if (S.status !== "dead") { respawn(S); push(S, { kind: "mode_changed", mode: m }); } else saveSessions();
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, mode: m, respawned: S.status !== "dead" }));
  }

  // Troca de modelo a quente. claude = respawn com --resume; grok = só grava (vale no PRÓXIMO turno,
  // que é um spawn novo de qualquer jeito — turn-based).
  if (parts[2] === "model" && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const m = typeof b.model === "string" ? b.model : "";
    if (S.engine === "grok" || S.engine === "api") {   // turn-based: aplica no PRÓXIMO turno, sem respawn
      S.model = m.slice(0, 60);
      audit({ act: "model", id: S.id, engine: S.engine, model: m || "(default)" });
      push(S, { kind: "model_changed", model: m }); saveSessions();
      res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, model: m }));
    }
    if (m && !MODELS.has(m)) { res.writeHead(400, JSONH); return res.end(`{"error":"modelo inválido"}`); }
    S.model = m;   // "" = padrão da conta
    audit({ act: "model", id: S.id, model: m || "(default)" });
    if (S.status !== "dead") { respawn(S); push(S, { kind: "model_changed", model: m }); } else saveSessions();
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, model: m }));
  }

  // Arquivar: some da lista principal sem perder o histórico (DELETE é destrutivo, isto não).
  if (parts[2] === "archive" && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    S.archived = b.archived !== false;
    if (S.archived) {
      clearQueue(S, "archive");
      clearTimeout(S.interruptTimer); S.interruptTimer = null;
      // NÃO anular S.child aqui: o guard `S.child === child` do exit handler falharia e ninguém
      // emitiria session_end (chat aberto ficaria pendurado) nem fecharia os SSE.
      if (S.child) { try { S.child.kill(); } catch {} }
      else { S.status = "dead"; for (const sub of S.subs) { try { sub.end(); } catch {} } S.subs.clear(); }
    }
    audit({ act: "archive", id: S.id, archived: S.archived });
    saveSessions(); notifySessions();
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, archived: S.archived }));
  }

  // Transcript em texto puro — alimenta o "Compartilhar" do app (ShareLink).
  if (parts[2] === "transcript" && req.method === "GET") {
    const lines = [`# ${S.title}`, `cwd: ${S.cwd}`, ""];
    for (const e of S.events) {
      if (e.kind === "user") lines.push(`\n## Você\n${e.text}`);
      else if (e.kind === "text") lines.push(`\n## Claude\n${e.text}`);
      else if (e.kind === "tool_use") lines.push(`\n[tool] ${e.tool}`);
    }
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end(lines.join("\n"));
  }

  // Aprovação em LOTE: resolve todas as pendentes desta sessão cujo tool esteja em `tools`
  // (default = família de edição) e, com always, adiciona a família à allowlist da sessão.
  // Isto NÃO é bypassPermissions: Bash e amigos seguem pedindo.
  if (parts[2] === "permission" && parts[3] === "bulk" && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const tools = Array.isArray(b.tools) && b.tools.length ? b.tools.filter(t => typeof t === "string") : EDIT_TOOLS;
    const approve = b.approve !== false;
    const always = b.always === true;

    // O seed do `always` era INCONDICIONAL e vinha do body do cliente → `{tools:["Bash"],always:true}`
    // semeava Bash na allowlist SEM nenhuma pendência real, auto-aprovando todo Bash da sessão daí em
    // diante = bypassPermissions pela porta dos fundos. Agora só entra em `always` a tool que teve uma
    // pendência EFETIVAMENTE resolvida nesta chamada, e nunca as de NEVER_ALWAYS.
    const seeded = new Set();
    let n = 0;
    for (const [rid, P] of [...pending]) {
      if (P.sid !== S.id || !tools.includes(P.tool)) continue;
      if (resolveApproval(rid, approve, "app-bulk")) {
        n++;
        if (approve && always && !NEVER_ALWAYS.has(P.tool)) { S.always.add(P.tool); seeded.add(P.tool); }
      }
    }
    audit({ act: "approval_bulk", sid: S.id, tools, approve, always, resolved: n, seeded: [...seeded] });
    push(S, { kind: "bulk_resolved", tools, approve, always, resolved: n, seeded: [...seeded] });
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, resolved: n, always, seeded: [...seeded] }));
  }

  if (parts[2] === "permission" && req.method === "POST") {
    let b; try { b = JSON.parse((await readBody(req)) || "{}"); } catch { b = {}; }
    const ok = resolveApproval(String(b.requestId || ""), b.approve === true, "app", b.always === true, typeof b.reason === "string" ? b.reason : "");
    res.writeHead(ok ? 200 : 404, JSONH); return res.end(JSON.stringify({ ok }));
  }

  // Revogar um grant "sempre permitir" da sessão. Antes o único undo era matar/respawnar a sessão.
  // Enforce é runtime (S.always.has em 826) → deletar do Set já tira efeito na próxima chamada da tool.
  if (parts[2] === "always" && parts[3] && req.method === "DELETE") {
    const tool = decodeURIComponent(parts[3]);
    const had = S.always.delete(tool);
    if (had) { audit({ act: "always_revoke", id: S.id, tool }); saveSessions(); notifySessions(); }
    res.writeHead(had ? 200 : 404, JSONH); return res.end(JSON.stringify({ ok: had, always: [...S.always] }));
  }

  // Remoção GRANULAR da fila: dropa UM item por qid (o único controle antes era Stop, que nukava tudo).
  // Só toca itens que AINDA não dispararam; se já saiu pro stdin, 404 (o cliente ignora).
  if (parts[2] === "queue" && parts[3] && req.method === "DELETE") {
    const qid = Number(parts[3]);
    const idx = (S.queue || []).findIndex(it => it.qid === qid);
    if (idx < 0) { res.writeHead(404, JSONH); return res.end(`{"ok":false}`); }
    S.queue.splice(idx, 1);
    push(S, { kind: "queue_removed", removedId: qid, depth: S.queue.length });
    audit({ act: "queue_drop", id: S.id, qid });
    saveSessions();
    res.writeHead(200, JSONH); return res.end(JSON.stringify({ ok: true, depth: S.queue.length }));
  }

  // Stop = cancela o TURNO, não a sessão. O CLI aceita control_request/interrupt (verificado 09-jul:
  // responde control_response success, emite result error_during_execution, e o PROCESSO SOBREVIVE).
  // SIGINT — o que havia aqui antes — matava o processo e deixava a sessão `dead`.
  if (parts[2] === "interrupt" && req.method === "POST") {
    clearQueue(S, "interrupt");   // Stop para tudo: o que estava engatilhado não dispara sozinho
    // aprovações penduradas desta sessão morrem negadas — senão o badge "requer entrada" fica preso
    for (const [rid, P] of [...pending]) if (P.sid === S.id) resolveApproval(rid, false, "interrupt");
    if (S.engine === "grok") {
      // turn-based: matar o processo DO TURNO basta — o exit handler emite turn_end e a sessão (resume) sobrevive
      try { S.child?.kill("SIGTERM"); } catch {}
      audit({ act: "interrupt", id: S.id, engine: "grok" });
      res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
    }
    if (S.engine === "api") {   // F4: aborta o fetch em voo + sinaliza o loop; Bash do turno morre junto
      S.apiStop = true;
      try { S.apiAbort?.abort(); } catch {}
      try { S.apiChild?.kill("SIGKILL"); } catch {}
      audit({ act: "interrupt", id: S.id, engine: "api" });
      res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
    }
    let sent = false;
    try {
      S.child.stdin.write(JSON.stringify({ type: "control_request", request_id: `int-${Date.now()}`, request: { subtype: "interrupt" } }) + "\n");
      sent = true;
    } catch (e) { log(`[${S.id}] interrupt write falhou: ${e.message}`); }
    if (sent) {
      // Rede de segurança para CLI que ignore control_request. PERIGO: sem amarrar ao MESMO turno e ao
      // MESMO processo, o timer mataria um turno novo iniciado nesses 5s (SIGINT derruba o processo).
      // Por isso: guarda o child da época, e o turn_end cancela o timer.
      const alvo = S.child;
      clearTimeout(S.interruptTimer);
      S.interruptTimer = setTimeout(() => {
        S.interruptTimer = null;
        if (S.child === alvo && S.status === "running") {
          try { alvo?.kill("SIGINT"); } catch {}
          log(`[${S.id}] interrupt: fallback SIGINT (control_request ignorado)`);
        }
      }, 5000);
    } else {
      try { S.child?.kill("SIGINT"); } catch {}
    }
    audit({ act: "interrupt", id: S.id });
    res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
  }

  if (parts.length === 2 && req.method === "DELETE") {
    try { S.child?.kill(); } catch {}
    denyPending(S, "delete");   // solta os res pendurados + timers de 120s antes de sumir do Map
    try { unlinkSync(`${CFGDIR}/${S.id}.json`); } catch {}
    // fecha os SSE pendurados: sem isso o cliente (app/xneog-code) fica preso num stream de uma
    // sessão que não existe mais — nunca reconecta, nunca vê o 404, e a conexão vaza no servidor.
    for (const sub of S.subs) { try { sub.end(); } catch {} }
    S.subs.clear();
    sessions.delete(S.id);
    audit({ act: "delete", id: S.id });
    saveSessions(); notifySessions();
    res.writeHead(200, JSONH); return res.end(`{"ok":true}`);
  }

  if (parts[2] === "stream" && req.method === "GET") {
    // teto por sessão: cada from=0 força replay de até MAX_EVENTS + instala um heartbeat; sem teto um
    // token válido abre streams sem limite (fd/timer/CPU de replay). 429 acima do teto (o app usa 1).
    if (S.subs.size >= MAX_SESSION_SUBS) { res.writeHead(429, JSONH); return res.end(`{"error":"limite de streams desta sessão"}`); }
    // presença: o cliente se identifica (?client=terminal|app) → todos os outros veem quem está junto
    const tag0 = url.searchParams.get("client");
    res.clientTag = tag0 === "terminal" ? "terminal" : tag0 === "app" ? "app" : "";
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no" });
    const from = Math.max(0, Number(url.searchParams.get("from") || 0));
    // F3: pedido anterior ao buffer em memória → completa do transcript em disco (replay profundo)
    const first = S.events.length ? S.events[0].i : S.seq;
    if (from < first) for (const item of transRead(S.id, from)) {
      if (item.i < first) res.write(`id: ${item.i}\ndata: ${JSON.stringify(item)}\n\n`);
    }
    for (const item of S.events) if (item.i >= from) res.write(`id: ${item.i}\ndata: ${JSON.stringify(item)}\n\n`);
    S.subs.add(res);
    if (res.clientTag) push(S, { kind: "presence", ...presenceOf(S) });   // avisa quem já está na sessão
    else { try { res.write(`data: ${JSON.stringify({ kind: "presence", ...presenceOf(S), ts: Date.now() })}\n\n`); } catch {} }
    const hb = setInterval(() => { try { res.write(": hb\n\n"); } catch {} }, 15000);
    req.on("close", () => { clearInterval(hb); S.subs.delete(res); if (res.clientTag) push(S, { kind: "presence", ...presenceOf(S) }); });
    return;
  }

  res.writeHead(404, JSONH); res.end(`{"error":"not found"}`);
});

// SIGTERM: mata os filhos mas NÃO apaga o histórico — o disco guarda a metadata p/ o revive.
process.on("SIGTERM", () => { for (const S of sessions.values()) { try { S.child?.kill(); } catch {} } try { clearTimeout(_saveTimer); const arr = [...sessions.values()].map(sessMeta).sort((a, b) => b.lastTs - a.lastTs).slice(0, MAX_PERSIST); writeFileSync(SESS_FILE, JSON.stringify(arr, null, 2), { mode: 0o600 }); } catch {} process.exit(0); });

loadSessions();

// Configs .mcp/<id>.json carregam a BRIDGE_KEY. Só o DELETE de sessão os removia: crash, kill ou
// restart deixavam órfãos acumulando para sempre. No boot não há processo vivo → todos são órfãos.
try {
  let n = 0;
  for (const f of readdirSync(CFGDIR)) {
    if (!f.endsWith(".json")) continue;
    try { unlinkSync(`${CFGDIR}/${f}`); n++; } catch {}
  }
  if (n) log(`removidos ${n} mcp-config órfãos (continham BRIDGE_KEY)`);
} catch {}

// Map em memória cresce sem teto: só o disco era capado em MAX_PERSIST. Poda as MORTAS mais antigas
// (nunca as que têm cliente SSE anexado nem as vivas).
function pruneSessions(){
  const mortas = [...sessions.values()]
    .filter(S => S.status === "dead" && S.subs.size === 0)
    .sort((a, b) => b.lastTs - a.lastTs);
  for (const S of mortas.slice(MAX_PERSIST)) {
    sessions.delete(S.id);
    try { unlinkSync(`${CFGDIR}/${S.id}.json`); } catch {}
  }
}
setInterval(pruneSessions, 10 * 60 * 1000).unref();

server.listen(PORT, "127.0.0.1", () => console.log(`xneog-code-bridge em http://127.0.0.1:${PORT}`));
