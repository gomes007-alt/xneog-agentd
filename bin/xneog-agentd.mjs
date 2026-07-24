#!/usr/bin/env node
/**
 * xneog-agentd — daemon de sessões agentic para self-host (BYOK).
 *
 * Comandos:
 *   xneog-agentd init      cria ~/.xneog/{env,agentd/} e gera NATIVE_API_KEY
 *   xneog-agentd run       roda o daemon em foreground (porta 8802)
 *   xneog-agentd install   instala como serviço launchd (macOS) e inicia
 *   xneog-agentd status    checa /meta do daemon local
 *
 * Estado vive em ~/.xneog/agentd (sessions.json, transcripts/, devices.json, logs/).
 * Segredos vivem em ~/.xneog/env (0600). O daemon nunca vê ANTHROPIC_API_KEY —
 * engine claude usa o Claude Code do próprio usuário (login/plano dele).
 */
import { readFileSync, writeFileSync, existsSync, mkdirSync, copyFileSync } from "node:fs";
import { homedir } from "node:os";
import { randomBytes } from "node:crypto";
import { spawn, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HOME = homedir();
const XDIR = `${HOME}/.xneog`;
const ADIR = `${XDIR}/agentd`;
const ENVF = `${XDIR}/env`;
const PKG = dirname(dirname(fileURLToPath(import.meta.url)));
const DAEMON = join(PKG, "daemon", "xneog-code-bridge.mjs");
const PLIST = `${HOME}/Library/LaunchAgents/com.xneog.agentd.plist`;
const cmd = process.argv[2] || "help";

function loadEnv(p) {
  const o = {};
  if (existsSync(p)) for (const l of readFileSync(p, "utf8").split("\n")) {
    const m = l.match(/^\s*([A-Z_]+)\s*=\s*(.*)\s*$/);
    if (m) o[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
  return o;
}

function findClaude() {
  for (const p of [`${HOME}/.local/bin/claude`, "/usr/local/bin/claude", "/opt/homebrew/bin/claude"]) {
    if (existsSync(p)) return p;
  }
  try { return execFileSync("/usr/bin/which", ["claude"], { encoding: "utf8" }).trim() || null; }
  catch { return null; }
}

function envForDaemon() {
  return {
    ...process.env,
    XNEOG_AGENTD_DIR: ADIR,
    ...(findClaude() ? { XNEOG_CLAUDE_BIN: findClaude() } : {}),
  };
}

if (cmd === "init") {
  mkdirSync(ADIR, { recursive: true });
  const env = loadEnv(ENVF);
  if (!env.NATIVE_API_KEY) {
    const key = randomBytes(32).toString("hex");
    writeFileSync(ENVF, (existsSync(ENVF) ? readFileSync(ENVF, "utf8").replace(/\n?$/, "\n") : "") + `NATIVE_API_KEY=${key}\n`, { mode: 0o600 });
    console.log(`✓ NATIVE_API_KEY gerada em ${ENVF} (0600)`);
  } else console.log(`✓ NATIVE_API_KEY já existe em ${ENVF}`);
  for (const f of ["grok-jail.sb"]) {
    const dst = join(ADIR, f);
    if (!existsSync(dst)) copyFileSync(join(PKG, "daemon", f), dst);
  }
  const claude = findClaude();
  console.log(claude ? `✓ Claude Code encontrado: ${claude}` : "⚠ Claude Code não encontrado — engine claude indisponível até instalar (https://claude.com/claude-code)");
  console.log(`✓ estado em ${ADIR}`);
  console.log("\nPróximo: xneog-agentd run  (ou: xneog-agentd install para launchd)");
} else if (cmd === "run") {
  if (!loadEnv(ENVF).NATIVE_API_KEY) { console.error("rode primeiro: xneog-agentd init"); process.exit(1); }
  mkdirSync(ADIR, { recursive: true });
  const child = spawn(process.execPath, [DAEMON], { stdio: "inherit", env: envForDaemon() });
  child.on("exit", (c) => process.exit(c ?? 1));
} else if (cmd === "install") {
  if (process.platform !== "darwin") { console.error("install (launchd) é macOS-only; use `run` sob seu supervisor"); process.exit(1); }
  if (!loadEnv(ENVF).NATIVE_API_KEY) { console.error("rode primeiro: xneog-agentd init"); process.exit(1); }
  const envd = envForDaemon();
  const plist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.xneog.agentd</string>
  <key>ProgramArguments</key><array>
    <string>${process.execPath}</string>
    <string>${DAEMON}</string>
  </array>
  <key>EnvironmentVariables</key><dict>
    <key>XNEOG_AGENTD_DIR</key><string>${ADIR}</string>
    ${envd.XNEOG_CLAUDE_BIN ? `<key>XNEOG_CLAUDE_BIN</key><string>${envd.XNEOG_CLAUDE_BIN}</string>` : ""}
    <key>PATH</key><string>${HOME}/.local/bin:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>${ADIR}/logs/launchd.out.log</string>
  <key>StandardErrorPath</key><string>${ADIR}/logs/launchd.err.log</string>
</dict></plist>\n`;
  mkdirSync(`${ADIR}/logs`, { recursive: true });
  writeFileSync(PLIST, plist);
  try { execFileSync("launchctl", ["unload", PLIST], { stdio: "ignore" }); } catch {}
  execFileSync("launchctl", ["load", PLIST]);
  console.log(`✓ serviço com.xneog.agentd instalado e iniciado (${PLIST})`);
  console.log("verifique: xneog-agentd status");
} else if (cmd === "status") {
  const key = loadEnv(ENVF).NATIVE_API_KEY || "";
  try {
    const r = await fetch("http://127.0.0.1:8802/meta", { headers: { Authorization: `Bearer ${key}` } });
    console.log(`daemon :8802 → HTTP ${r.status}`);
    if (r.ok) console.log(JSON.stringify(await r.json(), null, 2));
  } catch { console.log("daemon :8802 → offline"); process.exit(1); }
} else {
  console.log("uso: xneog-agentd <init|run|install|status>");
}
