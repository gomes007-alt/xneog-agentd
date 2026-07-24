#!/usr/bin/env node
/**
 * mcp-approval — MCP server stdio mínimo usado como --permission-prompt-tool das sessões do bridge.
 * Quando o claude precisa de permissão pra uma tool fora da allowlist, chama `approve` aqui;
 * este server repassa pro bridge (/internal/approval), que segura até o owner decidir no app
 * (ou 120s → deny fail-closed). Retorna {behavior:"allow"|"deny"} pro claude.
 */
import { createInterface } from "node:readline";

const PORT = process.env.BRIDGE_PORT || "8802";
const KEY = process.env.BRIDGE_KEY || "";
const SESSION = process.env.BRIDGE_SESSION || "";
const APPROVAL_SECRET = process.env.APPROVAL_SECRET || "";   // 2ª parede: só quem o bridge spawna tem
const out = (o) => process.stdout.write(JSON.stringify(o) + "\n");

createInterface({ input: process.stdin }).on("line", async (l) => {
  let m; try { m = JSON.parse(l); } catch { return; }
  if (m.method === "initialize") {
    return out({ jsonrpc: "2.0", id: m.id, result: { protocolVersion: m.params?.protocolVersion || "2024-11-05", capabilities: { tools: {} }, serverInfo: { name: "xneog-approver", version: "1.0.0" } } });
  }
  if (m.method === "tools/list") {
    return out({ jsonrpc: "2.0", id: m.id, result: { tools: [{
      name: "approve",
      description: "Pede aprovação do owner (app xNeog) antes de executar uma ferramenta.",
      inputSchema: { type: "object", properties: { tool_name: { type: "string" }, input: { type: "object" }, tool_use_id: { type: "string" } }, required: ["tool_name", "input"] },
    }] } });
  }
  if (m.method === "tools/call") {
    const a = m.params?.arguments || {};
    let decision = { behavior: "deny", message: "fila de aprovação indisponível" };
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}/internal/approval`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${KEY}`, "x-approval-secret": APPROVAL_SECRET },
        body: JSON.stringify({ session: SESSION, tool_name: a.tool_name, input: a.input, tool_use_id: a.tool_use_id }),
        signal: AbortSignal.timeout(130000),   // cinto de segurança fail-closed: acima dos 120s do bridge → deny
      });
      decision = await r.json();
    } catch {}
    return out({ jsonrpc: "2.0", id: m.id, result: { content: [{ type: "text", text: JSON.stringify(decision) }] } });
  }
  if (m.id !== undefined) out({ jsonrpc: "2.0", id: m.id, result: {} });
});
