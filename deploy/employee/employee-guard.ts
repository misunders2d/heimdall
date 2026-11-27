// Install administrator-owned under /opt/assistant/heimdall. No memory extension.
import { realpathSync } from "node:fs";
import { resolve, relative, isAbsolute } from "node:path";

// OMP loads this guard directly (no authorizer passed). Business MCP tools are
// admitted only for the servers Heimdall listed in HEIMDALL_EMPLOYEE_MCP (set
// outside model control); OMP names them mcp__<server>_<tool>.
const businessServers = ["mellanni", "linear"];
export function approvedMCP(event: any): boolean {
  let approved: unknown;
  try { approved = JSON.parse(process.env.HEIMDALL_EMPLOYEE_MCP || "[]"); } catch { return false; }
  if (!Array.isArray(approved)) return false;
  const tool = String(event?.toolName || "");
  return approved.some(s => businessServers.includes(s) && tool.startsWith("mcp__" + s + "_") && tool.length > ("mcp__" + s + "_").length);
}

export default function (pi: any, authorizedMCP: (event: any) => boolean = approvedMCP) {
  pi.on("tool_call", async (event: any) => {
    if (process.env.PI_RESTRICTED_EXTERNAL_USER !== "1") return;
    if (authorizedMCP(event)) return;
    if (event.toolName === "read") {
      try {
        const target = realpathSync(resolve(event.input.path));
        if (/(^|\/)(google-access\.json|auth\.json|\.credentials\.json|agent\.db|mcp-auth\.json|settings\.json|mcp\.json|.*\.lock)$/.test(target)) throw new Error("Credential/config file denied");
        for (const root of ["/opt/assistant", process.env.HEIMDALL_TURN_DIR].filter(Boolean)) {
          const rel = relative(realpathSync(root!), target);
          if (rel !== ".." && !rel.startsWith("../") && !isAbsolute(rel)) return;
        }
      } catch {}
    }
    return { block: true, reason: "Employee reader is confined to shared skills and this turn" };
  });
}
