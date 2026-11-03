// Install administrator-owned under /opt/assistant/heimdall. No memory extension.
import { realpathSync } from "node:fs";
import { resolve, relative, isAbsolute } from "node:path";
export default function (pi: any, authorizedMCP: (event: any) => boolean = () => false) {
  pi.on("tool_call", async (event: any) => {
    if (process.env.PI_RESTRICTED_EXTERNAL_USER !== "1") return;
    if (authorizedMCP(event)) return;
    if (event.toolName === "read") {
      try {
        const target = realpathSync(resolve(event.input.path));
        if (/(^|\/)(auth\.json|mcp-auth\.json|settings\.json|mcp\.json|.*\.lock)$/.test(target)) throw new Error("Credential/config file denied");
        for (const root of ["/opt/assistant", process.env.HEIMDALL_TURN_DIR].filter(Boolean)) {
          const rel = relative(realpathSync(root!), target);
          if (rel !== ".." && !rel.startsWith("../") && !isAbsolute(rel)) return;
        }
      } catch {}
    }
    return { block: true, reason: "Employee reader is confined to shared skills and this turn" };
  });
}
