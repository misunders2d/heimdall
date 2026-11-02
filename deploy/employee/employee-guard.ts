// Install administrator-owned under /opt/assistant/heimdall. No memory extension.
import { realpathSync } from "node:fs";
import { resolve, relative, isAbsolute } from "node:path";
export default function (pi: any) {
  pi.on("tool_call", async (event: any) => {
    if (process.env.PI_RESTRICTED_EXTERNAL_USER !== "1") return;
    if (event.toolName === "read") {
      try {
        const target = realpathSync(resolve(event.input.path));
        for (const root of ["/opt/assistant", process.env.HEIMDALL_TURN_DIR].filter(Boolean)) {
          const rel = relative(realpathSync(root!), target);
          if (rel !== ".." && !rel.startsWith("../") && !isAbsolute(rel)) return;
        }
      } catch {}
    }
    return { block: true, reason: "Employee reader is confined to shared skills and this turn" };
  });
}
