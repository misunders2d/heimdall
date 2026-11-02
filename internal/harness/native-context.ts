// Pi 1.4.0 native events. Metadata never becomes a native user/custom message.
import { readdirSync, readFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";

export default function (pi: any) {
  const dir = process.env.HD_CONTEXT_DIR;
  let context: string[] = [];
  pi.on("before_agent_start", () => { context = []; });
  pi.on("message_end", (event: any) => {
    if (!dir || event.message.role !== "user") return;
    const content = event.message.content;
    const text = typeof content === "string" ? content : content.filter((p: any) => p.type === "text").map((p: any) => p.text).join("");
    const prefix = createHash("sha256").update(text).digest("hex") + "-";
    for (const name of readdirSync(dir).sort()) {
      if (!name.startsWith(prefix) || !name.endsWith(".json")) continue;
      const path = join(dir, name);
      const entry = JSON.parse(readFileSync(path, "utf8"));
      if (entry.prompt !== text) continue;
      unlinkSync(path);
      context.push(entry.context);
      break;
    }
  });
  pi.on("context_with_system", (event: any) => {
    if (!context.length) return;
    const messages = [...event.messages];
    const first = messages[0];
    if (first?.role !== "system") throw new Error("Heimdall requires native system context");
    const addition = "\n\n" + context.join("\n\n");
    messages[0] = { ...first, content: typeof first.content === "string" ? first.content + addition : [...first.content, { type: "text", text: addition }] };
    return { messages };
  });
}
