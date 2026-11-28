import { spawn } from "node:child_process"
import { existsSync } from "node:fs"
import { join } from "node:path"

import { Plugin } from "@opencode/plugin"

type HookEvent = "pre-tool" | "post-tool"

type HookResult = {
  code: number
  stdout: string
  stderr: string
}

type ToolAfterEvent = {
  tool: string
  status: "completed" | "error"
  input?: unknown
  result?: {
    metadata?: Record<string, unknown>
    [key: string]: unknown
  }
  error?: { message?: string }
}

const TRIGGER = /\b(?:git\s+(?:commit|push)|gh\s+pr\s+create)\b/i
const EDIT_TOOLS = new Set(["edit", "write", "multiedit", "patch", "apply_patch"])

function cliAvailable(cwd: string): boolean {
  return existsSync(join(cwd, "tools", "privacy_guard", "__main__.py"))
}

function runHook(cwd: string, event: HookEvent, payload: unknown): Promise<HookResult> {
  if (!cliAvailable(cwd)) {
    return Promise.resolve({ code: 0, stdout: "", stderr: "" })
  }

  return new Promise((resolve) => {
    const child = spawn(
      "uv",
      ["run", "python", "-m", "tools.privacy_guard", "hook", "--harness", "opencode", "--event", event],
      { cwd, stdio: ["pipe", "pipe", "pipe"] },
    )

    let stdout = ""
    let stderr = ""

    child.stdout?.setEncoding("utf8")
    child.stderr?.setEncoding("utf8")
    child.stdout?.on("data", (chunk: string) => {
      stdout += chunk
    })
    child.stderr?.on("data", (chunk: string) => {
      stderr += chunk
    })

    child.on("error", (error) => {
      console.warn(`[privacy-guard] unable to run the privacy guard: ${error.message}`)
      resolve({ code: 0, stdout, stderr })
    })

    child.on("close", (code) => {
      resolve({ code: code ?? 0, stdout, stderr })
    })

    child.stdin?.on("error", () => {})
    child.stdin?.end(JSON.stringify(payload ?? {}))
  })
}

function outputMessage(result: HookResult): string {
  const raw = (result.stderr || result.stdout).trim()
  if (!raw) {
    return ""
  }
  try {
    const parsed = JSON.parse(raw) as { reason?: unknown }
    if (typeof parsed.reason === "string" && parsed.reason.trim()) {
      return parsed.reason.trim()
    }
  } catch {
    // Not every harness emits JSON on stdout.
  }
  return raw
}

export default Plugin.define({
  id: "privacy-guard",
  async setup(ctx) {
    const cwd = ctx.location.directory

    await ctx.permission.hook("evaluate", async (event) => {
      const resources = event.resources ?? []
      const command = resources.join(" ")
      const isShell = event.action === "shell" || event.action === "bash"

      if (!isShell || !TRIGGER.test(command)) {
        return
      }

      const result = await runHook(cwd, "pre-tool", {
        action: event.action,
        command,
        resources,
        sessionID: event.sessionID,
        agent: event.agent,
        metadata: event.metadata,
      })

      if (result.code === 2) {
        event.effect = "deny"
        event.message =
          outputMessage(result) || "Blocked by privacy-guard: this command may expose customer data."
      }
    })

    await ctx.tool.hook("execute.after", async (rawEvent) => {
      const event = rawEvent as unknown as ToolAfterEvent
      const tool = typeof event.tool === "string" ? event.tool.toLowerCase() : ""

      if (!EDIT_TOOLS.has(tool)) {
        return
      }

      const input = event.input && typeof event.input === "object" ? (event.input as Record<string, unknown>) : {}
      const filePath = [input.filePath, input.file_path, input.path].find((value): value is string => typeof value === "string")

      const result = await runHook(cwd, "post-tool", {
        tool,
        status: event.status,
        input,
        file_path: filePath,
        result: event.status === "completed" ? event.result : undefined,
        error: event.status === "error" ? event.error : undefined,
      })

      const message = outputMessage(result)
      if (!message) {
        return
      }

      console.warn(`[privacy-guard] ${message}`)

      if (event.status === "completed") {
        ;(rawEvent as { result: ToolAfterEvent["result"] }).result = {
          ...event.result,
          metadata: { ...(event.result?.metadata ?? {}), "privacy-guard": message },
        }
      }
    })
  },
})
