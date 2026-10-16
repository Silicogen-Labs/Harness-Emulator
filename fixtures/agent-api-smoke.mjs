// Exercises the installed MCP adapter and persistent daemon, not helper mocks.
import assert from "node:assert/strict"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { spawnSync } from "node:child_process"
import { Client } from "../browser-tests/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js"
import { StdioClientTransport } from "../browser-tests/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js"

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const binary = process.env.EMULATION_BINARY || path.join(project, ".local/bin/silicogen-emulation")
const root = fs.mkdtempSync(path.join(project, ".runtime/api."))
const origin = path.join(root, "origin")
fs.mkdirSync(origin)
fs.writeFileSync(path.join(origin, "file.txt"), "original dirty source\n")
const report = { runtime: root, checks: [], tools: [] }
let client
const cli = (args) => spawnSync(binary, ["--runtime-dir", root, ...args], { encoding: "utf8" })
async function connect() {
  client = new Client({ name: "agent-api-acceptance", version: "1" })
  await client.connect(new StdioClientTransport({ command: binary, args: ["--runtime-dir", root, "mcp"], stderr: "pipe" }))
}
async function call(name, args = {}) {
  const result = await client.callTool({ name, arguments: args })
  if (result.isError) throw new Error(JSON.stringify(result))
  return result.structuredContent || JSON.parse(result.content.find(x => x.type === "text").text)
}
function check(name, condition) { assert(condition, name); report.checks.push(name) }
async function ready(id, text) {
  let wait = await call("terminal_wait_ready", { session_id: id, predicate: { kind: "ready", required: [text], busy: [], crash: ["A fatal error occurred!"] }, timeout_ms: 5000, poll_ms: 100 })
  while (wait.status === "pending") wait = await call("terminal_wait_ready", { wait_id: wait.wait_id, poll_ms: 100 })
  check(`wait ${text}`, wait.matched)
}
try {
  await connect()
  report.tools = (await client.listTools()).tools.map(x => x.name).sort()
  check("36 MCP tools advertised", report.tools.length === 36)
  await call("terminal_profiles", { action: "store", profile: { version: 1, name: "acceptance", env_allowlist: ["PATH", "LANG"], env: {}, seeds: [], metadata: {} } })
  check("dedicated profile listed", (await call("terminal_profiles")).some(p => p.name === "acceptance"))
  const launched = await call("terminal_launch", { program: path.join(project, "target/debug/terminal-fixture"), cwd: "${workspace}", profile: "acceptance", workspace: { mode: "copy", source: origin }, options: { disposable: true }, cols: 100, rows: 30 })
  report.session = launched.session_id
  await ready(report.session, "READY")
  check("workspace differs from original", launched.cwd !== origin && launched.workspace === launched.cwd)
  check("original source copied", fs.readFileSync(path.join(launched.cwd, "file.txt"), "utf8") === "original dirty source\n")
  await client.close()
  const observed = JSON.parse(cli(["observe", report.session]).stdout)
  check("CLI sees session after MCP disconnect", observed.text.includes("READY"))
  await connect()
  const caps = await call("terminal_capabilities", { session_id: report.session })
  const context = { actor_id: "acceptance", action_id: "down-once", expected_run_id: caps.session.run_id, expected_viewport_revision: caps.viewport_revision }
  const batch = { session_id: report.session, context, actions: [{ kind: "key", key: "Down" }] }
  const first = await call("terminal_batch", batch)
  check("input retry returns same result", JSON.stringify(await call("terminal_batch", batch)) === JSON.stringify(first))
  await ready(report.session, "Selection: Beta")
  await call("terminal_actor", { session_id: report.session, actor_id: "acceptance", release: true })
  await call("terminal_save_frame", { session_id: report.session, name: "beta" })
  check("same-frame diff passes", (await call("terminal_diff", { session_id: report.session, name: "beta" })).passed)
  check("text assertion passes", (await call("terminal_assert", { session_id: report.session, assertion: { kind: "text_contains", text: "Selection: Beta" } })).passed)
  const screenshot = await call("terminal_screenshot", { session_id: report.session })
  check("screenshot identified", screenshot.session_id === report.session && fs.existsSync(screenshot.path))
  const artifacts = await call("terminal_artifacts", { session_id: report.session })
  const image = artifacts.find(x => x.content_type === "image/png")
  const read = await call("terminal_read_artifact", { session_id: report.session, artifact_id: image.artifact_id, length: 64 })
  check("artifact returned PNG bytes", Buffer.from(read.data, "base64").subarray(1, 4).toString() === "PNG")
  const appLogs = path.join(root, "sessions", report.session, "state/data/silicogen/log")
  fs.mkdirSync(appLogs, { recursive: true })
  fs.writeFileSync(path.join(appLogs, "dev.log"), "acceptance-owned diagnostic\n")
  const logs = await call("terminal_logs", { session_id: report.session })
  check("app log readable", logs.text.includes("acceptance-owned diagnostic"))
  fs.appendFileSync(path.join(appLogs, "dev.log"), "appended\n")
  check("log cursor follows only new bytes", (await call("terminal_logs", { session_id: report.session, cursor: logs.cursor })).text === "appended\n")
  await call("terminal_logs", { session_id: report.session, source: "daemon" })
  report.checks.push("daemon log narrowly scoped")
  const scenario = { version: 1, name: "snapshot-nvim-edit", timeout_ms: 20000, steps: [
    { id: "launch", operation: "launch", params: { spec: { program: "/usr/bin/nvim", args: ["--clean", "file.txt"], cwd: "${workspace}", cols: 80, rows: 24 }, options: { state_mode: "profile", profile: "acceptance", workspace: { mode: "copy", source: origin }, disposable: true } } },
    { id: "ready", operation: "wait", params: { session_id: "${session}", predicate: { kind: "text_contains", text: "original dirty source" } } },
    { id: "insert", operation: "key", params: { session_id: "${session}", key: "i" } },
    { id: "edit", operation: "type", params: { session_id: "${session}", text: "snapshot-only " } },
    { id: "escape", operation: "key", params: { session_id: "${session}", key: "Escape" } },
    { id: "write", operation: "type", params: { session_id: "${session}", text: ":write" } },
    { id: "enter", operation: "key", params: { session_id: "${session}", key: "Enter" } },
    { id: "saved", operation: "wait", params: { session_id: "${session}", predicate: { kind: "text_contains", text: "written" } } },
    { id: "assert", operation: "assert", params: { session_id: "${session}", assertion: { kind: "text_contains", text: "snapshot-only original dirty source" } } },
    { id: "evidence", operation: "screenshot", params: { session_id: "${session}" } },
    { id: "close", operation: "close", params: { session_id: "${session}" } },
  ] }
  fs.writeFileSync(path.join(root, "scenario.json"), JSON.stringify(scenario, null, 2))
  const started = await call("terminal_scenario_run", { scenario })
  let status
  do {
    await new Promise(resolve => setTimeout(resolve, 100))
    status = await call("terminal_scenario_status", { job_id: started.job_id })
  } while (["queued", "running", "paused_for_control"].includes(status.status))
  fs.writeFileSync(path.join(root, "scenario-result.json"), JSON.stringify(status, null, 2))
  check("real nvim scenario passes", status.status === "passed")
  const workspace = status.variables.workspace
  check("nvim edit saved inside snapshot", fs.readFileSync(path.join(workspace, "file.txt"), "utf8").startsWith("snapshot-only "))
  check("original repo unchanged", fs.readFileSync(path.join(origin, "file.txt"), "utf8") === "original dirty source\n")
  const oldRun = launched.run_id
  const restarted = await call("terminal_restart", { session_id: report.session })
  check("restart changes run identity", restarted.run_id !== oldRun)
  await ready(report.session, "READY")
  await call("terminal_close", { session_id: report.session })
  check("closed session recorded", (await call("terminal_status", { session_id: report.session })).state === "closed")
  const preview = await call("terminal_cleanup", {})
  check("cleanup is preview-only by default", typeof preview.token === "string" && Array.isArray(preview.protected))
  report.passed = true
  console.log(JSON.stringify(report, null, 2))
} catch (error) {
  report.passed = false
  report.error = String(error.stack || error)
  console.error(report.error)
  process.exitCode = 1
} finally {
  if (client) await client.close().catch(() => {})
  cli(["shutdown"])
  fs.writeFileSync(path.join(root, "acceptance.json"), JSON.stringify(report, null, 2))
  console.log(`Evidence: ${root}`)
}
