// Real Silicogen source TUI, synthetic provider/account data, no live model calls.
// Intentionally observes the known Select skill crash and verifies recovery; it
// does not repair or modify the sibling Silicogen source checkout.
import assert from "node:assert/strict"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { spawnSync } from "node:child_process"
import { Client } from "../browser-tests/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js"
import { StdioClientTransport } from "../browser-tests/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js"

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const binary = process.env.EMULATION_BINARY || path.join(root, "target/release/silicogen-emulation")
const demo = spawnSync("bash", [path.join(root, "fixtures/run-silicogen-subagents-demo.sh")], { cwd: root, env: process.env, encoding: "utf8", timeout: 60000, maxBuffer: 4 * 1024 * 1024 })
assert.equal(demo.status, 0, demo.stderr || demo.stdout)
const runtime = demo.stdout.match(/^Runtime data:\s+(.+)$/m)[1]
const originalId = demo.stdout.match(/^TUI session:\s+(\S+)$/m)[1]
const client = new Client({ name: "silicogen-agent-acceptance", version: "1" })
let report = { runtime, checks: [] }
async function call(name, args = {}) {
  const result = await client.callTool({ name, arguments: args })
  if (result.isError) throw new Error(JSON.stringify(result))
  return result.structuredContent || JSON.parse(result.content.find(p => p.type === "text").text)
}
try {
  await client.connect(new StdioClientTransport({ command: binary, args: ["--runtime-dir", runtime, "mcp"], stderr: "pipe" }))
  const tools = new Set((await client.listTools()).tools.map(tool => tool.name))
  const requiredTools = ["terminal_launch", "terminal_observe", "terminal_wait_ready", "terminal_batch", "terminal_assert", "terminal_finalize", "terminal_scenario_run", "terminal_cleanup"]
  assert(requiredTools.every(name => tools.has(name)), `missing required MCP tools: ${requiredTools.filter(name => !tools.has(name)).join(", ")}`)
  const old = await call("terminal_status", { session_id: originalId })
  await call("terminal_close", { session_id: originalId })
  const source = process.env.SILICOGEN_SOURCE_DIR || path.resolve(root, "../Silicogen-AI")
  const launched = await call("terminal_launch", {
    name: "Silicogen agent acceptance", program: old.program, args: old.args, cwd: old.cwd,
    profile: "silicogen-fixture", cols: 120, rows: 40,
    env: { EMULATION_FIXTURE_API_KEY: "placeholder-never-used", SILICOGEN_SOURCE_DIR: source, SILICOGEN_STARTUP_TRACE: "1", SILICOGEN_DISABLE_MODELS_FETCH: "1", SILICOGEN_DISABLE_REMOTE_SKILLS: "1" },
    options: { disposable: true },
  })
  const id = launched.session_id
  report.session_id = id
  assert.equal(launched.capabilities.launch.state_mode, "profile")
  const contract = await call("terminal_observe", { session_id: id, response: { mode: "summary" } })
  assert.equal(contract.response?.contract_revision, 2, "MCP projection contract revision 2 must be negotiated")
  const step = (id, operation, params, expect) => ({ id, operation, params: { session_id: launched.session_id, ...params }, ...(expect ? { expect } : {}) })
  const key = (id, key) => step(id, "key", { key })
  const wait = (id, text) => step(id, "wait", { predicate: { kind: "text_contains", text }, timeout_ms: 15000, trace_stage: id === "startup" || id === "recovered" ? "tui-complete" : undefined })
  const command = (id, title) => [key(`${id}-palette`, "Ctrl+P"), step(`${id}-filter`, "type", { text: title }), wait(`${id}-visible`, title), key(`${id}-choose`, "Enter")]
  const scenario = { version: 1, name: "real-silicogen-profile-logs-sidebar-mcp-crash-restart", timeout_ms: 120000, close_on_failure: false, steps: [
    wait("startup", "Message..."),
    step("trace", "logs", { source: "startup", max_bytes: 65536 }, { pointer: "/text", truthy: true }),
    ...command("sidebar", "Open sidebar panel"), wait("sidebar-open-checked", "Usage"),
    step("sidebar-evidence", "screenshot", {}),
    ...command("close-sidebar", "Close sidebar panel"),
    ...command("mcp", "Toggle MCPs"), wait("fixture-mcp-visible", "fixture-toggle"),
    key("enable-fixture-mcp", "Space"), wait("fixture-mcp-connected", "✓ Enabled"),
    step("assert-chip", "assert", { assertion: { kind: "text_contains", text: "fixture-toggle connected" } }),
    step("save-frame", "save_frame", { name: "mcp-chip-${scenario_id}" }),
    step("diff-frame", "diff", { name: "mcp-chip-${scenario_id}" }),
    step("mcp-evidence", "screenshot", {}),
    key("disable-fixture-mcp", "Space"), wait("fixture-mcp-disabled", "○ Disabled"),
    key("close-mcp", "Escape"),
    ...command("expected-app-crash", "Select skill"),
    wait("crash-observed", "PromptRef context must be used within a context provider"),
    step("crash-evidence", "screenshot", {}),
    step("crash-logs", "logs", { source: "app", max_bytes: 65536 }),
    step("restart-once", "restart", {}),
    wait("recovered", "Message..."),
    step("recovered-evidence", "screenshot", {}),
    step("close", "close", {}),
  ] }
  fs.writeFileSync(path.join(runtime, "acceptance-scenario.json"), JSON.stringify(scenario, null, 2))
  const started = await call("terminal_scenario_run", { scenario })
  let status
  do {
    await new Promise(resolve => setTimeout(resolve, 100))
    status = await call("terminal_scenario_status", { job_id: started.job_id })
  } while (["queued", "running", "paused_for_control"].includes(status.status))
  report.scenario_id = started.job_id
  fs.writeFileSync(path.join(runtime, "acceptance-scenario-result.json"), JSON.stringify(status, null, 2))
  assert.equal(status.status, "passed", status.error || JSON.stringify(status.steps.at(-1)))
  const restart = status.steps.find(s => s.step_id === "restart-once")
  assert.notEqual(restart.run_id, launched.run_id)
  assert.equal(status.steps.filter(s => s.operation === "restart").length, 1)
  assert.equal((await call("terminal_status", { session_id: id })).state, "closed")
  report.checks = ["MCP launch with dedicated profile", "actual startup trace via MCP", "sidebar terminal interaction", "local MCP enabled chip", "immutable frame diff", "known app crash observed", "one restart and new run identity", "durable scenario passed", "session closed"]
  report.passed = true
  console.log(JSON.stringify(report, null, 2))
} catch (error) {
  report.passed = false
  report.error = String(error.stack || error)
  console.error(report.error)
  process.exitCode = 1
  if (report.session_id) {
    const screen = await call("terminal_observe", {session_id: report.session_id}).catch(() => null)
    if (screen) fs.writeFileSync(path.join(runtime,"failure-frame.json"),JSON.stringify(screen,null,2))
    await call("terminal_screenshot", {session_id: report.session_id}).catch(() => {})
    console.error(screen?.text || "No failure frame available")
  }
} finally {
  await client.close().catch(() => {})
  spawnSync(binary, ["--runtime-dir", runtime, "shutdown"], { encoding: "utf8" })
  fs.writeFileSync(path.join(runtime, "acceptance.json"), JSON.stringify(report, null, 2))
  console.log(`Evidence: ${runtime}`)
}
