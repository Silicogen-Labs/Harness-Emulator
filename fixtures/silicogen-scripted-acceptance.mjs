#!/usr/bin/env node
import assert from "node:assert/strict"
import crypto from "node:crypto"
import fs from "node:fs"
import path from "node:path"
import { spawnSync } from "node:child_process"
import { setTimeout as delay } from "node:timers/promises"
import { fileURLToPath } from "node:url"
import { Client } from "../browser-tests/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js"
import { StdioClientTransport } from "../browser-tests/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js"

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const sourceRoot = path.resolve(process.env.SILICOGEN_SOURCE_DIR || path.join(root, "../Silicogen-AI"))
const packageRoot = path.join(sourceRoot, "packages/silicogen")
const binary = path.resolve(process.env.EMULATION_BINARY || path.join(root, "target/debug/silicogen-emulation"))
const bun = process.env.BUN || "bun"
const runtimeDir = path.join(root, ".runtime", `silicogen-scripted-acceptance-${process.pid}-${crypto.randomBytes(4).toString("hex")}`)
const apiKey = "synthetic-acceptance-key-never-valid-for-network-use"
const report = {
  startedAt: new Date().toISOString(),
  runtimeDir,
  sourceRoot,
  binary,
  binaryVersion: "unavailable",
  binarySha256: "unavailable",
  scenarios: [],
  checks: [],
  failures: [],
  fixtureProtocol: "OpenAI-compatible POST /v1/chat/completions only",
}

let client
let transport
const sessions = new Set()
const instances = []

function record(name, detail = {}) {
  report.checks.push({ name, ...detail })
  console.log(`PASS ${name}`)
}

function decode(result) {
  if (result?.isError) throw new Error(JSON.stringify(result))
  if (result?.structuredContent) return result.structuredContent
  const block = result?.content?.find((part) => part.type === "text")
  if (!block) return result
  try { return JSON.parse(block.text) } catch { return block.text }
}

async function call(name, args = {}) {
  return decode(await client.callTool({ name, arguments: args }))
}

async function waitFor(predicate, { timeoutMs = 12000, pollMs = 100, label = "condition" } = {}) {
  const until = Date.now() + timeoutMs
  let last
  while (Date.now() < until) {
    last = await predicate()
    if (last) return last
    await delay(pollMs)
  }
  throw new Error(`timed out waiting for ${label}; last=${JSON.stringify(last)}`)
}

async function appRequest(bridgeDirectory, directory, route, options = {}) {
  const url = new URL(route, "http://acceptance-bridge.invalid")
  url.searchParams.set("directory", directory)
  const requestId = crypto.randomUUID()
  const requestPath = path.join(bridgeDirectory, `${requestId}.request.json`)
  const temporaryPath = `${requestPath}.tmp`
  const responsePath = path.join(bridgeDirectory, `${requestId}.response.json`)
  fs.writeFileSync(temporaryPath, `${JSON.stringify({
    id: requestId,
    directory,
    method: options.method || "GET",
    route: `${url.pathname}${url.search}`,
    body: options.body ?? null,
  })}\n`, { flag: "wx", mode: 0o600 })
  fs.renameSync(temporaryPath, requestPath)
  const deadline = Date.now() + 10000
  while (Date.now() < deadline && !fs.existsSync(responsePath)) await delay(20)
  if (!fs.existsSync(responsePath)) throw new Error(`acceptance file bridge timed out for ${options.method || "GET"} ${url.pathname}`)
  const response = JSON.parse(fs.readFileSync(responsePath, "utf8"))
  fs.unlinkSync(responsePath)
  if (response.id !== requestId || typeof response.text !== "string") throw new Error("invalid acceptance bridge response envelope")
  if (Buffer.byteLength(response.text) > 2 * 1024 * 1024) throw new Error("acceptance bridge response exceeded 2 MiB")
  let parsedBody
  try { parsedBody = response.text ? JSON.parse(response.text) : null } catch { parsedBody = response.text }
  return { status: response.status, body: parsedBody, text: response.text }
}

async function listSessions(instance) {
  const result = await appRequest(instance.apiBridgeDirectory, instance.workspace, "/session")
  if (result.status !== 200 || !Array.isArray(result.body)) return []
  return result.body
}

async function getMessages(instance, sessionId) {
  const result = await appRequest(instance.apiBridgeDirectory, instance.workspace, `/session/${encodeURIComponent(sessionId)}/message`)
  if (result.status !== 200 || !Array.isArray(result.body)) {
    throw new Error(`messages API returned ${result.status}: ${JSON.stringify(result.body)}`)
  }
  return result.body
}

async function observe(sessionId) {
  return call("terminal_observe", { session_id: sessionId, response: { mode: "text", max_bytes: 12000 } })
}

async function screenText(sessionId) {
  const value = await observe(sessionId)
  return value.text ?? value.snapshot?.text ?? JSON.stringify(value)
}

async function choosePort() {
  const server = await import("node:net").then(({ createServer }) => createServer())
  await new Promise((resolve, reject) => server.once("error", reject).listen(0, "127.0.0.1", resolve))
  const { port } = server.address()
  await new Promise((resolve) => server.close(resolve))
  return port
}

async function waitTerminalText(sessionId, text, timeoutMs = 12000) {
  let result = await call("terminal_wait_ready", {
    session_id: sessionId,
    predicate: { kind: "text_contains", text },
    timeout_ms: timeoutMs,
    poll_ms: 100,
    response: { mode: "text", max_bytes: 12000 },
  })
  while (result.status === "pending") {
    result = await call("terminal_wait_ready", {
      wait_id: result.wait_id,
      poll_ms: 100,
      response: { mode: "text", max_bytes: 12000 },
    })
  }
  if (!result.matched) throw new Error(`TUI wait failed for ${JSON.stringify(text)}: ${JSON.stringify(result)}`)
  return result
}

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`)
}

function ownedDaemonPids() {
  const result = spawnSync("ps", ["-eo", "pid=,args="], { encoding: "utf8", timeout: 3000 })
  if (result.status !== 0) throw new Error(`could not inspect owned runtime process: ${result.stderr || result.error}`)
  const marker = `${binary} daemon --runtime-dir ${runtimeDir}`
  return result.stdout.split("\n").flatMap((line) => {
    const match = line.trim().match(/^(\d+)\s+(.*)$/)
    return match && match[2].includes(marker) ? [Number(match[1])] : []
  })
}

async function stopOwnedDaemon() {
  const shutdown = spawnSync(binary, ["--runtime-dir", runtimeDir, "shutdown"], { encoding: "utf8", timeout: 10000 })
  const signalled = []
  for (const pid of ownedDaemonPids()) {
    signalled.push(pid)
    try { process.kill(pid, "SIGTERM") } catch (error) { if (error?.code !== "ESRCH") throw error }
  }
  const deadline = Date.now() + 2500
  while (Date.now() < deadline && ownedDaemonPids().length) await delay(50)
  const remaining = ownedDaemonPids()
  return { shutdownStatus: shutdown.status, shutdownError: shutdown.error ? String(shutdown.error) : undefined, signalled, remaining }
}

async function setupProfile() {
  const profile = await call("terminal_profiles", { action: "template" })
  profile.name = "scripted-provider-acceptance"
  profile.description = "Controlled Silicogen scripted provider inside Bubblewrap network=none; synthetic credentials; independent copied project/state"
  profile.env_allowlist = ["PATH", "LANG", "LC_ALL", "USER", "LOGNAME"]
  profile.env = {}
  profile.seeds = []
  profile.test_policy.isolation.backend = "bubblewrap"
  profile.test_policy.isolation.network = "none"
  profile.test_policy.dispose_on_pass = false
  await call("terminal_profiles", { action: "store", profile })
  record("dedicated profile enforces Bubblewrap network=none with synthetic-only environment")
}

function newProject(name, scriptFile) {
  const dir = path.join(runtimeDir, name)
  fs.mkdirSync(dir, { recursive: true })
  const configPath = path.join(dir, "silicogen.json")
  const config = JSON.parse(fs.readFileSync(path.join(root, "fixtures/silicogen-scripted-provider-config.json"), "utf8"))
  config.provider["emulation-fixture"].api = "http://127.0.0.1:1/v1"
  config.disabled_providers = [...new Set([...(config.disabled_providers ?? []), "azure"])]
  config.permission = { edit: "allow", bash: "deny", webfetch: "deny" }
  config.experimental = { ...(config.experimental ?? {}), disable_paste_summary: true }
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`)
  fs.writeFileSync(path.join(dir, "acceptance-input.txt"), "Synthetic input file owned by this acceptance run.\n")
  fs.writeFileSync(path.join(dir, ".acceptance-scenario"), scriptFile)
  return dir
}

async function startApp(name, scriptFile) {
  const project = newProject(`${name}-project`, scriptFile)
  const providerScript = path.join(root, "fixtures/provider-scripts", scriptFile)
  assert.ok(fs.existsSync(providerScript), `missing provider script ${providerScript}`)
  const port = await choosePort()
  const launched = await call("terminal_launch", {
    name: `${name} real Silicogen TUI and backend`,
    program: bun,
    args: [
      "run", "--cwd", packageRoot, "--conditions=browser",
      path.join(root, "fixtures/silicogen-runtime-wrapper.mjs"),
      "${workspace}", "--fixture-auth", path.join(packageRoot, "src/index.ts"),
      "--hostname", "127.0.0.1", "--port", String(port),
    ],
    cwd: "${workspace}",
    profile: "scripted-provider-acceptance",
    workspace: { mode: "copy", source: project, exclude: [] },
    env: {
      PATH: process.env.PATH,
      LANG: process.env.LANG || "C.UTF-8",
      SILICOGEN_SOURCE_DIR: sourceRoot,
      SILICOGEN_STARTUP_TRACE: "1",
      SILICOGEN_DISABLE_MODELS_FETCH: "1",
      SILICOGEN_DISABLE_REMOTE_SKILLS: "1",
      EMULATION_FIXTURE_API_KEY: apiKey,
      SILICOGEN_FIXTURE_PROVIDER_SCRIPT: providerScript,
      SILICOGEN_ACCEPTANCE_API_PORT: String(port),
      CI: "1",
    },
    cols: 120,
    rows: 38,
    record_output: true,
  })
  sessions.add(launched.session_id)
  const workspace = launched.workspace ?? launched.cwd
  const launchDiagnosticsPath = path.join(runtimeDir, "sessions", launched.session_id, "launch-diagnostics.json")
  const launchDiagnostics = JSON.parse(fs.readFileSync(launchDiagnosticsPath, "utf8"))
  assert.equal(launchDiagnostics.isolation?.backend, "bubblewrap")
  assert.equal(launchDiagnostics.isolation?.network, "none")
  assert.equal(launchDiagnostics.isolation?.network_confined, true)
  assert.equal(Object.values(launchDiagnostics.environment_sources ?? {}).includes("daemon_ambient"), false)
  record(`${name}: launch diagnostics prove network=none and no daemon-ambient environment`, {
    backend: launchDiagnostics.isolation.backend,
    network: launchDiagnostics.isolation.network,
    inheritedEnvironmentNames: Object.entries(launchDiagnostics.environment_sources ?? {}).filter(([, source]) => source === "daemon_ambient").map(([key]) => key),
  })
  const apiBridgeDirectory = path.join(workspace, ".runtime", "acceptance-api")
  let lastApiProbe
  try {
    await waitFor(async () => {
      const appLogPath = path.join(runtimeDir, "sessions", launched.session_id, "state", "data", "silicogen", "log", "dev.log")
      const currentLog = fs.existsSync(appLogPath) ? fs.readFileSync(appLogPath, "utf8").slice(-6000) : ""
      if (currentLog.includes("tui bootstrap failed") || currentLog.includes("provider.id")) {
        throw new Error(`Silicogen TUI bootstrap failed before acceptance readiness; app_log_tail=${currentLog}`)
      }
      if (!fs.existsSync(path.join(apiBridgeDirectory, "ready"))) return null
      try {
        const response = await appRequest(apiBridgeDirectory, workspace, "/session")
        lastApiProbe = { status: response.status, body: response.body }
        return response.status === 200 && Array.isArray(response.body) ? response : null
      } catch (error) {
        if (["ENOENT", "ECONNREFUSED", "ECONNRESET"].includes(error?.code)) return null
        throw error
      }
    }, { timeoutMs: 25000, pollMs: 200, label: `${name} local Silicogen session API` })
  } catch (error) {
    const status = await call("terminal_status", { session_id: launched.session_id }).catch(() => null)
    const logPath = path.join(runtimeDir, "sessions", launched.session_id, "state", "data", "silicogen", "log", "dev.log")
    const appLogTail = fs.existsSync(logPath) ? fs.readFileSync(logPath, "utf8").slice(-6000) : "unavailable"
    throw new Error(`${error.message}; session_status=${JSON.stringify(status)}; last_api_probe=${JSON.stringify(lastApiProbe)}; app_log_tail=${appLogTail}`)
  }
  await waitTerminalText(launched.session_id, "Ask anything...", 20000)
  const initial = await screenText(launched.session_id)
  fs.writeFileSync(path.join(runtimeDir, `${name}-initial-screen.txt`), `${initial}\n`)
  const instance = { name, scriptFile, project, workspace, apiBridgeDirectory, tuiSession: launched.session_id }
  instances.push(instance)
  record(`${name}: actual TUI reached observed composer through route-allowlisted workspace bridge`, { sessionId: launched.session_id, workspace, initialScreen: initial.slice(0, 1800) })
  return instance
}

async function submitFromTui(instance, prompt) {
  const before = await listSessions(instance)
  await call("terminal_type", { session_id: instance.tuiSession, text: prompt })
  await call("terminal_key", { session_id: instance.tuiSession, key: "Enter" })
  const session = await waitFor(async () => {
    const current = await listSessions(instance)
    const ids = new Set(before.map((x) => x.id))
    const added = current.filter((x) => !ids.has(x.id)).sort((a, b) => (b.time?.updated ?? 0) - (a.time?.updated ?? 0))
    for (const candidate of added) {
      const messages = await getMessages(instance, candidate.id)
      const user = messages.find((message) => message.info?.role === "user")
      if (user) return { info: candidate, messages, user }
    }
    return null
  }, { timeoutMs: 20000, pollMs: 150, label: `${instance.name} persisted user-message baseline` })
  instance.sessionId = session.info.id
  instance.userMessageId = session.user.info.id
  record(`${instance.name}: UI submission persisted; baseline anchored to user message`, { sessionId: instance.sessionId, userMessageId: instance.userMessageId })
  return session
}

function assistantMessages(messages, userMessageId) {
  return messages.filter((message) => message.info?.role === "assistant" && (message.info.parentID === userMessageId || message.info.id))
}

async function waitTurn(instance, { timeoutMs = 45000 } = {}) {
  return waitFor(async () => {
    const [messages, screen] = await Promise.all([
      getMessages(instance, instance.sessionId),
      screenText(instance.tuiSession),
    ])
    const assistants = assistantMessages(messages, instance.userMessageId)
    const latest = assistants.at(-1)
    const idleComposer = screen.includes("Message...")
    const finish = typeof latest?.info?.finish === "string" ? latest.info.finish : latest?.info?.finish?.reason
    const hasTerminalAssistant = latest && ["stop", "tool-calls", "error", "abort"].includes(finish)
    if (idleComposer && hasTerminalAssistant) return { messages, screen, assistant: latest }
    return null
  }, { timeoutMs, pollMs: 200, label: `${instance.name} same-instance assistant terminal state and idle TUI` })
}

async function snapshotProviderReport(instance, tag) {
  const providerState = path.join(instance.workspace, ".runtime", "provider", "provider-report.json")
  const reportFile = path.join(runtimeDir, `${tag}-provider-report.json`)
  await waitFor(() => fs.existsSync(providerState), { timeoutMs: 5000, pollMs: 50, label: "sandbox-local provider report after owned session close" })
  fs.copyFileSync(providerState, reportFile)
  return reportFile
}

async function scenarioSuccess() {
  const instance = await startApp("success", "silicogen-success-acceptance.json")
  const submitted = await submitFromTui(instance, "Create fixture-output.txt containing exactly: scripted provider acceptance")
  assert.equal(submitted.user.info.id, instance.userMessageId)
  const completed = await waitTurn(instance)
  const messages = completed.messages
  const rendered = JSON.stringify(messages)
  const file = path.join(instance.workspace, "fixture-output.txt")
  assert.equal(fs.readFileSync(file, "utf8"), "scripted provider acceptance\n")
  record("real Silicogen write tool created exact file inside copied workspace", { file, contents: fs.readFileSync(file, "utf8") })
  assert.match(rendered, /fixture-output\.txt/)
  assert.match(rendered, /Created fixture-output\.txt with the exact scripted contents\./)
  const assistantFinish = typeof completed.assistant.info.finish === "string"
    ? completed.assistant.info.finish
    : completed.assistant.info.finish?.reason
  assert.equal(assistantFinish, "stop")
  record("main scripted turn completed with assistant stop", { assistantFinish })

  const title = (await appRequest(instance.apiBridgeDirectory, instance.workspace, `/session/${instance.sessionId}`)).body
  assert.equal(title.title, "Scripted provider acceptance")
  record("title helper result persisted", { title: title.title })

  const turnRecap = await waitFor(async () => {
    const current = await getMessages(instance, instance.sessionId)
    const user = current.find((message) => message.info?.id === instance.userMessageId)
    return user?.info?.summary?.recap?.status === "ok" ? user.info.summary.recap : null
  }, { timeoutMs: 16000, pollMs: 200, label: "per-turn recap JSON persisted on user message" })
  assert.equal(turnRecap.summary, "The turn wrote fixture-output.txt.")
  record("per-turn recap helper produced parsed JSON and persisted recap", { status: turnRecap.status, summary: turnRecap.summary })

  const catchup = await appRequest(instance.apiBridgeDirectory, instance.workspace, `/session/${instance.sessionId}/recap`, {
    method: "POST", body: JSON.stringify({ providerID: "emulation-fixture", modelID: "fixture-recap" }),
  })
  assert.equal(catchup.status, 200, JSON.stringify(catchup.body))
  assert.equal(catchup.body.summary, "The scripted turn created fixture-output.txt.")
  assert.equal(catchup.body.next_action, null)
  record("explicit catch-up endpoint returned valid parsed JSON", catchup.body)

  await call("terminal_key", { session_id: instance.tuiSession, key: "Ctrl+P" })
  await waitTerminalText(instance.tuiSession, "Commands", 6000)
  const palette = await screenText(instance.tuiSession)
  fs.writeFileSync(path.join(runtimeDir, "success-command-palette.txt"), `${palette}\n`)
  assert.match(palette.toLowerCase(), /commands|search/)
  await call("terminal_key", { session_id: instance.tuiSession, key: "Escape" })
  record("TUI command/search palette opened and closed from observed session state", { screen: palette.slice(0, 1200) })

  await call("terminal_close", { session_id: instance.tuiSession })
  sessions.delete(instance.tuiSession)
  const reportFile = await snapshotProviderReport(instance, "success")
  const providerReport = JSON.parse(fs.readFileSync(reportFile, "utf8"))
  assert.equal(providerReport.counts.unmatched, 0)
  assert.equal(providerReport.counts.errors, 0)
  assert.ok(providerReport.counts.matched >= 4, JSON.stringify(providerReport.counts))
  record("provider report confirms scripted requests matched with no routing or server errors", providerReport.counts)
  instance.providerReportFile = reportFile
  return instance
}

async function main() {
  fs.mkdirSync(runtimeDir, { recursive: true, mode: 0o700 })
  const version = spawnSync(binary, ["--version"], { encoding: "utf8", timeout: 5000 })
  if (version.status !== 0) throw new Error(`runtime binary unavailable: ${version.stderr || version.error}`)
  report.binaryVersion = version.stdout.trim()
  report.binarySha256 = crypto.createHash("sha256").update(fs.readFileSync(binary)).digest("hex")
  report.sourceCommit = spawnSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).stdout.trim() || "unknown"
  report.sourceDirty = spawnSync("git", ["status", "--short"], { cwd: root, encoding: "utf8" }).stdout.trim().length > 0
  transport = new StdioClientTransport({ command: binary, args: ["--runtime-dir", runtimeDir, "mcp"], stderr: "pipe" })
  client = new Client({ name: "silicogen-scripted-provider-acceptance", version: "1.0.0" })
  await client.connect(transport)
  const tools = (await client.listTools()).tools.map((item) => item.name)
  assert.ok(tools.includes("terminal_launch") && tools.includes("terminal_observe") && tools.includes("terminal_key") && tools.includes("terminal_type"))
  record("actual emulator MCP advertises launch, observe and keyboard interaction")
  await setupProfile()
  const success = await scenarioSuccess()
  report.scenarios.push({ name: "success-write-title-summary-turn-recap-explicit-recap", status: "passed", session: success.sessionId, userMessage: success.userMessageId })
  report.completedAt = new Date().toISOString()
  report.passed = report.failures.length === 0
}

try {
  await main()
} catch (error) {
  report.passed = false
  report.failures.push({ message: String(error?.stack || error) })
  console.error(error?.stack || error)
} finally {
  report.finishedAt = new Date().toISOString()
  fs.mkdirSync(runtimeDir, { recursive: true, mode: 0o700 })
  writeJson(path.join(runtimeDir, "SCRIPTED_ACCEPTANCE.json"), report)
  const cleanup = { sessions: [] }
  for (const sessionId of sessions) {
    try {
      await call("terminal_close", { session_id: sessionId })
      cleanup.sessions.push({ sessionId, closed: true })
    } catch (error) {
      cleanup.sessions.push({ sessionId, closed: false, error: String(error?.message || error) })
      report.failures.push({ message: `failed to close test-owned session ${sessionId}: ${String(error?.message || error)}` })
    }
  }
  if (client) await client.close().catch(() => {})
  try {
    Object.assign(cleanup, await stopOwnedDaemon())
  } catch (error) {
    cleanup.error = String(error?.message || error)
    report.failures.push({ message: `failed to stop test-owned runtime daemon: ${cleanup.error}` })
  }
  report.cleanup = cleanup
  if (cleanup.remaining?.length) {
    report.failures.push({ message: `test-owned runtime daemon remained after cleanup: ${cleanup.remaining.join(",")}` })
  }
  report.passed = report.failures.length === 0
  writeJson(path.join(runtimeDir, "SCRIPTED_ACCEPTANCE.json"), report)
  console.log(`Evidence: ${runtimeDir}`)
  if (!report.passed) process.exitCode = 1
}
