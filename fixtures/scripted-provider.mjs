#!/usr/bin/env node
import { createServer } from "node:http"
import { readFile, mkdir, writeFile } from "node:fs/promises"
import { dirname, isAbsolute, relative, resolve, sep } from "node:path"
import { randomUUID } from "node:crypto"
import { pathToFileURL } from "node:url"

const BODY_LIMIT = 1024 * 1024
const REPORT_MAX_EVENTS = 128
const REPORT_MAX_BYTES = 64 * 1024
const SAFE_HOST = "127.0.0.1"

function fail(message) {
  throw new Error(message)
}

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
}

function validateScript(script) {
  if (!isRecord(script) || script.version !== 1 || !Array.isArray(script.rules) || script.rules.length === 0) {
    fail("script must have version: 1 and a non-empty rules array")
  }
  const ids = new Set()
  for (const rule of script.rules) {
    if (!isRecord(rule) || typeof rule.ruleId !== "string" || !rule.ruleId.trim()) fail("each rule needs an explicit ruleId")
    if (ids.has(rule.ruleId)) fail(`duplicate ruleId: ${rule.ruleId}`)
    ids.add(rule.ruleId)
    if (!isRecord(rule.match) || typeof rule.match.model !== "string" || typeof rule.match.stream !== "boolean") {
      fail(`rule ${rule.ruleId} needs match.model and match.stream`)
    }
    if (rule.match.criteria !== undefined && !isRecord(rule.match.criteria)) fail(`rule ${rule.ruleId} criteria must be an object`)
    if (!Array.isArray(rule.responses) || !rule.responses.length) fail(`rule ${rule.ruleId} needs a non-empty responses array`)
    for (const response of rule.responses) validateResponse(rule.ruleId, response)
  }
  return script
}

function validateResponse(ruleId, response) {
  if (!isRecord(response)) fail(`rule ${ruleId} contains a non-object response`)
  if (response.status !== undefined && (!Number.isInteger(response.status) || response.status < 400 || response.status > 599)) {
    fail(`rule ${ruleId} response status must be an HTTP error status (400-599)`)
  }
  for (const key of ["delayMs", "chunkDelayMs"]) {
    if (response[key] !== undefined && (!Number.isInteger(response[key]) || response[key] < 0 || response[key] > 300_000)) {
      fail(`rule ${ruleId} ${key} must be an integer from 0 to 300000`)
    }
  }
  if (response.toolCalls !== undefined && !Array.isArray(response.toolCalls)) fail(`rule ${ruleId} toolCalls must be an array`)
  if (response.content !== undefined && typeof response.content !== "string") fail(`rule ${ruleId} content must be a string`)
  if (response.finishReason !== undefined && !["stop", "tool_calls"].includes(response.finishReason)) {
    fail(`rule ${ruleId} finishReason must be stop or tool_calls`)
  }
}

function getPath(value, path) {
  return path.split(".").reduce((node, part) => (node == null ? undefined : node[part]), value)
}

function criterionMatches(actual, expected) {
  if (isRecord(expected)) {
    if (Object.keys(expected).length === 1 && typeof expected.contains === "string") {
      return typeof actual === "string" && actual.includes(expected.contains)
    }
    return false
  }
  return Object.is(actual, expected)
}

function matches(rule, body) {
  if (rule.match.model !== body.model || rule.match.stream !== (body.stream === true)) return false
  for (const [path, expected] of Object.entries(rule.match.criteria ?? {})) {
    if (!criterionMatches(getPath(body, path), expected)) return false
  }
  return true
}

function sanitizedEvent(event) {
  return {
    at: event.at,
    requestId: event.requestId,
    ruleId: event.ruleId,
    model: event.model,
    stream: event.stream,
    status: event.status,
    outcome: event.outcome,
    finishReason: event.finishReason,
    durationMs: event.durationMs,
    bodyBytes: event.bodyBytes,
  }
}

function boundedReport(events, counters, cancelled, totalEvents) {
  let selected = events.slice(-REPORT_MAX_EVENTS)
  let report = {
    version: 1,
    counts: { ...counters, cancelled },
    retention: { maxEvents: REPORT_MAX_EVENTS, maxBytes: REPORT_MAX_BYTES, totalEvents, retainedEvents: selected.length, droppedEvents: totalEvents - selected.length, truncated: totalEvents > selected.length },
    events: selected.map(sanitizedEvent),
  }
  let json = JSON.stringify(report)
  while (Buffer.byteLength(json) > REPORT_MAX_BYTES && selected.length) {
    selected = selected.slice(1)
    report = {
      version: 1,
      counts: { ...counters, cancelled },
      retention: { maxEvents: REPORT_MAX_EVENTS, maxBytes: REPORT_MAX_BYTES, totalEvents, retainedEvents: selected.length, droppedEvents: totalEvents - selected.length, truncated: totalEvents > selected.length },
      events: selected.map(sanitizedEvent),
    }
    json = JSON.stringify(report)
  }
  return report
}

function delay(ms, signal) {
  if (!ms) return Promise.resolve()
  return new Promise((resolveDelay, reject) => {
    if (signal.aborted) return reject(signal.reason ?? new Error("aborted"))
    const timer = setTimeout(done, ms)
    function done() {
      signal.removeEventListener("abort", abort)
      resolveDelay()
    }
    function abort() {
      clearTimeout(timer)
      signal.removeEventListener("abort", abort)
      reject(signal.reason ?? new Error("aborted"))
    }
    signal.addEventListener("abort", abort, { once: true })
  })
}

async function readBody(request) {
  const chunks = []
  let size = 0
  for await (const chunk of request) {
    size += chunk.length
    if (size > BODY_LIMIT) throw Object.assign(new Error("request body too large"), { status: 413 })
    chunks.push(chunk)
  }
  try {
    return { body: JSON.parse(Buffer.concat(chunks).toString("utf8")), size }
  } catch {
    throw Object.assign(new Error("invalid JSON request"), { status: 400 })
  }
}

function completion(response, model, requestId, finishReason) {
  const toolCalls = response.toolCalls ?? []
  return {
    id: `chatcmpl-${requestId}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      message: {
        role: "assistant",
        content: response.content ?? (toolCalls.length ? null : ""),
        ...(toolCalls.length ? { tool_calls: toolCalls.map((call, index) => ({
          id: call.id ?? `call_${requestId}_${index}`,
          type: "function",
          function: { name: call.name, arguments: typeof call.arguments === "string" ? call.arguments : JSON.stringify(call.arguments ?? {}) },
        })) } : {}),
      },
      finish_reason: finishReason,
    }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  }
}

function sse(response, payload) {
  return response.write(`data: ${JSON.stringify(payload)}\n\n`)
}

async function writeStream(response, scripted, model, requestId, signal) {
  response.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache", connection: "keep-alive" })
  const chunkDelayMs = scripted.chunkDelayMs ?? 0
  const base = { id: `chatcmpl-${requestId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model }
  sse(response, { ...base, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] })
  const textChunks = scripted.textChunks ?? (scripted.content ? [...scripted.content].map((char) => char) : [])
  for (const text of textChunks) {
    await delay(chunkDelayMs, signal)
    sse(response, { ...base, choices: [{ index: 0, delta: { content: text }, finish_reason: null }] })
  }
  for (const [index, call] of (scripted.toolCalls ?? []).entries()) {
    const callId = call.id ?? `call_${requestId}_${index}`
    const args = typeof call.arguments === "string" ? call.arguments : JSON.stringify(call.arguments ?? {})
    await delay(chunkDelayMs, signal)
    sse(response, { ...base, choices: [{ index: 0, delta: { tool_calls: [{ index, id: callId, type: "function", function: { name: call.name, arguments: "" } }] }, finish_reason: null }] })
    const argChunks = call.argumentChunks ?? [...args].map((char) => char)
    for (const part of argChunks) {
      await delay(chunkDelayMs, signal)
      sse(response, { ...base, choices: [{ index: 0, delta: { tool_calls: [{ index, function: { arguments: part } }] }, finish_reason: null }] })
    }
  }
  await delay(chunkDelayMs, signal)
  const finishReason = scripted.finishReason ?? ((scripted.toolCalls?.length ?? 0) ? "tool_calls" : "stop")
  sse(response, { ...base, choices: [{ index: 0, delta: {}, finish_reason: finishReason }] })
  response.write("data: [DONE]\n\n")
  response.end()
  return finishReason
}

function reportPathWithinRuntime(path) {
  const absolute = resolve(path)
  const runtime = resolve(".runtime")
  const rel = relative(runtime, absolute)
  if (rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel)) fail("reportDir must be inside .runtime")
  return absolute
}

export async function startProviderFixture({ scriptPath, host = SAFE_HOST, port = 0, reportDir = ".runtime/provider-fixture" } = {}) {
  if (!scriptPath) fail("scriptPath is required")
  if (!Number.isInteger(port) || port < 0 || port > 65535) fail("port must be an integer from 0 to 65535")
  const script = validateScript(JSON.parse(await readFile(scriptPath, "utf8")))
  const reportDirectory = reportPathWithinRuntime(reportDir)
  const counters = Object.fromEntries(script.rules.map((rule) => [rule.ruleId, 0]))
  const events = []
  const active = new Map()
  let cancelled = 0
  let totalEvents = 0
  const totals = { received: 0, matched: 0, unmatched: 0, ambiguous: 0, errors: 0 }

  function addEvent(event) {
    totalEvents++
    events.push(sanitizedEvent(event))
    if (events.length > REPORT_MAX_EVENTS) events.shift()
  }

  async function handle(request, response) {
    if (request.method === "GET" && request.url === "/health") {
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({ ok: true, protocol: "openai-chat-completions", active: active.size }))
      return
    }
    if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
      response.writeHead(404, { "content-type": "application/json" })
      response.end(JSON.stringify({ error: { message: "fixture route not found", type: "not_found_error" } }))
      return
    }
    totals.received++
    const requestId = randomUUID().slice(0, 12)
    const started = Date.now()
    const controller = new AbortController()
    active.set(controller, response)
    let completed = false
    let selectedRule
    let status = 500
    let outcome = "error"
    let finishReason
    let bodyBytes = 0
    const abort = () => {
      if (!controller.signal.aborted) {
        controller.abort(new Error("client disconnected"))
        cancelled++
      }
    }
    request.once("aborted", abort)
    response.once("close", () => { if (!completed) abort() })
    try {
      const parsed = await readBody(request)
      bodyBytes = parsed.size
      const body = parsed.body
      if (!isRecord(body) || typeof body.model !== "string" || typeof body.stream !== "boolean") {
        throw Object.assign(new Error("request must specify model and stream"), { status: 400 })
      }
      const candidates = script.rules.filter((rule) => matches(rule, body))
      if (candidates.length !== 1) {
        const ambiguous = candidates.length > 1
        totals[ambiguous ? "ambiguous" : "unmatched"]++
        status = 422
        outcome = ambiguous ? "ambiguous" : "unmatched"
        response.writeHead(status, { "content-type": "application/json" })
        response.end(JSON.stringify({ error: { message: ambiguous ? "request matched multiple fixture rules" : "request matched no fixture rule", type: "fixture_routing_error" } }))
        completed = true
        return
      }
      selectedRule = candidates[0]
      totals.matched++
      const responseIndex = counters[selectedRule.ruleId]++
      const scripted = selectedRule.responses[responseIndex]
      if (!scripted) throw Object.assign(new Error("fixture rule response sequence exhausted"), { status: 409 })
      status = scripted.status ?? 200
      if (status >= 400) {
        await delay(scripted.delayMs ?? 0, controller.signal)
        response.writeHead(status, { "content-type": "application/json" })
        response.end(JSON.stringify({ error: { message: scripted.errorMessage ?? "scripted fixture error", type: "fixture_error", code: "scripted_error" } }))
        outcome = "scripted_error"
        completed = true
        return
      }
      await delay(scripted.delayMs ?? 0, controller.signal)
      if (controller.signal.aborted) return
      if (body.stream) {
        finishReason = await writeStream(response, scripted, body.model, requestId, controller.signal)
      } else {
        finishReason = scripted.finishReason ?? ((scripted.toolCalls?.length ?? 0) ? "tool_calls" : "stop")
        response.writeHead(200, { "content-type": "application/json" })
        response.end(JSON.stringify(completion(scripted, body.model, requestId, finishReason)))
      }
      status = 200
      outcome = "completed"
      completed = true
    } catch (error) {
      if (controller.signal.aborted || response.destroyed) {
        outcome = "cancelled"
      } else {
        status = error.status ?? 500
        totals.errors++
        if (!response.headersSent) response.writeHead(status, { "content-type": "application/json" })
        if (!response.destroyed) response.end(JSON.stringify({ error: { message: "fixture request failed", type: "fixture_error" } }))
        outcome = "error"
        completed = true
      }
    } finally {
      active.delete(controller)
      addEvent({ at: new Date().toISOString(), requestId, ruleId: selectedRule?.ruleId ?? null, model: selectedRule?.match.model ?? null, stream: selectedRule?.match.stream ?? null, status, outcome, finishReason, durationMs: Date.now() - started, bodyBytes })
    }
  }

  const server = createServer((request, response) => { void handle(request, response) })
  await new Promise((resolveListen, reject) => {
    server.once("error", reject)
    server.listen(port, host, () => { server.off("error", reject); resolveListen() })
  })
  const address = server.address()
  const baseURL = `http://${host}:${address.port}/v1`
  let closing
  return {
    server,
    baseURL,
    getReport() { return boundedReport(events, totals, cancelled, totalEvents) },
    async close() {
      if (closing) return closing
      closing = (async () => {
        for (const [controller, response] of active) {
          if (!controller.signal.aborted) {
            controller.abort(new Error("fixture shutting down"))
            cancelled++
          }
          response.destroy()
        }
        server.closeAllConnections()
        await new Promise((resolveClose, reject) => server.close((error) => error ? reject(error) : resolveClose()))
        await mkdir(reportDirectory, { recursive: true })
        const report = boundedReport(events, totals, cancelled, totalEvents)
        const reportText = JSON.stringify(report, null, 2)
        await writeFile(resolve(reportDirectory, "provider-report.json"), reportText, { mode: 0o600 })
        return report
      })()
      return closing
    },
  }
}

function parseArgs(args) {
  const result = { port: 0, host: SAFE_HOST, reportDir: ".runtime/provider-fixture" }
  for (let i = 0; i < args.length; i++) {
    const name = args[i]
    if (!["--script", "--port", "--report-dir", "--host"].includes(name)) fail(`unknown argument: ${name}`)
    const value = args[++i]
    if (!value) fail(`missing value for ${name}`)
    const key = { "--script": "scriptPath", "--port": "port", "--report-dir": "reportDir", "--host": "host" }[name]
    result[key] = key === "port" ? Number(value) : value
  }
  if (!result.scriptPath) fail("usage: node fixtures/scripted-provider.mjs --script <path> [--port <0-65535>] [--report-dir <.runtime path>] [--host <address>]")
  return result
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const fixture = await startProviderFixture(parseArgs(process.argv.slice(2)))
    process.stdout.write(`${JSON.stringify({ baseURL: fixture.baseURL, healthURL: fixture.baseURL.replace(/\/v1$/, "/health") })}\n`)
    const shutdown = async () => {
      await fixture.close()
      process.exit(0)
    }
    process.once("SIGINT", shutdown)
    process.once("SIGTERM", shutdown)
  } catch (error) {
    process.stderr.write(`${error.message}\n`)
    process.exitCode = 1
  }
}
