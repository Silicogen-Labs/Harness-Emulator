import test from "node:test"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises"
import { join, resolve } from "node:path"
import { startProviderFixture } from "./scripted-provider.mjs"

const root = resolve(".runtime/scripted-provider-tests")
await mkdir(root, { recursive: true })
let sequence = 0

async function fixtureFor(script) {
  const dir = await mkdtemp(join(root, `case-${++sequence}-`))
  const scriptPath = join(dir, "script.json")
  await writeFile(scriptPath, JSON.stringify(script))
  const reportDir = join(root, `report-${sequence}`)
  const fixture = await startProviderFixture({ scriptPath, reportDir })
  return { ...fixture, reportDir, async dispose() { await fixture.close() } }
}

const baseScript = {
  version: 1,
  rules: [{ ruleId: "stream", match: { model: "m", stream: true }, responses: [{
    content: "hello",
    toolCalls: [{ id: "call_1", name: "write_file", arguments: { path: "x.txt", content: "ok" } }],
    finishReason: "tool_calls",
  }] }],
}

function parseChunks(text) {
  return [...text.matchAll(/^data: (.+)$/gm)]
    .map((match) => match[1])
    .filter((line) => line !== "[DONE]")
    .map((line) => JSON.parse(line))
}

test("stream assembles text/tool call chunks and reports tool_calls plus DONE", async () => {
  const f = await fixtureFor(baseScript)
  try {
    const response = await fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json", authorization: "Bearer dummy-only" }, body: JSON.stringify({ model: "m", stream: true, messages: [] }) })
    const stream = await response.text()
    assert.equal(response.status, 200)
    const chunks = parseChunks(stream)
    assert.equal(chunks.at(-1).choices[0].finish_reason, "tool_calls")
    assert.equal(chunks.flatMap((chunk) => chunk.choices[0].delta.content ?? []).join(""), "hello")
    const toolDeltas = chunks.flatMap((chunk) => chunk.choices[0].delta.tool_calls ?? [])
    assert.equal(toolDeltas[0].function.name, "write_file")
    assert.equal(toolDeltas.slice(1).map((delta) => delta.function.arguments ?? "").join(""), '{"path":"x.txt","content":"ok"}')
    assert.match(stream, /data: \[DONE\]/)
  } finally { await f.dispose() }
})

test("non-stream title and recap helpers return OpenAI completion JSON", async () => {
  const f = await fixtureFor({ version: 1, rules: [
    { ruleId: "title", match: { model: "title-model", stream: false }, responses: [{ content: "Create a file" }] },
    { ruleId: "recap", match: { model: "recap-model", stream: false }, responses: [{ content: '{"summary":"Created it","next_action":null}' }] },
  ] })
  try {
    const call = async (model) => fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model, stream: false, messages: [] }) }).then((r) => r.json())
    const [title, recap] = await Promise.all([call("title-model"), call("recap-model")])
    assert.equal(title.choices[0].message.content, "Create a file")
    assert.equal(recap.choices[0].message.content, '{"summary":"Created it","next_action":null}')
    assert.equal(title.object, "chat.completion")
    assert.equal(recap.choices[0].finish_reason, "stop")
  } finally { await f.dispose() }
})

test("concurrent title, summary, recap, and main calls route by model, independent of arrival order", async () => {
  const f = await fixtureFor({ version: 1, rules: [
    { ruleId: "main", match: { model: "main", stream: true }, responses: [{ content: "main response" }] },
    { ruleId: "title", match: { model: "title", stream: true }, responses: [{ content: "title response" }] },
    { ruleId: "summary", match: { model: "summary", stream: true }, responses: [{ content: "summary response" }] },
    { ruleId: "recap", match: { model: "recap", stream: true }, responses: [{ content: "recap response" }] },
  ] })
  try {
    const calls = ["recap", "main", "summary", "title"].map(async (model) => {
      const response = await fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model, stream: true, messages: [] }) })
      return [model, await response.text()]
    })
    const results = Object.fromEntries(await Promise.all(calls))
    for (const model of ["main", "title", "summary", "recap"]) {
      const text = parseChunks(results[model]).flatMap((chunk) => chunk.choices[0].delta.content ?? []).join("")
      assert.equal(text, `${model} response`)
    }
    const report = f.getReport()
    assert.deepEqual(report.events.map((event) => event.ruleId).sort(), ["main", "recap", "summary", "title"])
  } finally { await f.dispose() }
})

test("delayed stream cancellation releases active work", async () => {
  const f = await fixtureFor({ version: 1, rules: [{ ruleId: "slow", match: { model: "slow", stream: true }, responses: [{ delayMs: 200, content: "late" }] }] })
  try {
    const controller = new AbortController()
    const pending = fetch(`${f.baseURL}/chat/completions`, { method: "POST", signal: controller.signal, headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "slow", stream: true, messages: [] }) })
    setTimeout(() => controller.abort(), 25)
    await assert.rejects(pending, { name: "AbortError" })
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 30))
    const health = await fetch(f.baseURL.replace(/\/v1$/, "/health")).then((r) => r.json())
    assert.equal(health.active, 0)
    assert.equal(f.getReport().counts.cancelled, 1)
  } finally { await f.dispose() }
})

test("close aborts and destroys active delayed responses within two seconds", async () => {
  const f = await fixtureFor({ version: 1, rules: [{ ruleId: "shutdown-delay", match: { model: "shutdown", stream: true }, responses: [{ delayMs: 10_000, content: "should never arrive" }] }] })
  const pending = fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "shutdown", stream: true, messages: [] }) })
  const pendingResult = assert.rejects(pending)
  try {
    const deadline = Date.now() + 1000
    while (f.getReport().counts.received === 0) {
      assert.ok(Date.now() < deadline, "delayed request was not admitted")
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 5))
    }
    let timer
    try {
      await Promise.race([
        f.close(),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("fixture.close exceeded two seconds")), 2000) }),
      ])
    } finally { clearTimeout(timer) }
    await pendingResult
    assert.equal(f.getReport().counts.cancelled, 1)
  } finally { await f.dispose() }
})

test("configured 5xx is followed by its deterministic retry response", async () => {
  const f = await fixtureFor({ version: 1, rules: [{ ruleId: "retry", match: { model: "retry", stream: false }, responses: [
    { status: 503, errorMessage: "temporary fixture outage" },
    { content: "recovered" },
  ] }] })
  try {
    const call = () => fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "retry", stream: false, messages: [] }) })
    const first = await call()
    assert.equal(first.status, 503)
    assert.equal((await first.json()).error.message, "temporary fixture outage")
    const second = await call()
    assert.equal(second.status, 200)
    assert.equal((await second.json()).choices[0].message.content, "recovered")
    assert.equal(f.getReport().events.filter((event) => event.ruleId === "retry").length, 2)
  } finally { await f.dispose() }
})

test("ambiguous requests fail without consuming any scripted response", async () => {
  const f = await fixtureFor({ version: 1, rules: [
    { ruleId: "one", match: { model: "m", stream: false }, responses: [{ content: "first" }] },
    { ruleId: "two", match: { model: "m", stream: false }, responses: [{ content: "second" }] },
  ] })
  try {
    const call = () => fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "m", stream: false, messages: [] }) })
    const a = await call(); const b = await call()
    assert.equal(a.status, 422); assert.equal(b.status, 422)
    assert.equal((await a.json()).error.type, "fixture_routing_error")
    assert.equal(f.getReport().counts.ambiguous, 2)
    assert.deepEqual(f.getReport().events.map((event) => event.ruleId), [null, null])
  } finally { await f.dispose() }
})

test("an unmatched request records a zero-match gap and does not consume a rule response", async () => {
  const f = await fixtureFor({ version: 1, rules: [
    { ruleId: "only", match: { model: "expected", stream: false }, responses: [{ content: "first response" }, { content: "second response" }] },
  ] })
  try {
    const call = (model) => fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model, stream: false, messages: [] }) })
    const unmatched = await call("unexpected")
    assert.equal(unmatched.status, 422)
    assert.equal((await unmatched.json()).error.type, "fixture_routing_error")
    const matched = await call("expected")
    assert.equal(matched.status, 200)
    assert.equal((await matched.json()).choices[0].message.content, "first response")
    const report = f.getReport()
    assert.equal(report.counts.unmatched, 1)
    assert.equal(report.events.find((event) => event.outcome === "unmatched").ruleId, null)
    assert.equal(report.events.find((event) => event.ruleId === "only").outcome, "completed")
  } finally { await f.dispose() }
})

test("reports are bounded and omit bearer tokens and prompt contents", async () => {
  const f = await fixtureFor({ version: 1, rules: [{ ruleId: "safe", match: { model: "safe", stream: false }, responses: [{ content: "ok" }] }] })
  try {
    for (let i = 0; i < 140; i++) {
      await fetch(`${f.baseURL}/chat/completions`, { method: "POST", headers: { "content-type": "application/json", authorization: "Bearer super-secret-sentinel" }, body: JSON.stringify({ model: "safe", stream: false, messages: [{ role: "user", content: "prompt-secret-sentinel" }] }) })
    }
    const report = f.getReport()
    assert.ok(JSON.stringify(report).length < 64 * 1024)
    assert.equal(report.events.length, 128)
    assert.deepEqual(report.retention, { maxEvents: 128, maxBytes: 64 * 1024, totalEvents: 140, retainedEvents: 128, droppedEvents: 12, truncated: true })
    assert.doesNotMatch(JSON.stringify(report), /super-secret-sentinel|prompt-secret-sentinel/)
    assert.equal(report.counts.received, 140)
    await f.dispose()
    const written = await readFile(resolve(f.reportDir, "provider-report.json"), "utf8")
    assert.doesNotMatch(written, /super-secret-sentinel|prompt-secret-sentinel/)
  } finally { await f.dispose() }
})
