import { test, expect } from "@playwright/test"
import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js"
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, readFileSync } from "node:fs"
import { createConnection, createServer } from "node:net"
import { randomBytes } from "node:crypto"
import path from "node:path"
import { fileURLToPath } from "node:url"

const emulationDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const binary = path.join(emulationDir, "target/debug/silicogen-emulation")
const fixtureProgram = path.join(emulationDir, "target/debug/terminal-fixture")
const runTag = `pw${process.pid}`
const runtimeDir = `.runtime/${runTag}`
const workDir = path.join(emulationDir, ".runtime", `work-${runTag}`)
const artifactDir = path.join(emulationDir, ".runtime", "viewer-tests", runTag)

let sessionId
let view
let readOnlyView
let originView
let originalRunId
const extraViews = []
const extraSessions = []
let mcpClient

async function mcp(name, args) {
  let result
  try {
    result = await mcpClient.callTool({ name, arguments: args })
  } catch (error) {
    throw new Error(`MCP tool ${name} failed: ${error.message}`)
  }
  if (result.isError) {
    const message = result.content?.filter((part) => part.type === "text").map((part) => part.text).join("\n")
    throw new Error(`MCP tool ${name} failed: ${message}`)
  }
  if (result.structuredContent) return result.structuredContent
  const text = result.content?.find((part) => part.type === "text")?.text
  return text ? JSON.parse(text) : {}
}

async function freeLoopbackPort() {
  const server = createServer()
  await new Promise((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const address = server.address()
  await new Promise((resolve) => server.close(resolve))
  return address.port
}

async function callClient(client, name, args) {
  const result = await client.callTool({ name, arguments: args })
  if (result.isError) {
    throw new Error(`MCP tool ${name} failed: ${result.content?.map((part) => part.text || "").join(" ")}`)
  }
  if (result.structuredContent) return result.structuredContent
  const text = result.content?.find((part) => part.type === "text")?.text
  return text ? JSON.parse(text) : {}
}

function cliResult(args) {
  return spawnSync(binary, ["--runtime-dir", runtimeDir, ...args], {
    cwd: emulationDir,
    env: { ...process.env },
    encoding: "utf8",
  })
}

test.beforeAll(async () => {
  mkdirSync(workDir, { recursive: true })
  mkdirSync(artifactDir, { recursive: true })
  const transport = new StdioClientTransport({
    command: binary,
    args: ["--runtime-dir", runtimeDir, "mcp"],
    cwd: emulationDir,
    env: { ...process.env },
    stderr: "pipe",
  })
  mcpClient = new Client({ name: "terminal-viewer-browser-tests", version: "1.0.0" })
  await mcpClient.connect(transport)
  const launched = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd: workDir,
    name: "browser-viewer-fixture",
    cols: 120,
    rows: 40,
  })
  sessionId = launched.session_id
  originalRunId = launched.run_id
  const ready = await mcp("terminal_wait", {
    session_id: sessionId,
    kind: "contains",
    value: "READY",
    timeout_ms: 10000,
  })
  expect(ready.matched).toBe(true)
  view = await mcp("terminal_view_start", {
    session_id: sessionId,
    allow_browser_control: true,
    idempotency_key: `${runTag}-primary`,
  })
  originView = await mcp("terminal_view_start", {
    session_id: sessionId,
    allow_browser_control: false,
    idempotency_key: `${runTag}-origin-probe`,
  })
})

test.afterAll(async () => {
  if (!sessionId) return
  if (mcpClient) {
    const status = await mcp("terminal_control_status", { session_id: sessionId }).catch(() => null)
    if (status?.owner === "browser") {
      await mcp("terminal_control_release", {
        session_id: sessionId,
        expected_generation: status.generation,
      }).catch(() => {})
    }
    if (view?.view_id) await mcp("terminal_view_stop", { view_id: view.view_id }).catch(() => {})
    if (readOnlyView?.view_id) await mcp("terminal_view_stop", { view_id: readOnlyView.view_id }).catch(() => {})
    if (originView?.view_id) await mcp("terminal_view_stop", { view_id: originView.view_id }).catch(() => {})
    for (const viewId of extraViews) await mcp("terminal_view_stop", { view_id: viewId }).catch(() => {})
    for (const extraSession of extraSessions) {
      await mcp("terminal_close", { session_id: extraSession }).catch(() => {})
    }
    await mcp("terminal_close", { session_id: sessionId }).catch(() => {})
    await mcpClient.close()
  }
  cliResult(["shutdown"])
})

test("browser watches and controls the same live PTY session", async ({ page, context }) => {
  await page.addInitScript(() => {
    const NativeWebSocket = window.WebSocket
    window.__viewerSockets = []
    window.__viewerMessages = []
    window.WebSocket = new Proxy(NativeWebSocket, {
      construct(target, args) {
        const socket = new target(...args)
        window.__viewerSockets.push(socket)
        socket.addEventListener("message", (event) => {
          if (typeof event.data !== "string") return
          try {
            window.__viewerMessages.push(JSON.parse(event.data))
          } catch {}
        })
        return socket
      },
    })
  })
  await page.goto(view.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#accessible-screen")).toContainText("READY")
  await expect(page.locator("#session-name")).toHaveText("browser-viewer-fixture")
  await expect(page.locator("#session-path")).toContainText("isolated app state")
  await expect(page.locator(".mode-label")).toHaveText("Per-session XDG state · local process · no filesystem sandbox")
  await expect(page.locator("#owner")).toHaveText("Input owner: automation")
  await expect.poll(() => page.evaluate(() => document.fonts.check("14px EmulationMono"))).toBe(true)
  await expect(page.getByRole("button", { name: "Reset terminal to launch size (120 × 40)" })).toBeDisabled()
  const initialRender = await page.locator("#terminal").evaluate((canvas) => ({
    cssWidth: canvas.getBoundingClientRect().width,
    pixelWidth: canvas.width,
    pixelScale: Number(canvas.dataset.pixelScale),
  }))
  expect(initialRender.pixelScale).toBeGreaterThan(1)
  expect(initialRender.pixelWidth).toBeGreaterThan(initialRender.cssWidth)
  await page.getByRole("button", { name: "Increase cell zoom" }).click()
  await expect(page.locator("#scale-value")).toHaveText("115%")
  await expect.poll(() => page.locator("#terminal").evaluate((canvas) => canvas.getBoundingClientRect().width))
    .toBeGreaterThan(initialRender.cssWidth)
  await page.getByRole("button", { name: "Reset zoom" }).click()
  await expect(page.locator("#scale-value")).toHaveText("100%")

  readOnlyView = await mcp("terminal_view_start", {
    session_id: sessionId,
    allow_browser_control: false,
    idempotency_key: `${runTag}-observer`,
  })
  const observer = await context.newPage()
  await observer.goto(readOnlyView.url)
  await expect(observer.locator("#connection")).toHaveText("Live")
  await expect(observer.locator("#frame-identity")).toHaveText(await page.locator("#frame-identity").textContent())

  await page.getByRole("button", { name: "Take control" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: browser")
  await expect(page.getByRole("button", { name: "Return to agent" })).toBeVisible()

  let deniedMessage = ""
  try {
    await mcpClient.callTool({
      name: "terminal_type",
      arguments: { session_id: sessionId, text: "must not reach the PTY" },
    })
  } catch (error) {
    deniedMessage = error.message
  }
  expect(deniedMessage).toContain("control_held_by_browser")
  await expect(page.locator("#accessible-screen")).not.toContainText("must not reach the PTY")

  const actionIdentity = await page.evaluate(() => {
    const messages = window.__viewerMessages
    return {
      frame: messages.filter((message) => message.type === "frame").at(-1).data,
      lease: messages.filter((message) => message.type === "control_changed" && message.lease).at(-1).lease,
    }
  })
  const duplicateAction = {
    type: "input",
    action_id: `${runTag}-duplicate-input`,
    run_id: actionIdentity.frame.frame.run_id,
    frame_id: actionIdentity.frame.frame.frame_id,
    view_revision: actionIdentity.frame.view_revision,
    viewport_revision: actionIdentity.frame.viewport_revision,
    lease_id: actionIdentity.lease.lease_id,
    generation: actionIdentity.lease.generation,
    input: { kind: "type", text: "__dedupe__" },
  }
  await page.evaluate((message) => {
    const socket = window.__viewerSockets[0]
    socket.send(JSON.stringify(message))
    socket.send(JSON.stringify(message))
  }, duplicateAction)
  await expect.poll(async () => {
    const text = await page.locator("#accessible-screen").textContent()
    return (text.match(/__dedupe__/g) || []).length
  }).toBe(1)

  await context.grantPermissions(["clipboard-read", "clipboard-write"], {
    origin: new URL(view.url).origin,
  })
  await page.locator("#keyboard-input").focus()
  await page.keyboard.type("browser typed")
  await expect(page.locator("#accessible-screen")).toContainText("Typed: __dedupe__browser typed")
  await expect(observer.locator("#accessible-screen")).toContainText("Typed: __dedupe__browser typed")

  await page.evaluate(() => {
    const input = document.querySelector("#keyboard-input")
    input.dispatchEvent(new CompositionEvent("compositionstart", { bubbles: true, data: "" }))
    input.dispatchEvent(new CompositionEvent("compositionupdate", { bubbles: true, data: "compose-漢字" }))
    input.dispatchEvent(new InputEvent("beforeinput", {
      bubbles: true,
      cancelable: true,
      inputType: "insertText",
      data: "compose-漢字",
      isComposing: true,
    }))
    input.dispatchEvent(new CompositionEvent("compositionend", { bubbles: true, data: "compose-漢字" }))
  })
  await expect(page.locator("#accessible-screen")).toContainText("Typed: __dedupe__browser typedcompose-漢字")
  await expect.poll(async () => {
    const text = await page.locator("#accessible-screen").textContent()
    return (text.match(/compose-漢字/g) || []).length
  }).toBe(1)

  await page.evaluate(() => navigator.clipboard.writeText("pasted by browser"))
  await page.locator("#keyboard-input").focus()
  await page.keyboard.press("Control+V")
  await expect(page.locator("#accessible-screen")).toContainText("Paste: pasted by browser")
  await expect(observer.locator("#accessible-screen")).toContainText("Paste: pasted by browser")

  await page.getByRole("button", { name: "Terminal mouse: off" }).click()
  const bounds = await page.locator("#terminal").boundingBox()
  expect(bounds).not.toBeNull()
  await page.mouse.click(bounds.x + bounds.width * 0.05, bounds.y + bounds.height * 0.075)
  await expect(page.locator("#accessible-screen")).toContainText("Mouse: code=3 row=3 col=5")
  await page.mouse.move(bounds.x + bounds.width * 0.05, bounds.y + bounds.height * 0.075)
  await page.mouse.wheel(0, 120)
  await expect(page.locator("#accessible-screen")).toContainText("Mouse: code=65 row=3 col=5")
  await expect(observer.locator("#accessible-screen")).toContainText("Mouse: code=65 row=3 col=5")

  await page.locator("#columns").fill("100")
  await page.locator("#rows").fill("30")
  await page.locator("#keyboard-input").focus()
  await page.keyboard.press("ArrowDown")
  await expect(page.locator("#accessible-screen")).toContainText("Selection: Beta")
  await expect(page.locator("#columns")).toHaveValue("100")
  await expect(page.locator("#rows")).toHaveValue("30")
  await page.getByRole("button", { name: "Resize terminal" }).click()
  await expect(page.locator("#accessible-screen")).toContainText("Size: 100x30")
  await expect(page.locator("#geometry")).toContainText("100 × 30 cells")
  await expect(observer.locator("#geometry")).toContainText("100 × 30 cells")

  let invalidResizeError
  try {
    await mcp("terminal_resize", { session_id: sessionId, cols: 301, rows: 30 })
  } catch (error) {
    invalidResizeError = error.message
  }
  expect(invalidResizeError).toBeTruthy()
  const afterInvalidResize = await mcp("terminal_observe", { session_id: sessionId })
  expect(afterInvalidResize.cols).toBe(100)
  expect(afterInvalidResize.rows).toBe(30)

  const resizedFrame = await page.evaluate(() => window.__viewerMessages.filter((message) => message.type === "frame").at(-1).data)
  expect(resizedFrame.viewport_revision).not.toBe(actionIdentity.frame.viewport_revision)
  const beforeStalePointer = await mcp("terminal_observe", { session_id: sessionId })
  const stalePointer = {
    ...duplicateAction,
    action_id: `${runTag}-stale-viewport`,
    frame_id: resizedFrame.frame.frame_id,
    view_revision: resizedFrame.view_revision,
    viewport_revision: actionIdentity.frame.viewport_revision,
    input: { kind: "mouse", input: { kind: "move", row: 1, col: 1 } },
  }
  await page.evaluate((message) => window.__viewerSockets[0].send(JSON.stringify(message)), stalePointer)
  await expect.poll(() => page.evaluate((actionId) =>
    window.__viewerMessages.some((message) => message.type === "action_result" && message.action_id === actionId),
  stalePointer.action_id)).toBe(true)
  const stalePointerResult = await page.evaluate((actionId) => window.__viewerMessages
    .filter((message) => message.type === "action_result" && message.action_id === actionId)
    .at(-1), stalePointer.action_id)
  expect(stalePointerResult.accepted).toBe(false)
  expect(stalePointerResult.error).toContain("stale_viewport")
  expect((await mcp("terminal_observe", { session_id: sessionId })).text).toBe(beforeStalePointer.text)

  const displayedFrame = (await page.locator("#frame-identity").textContent()).split(" · ")
  const [download] = await Promise.all([
    page.waitForEvent("download"),
    page.getByRole("button", { name: "Save displayed frame" }).click(),
  ])
  const snapshotPath = path.join(artifactDir, download.suggestedFilename())
  await download.saveAs(snapshotPath)
  const png = readFileSync(snapshotPath)
  expect([...png.subarray(0, 8)]).toEqual([137, 80, 78, 71, 13, 10, 26, 10])
  expect(download.suggestedFilename()).toContain(sessionId)
  expect(download.suggestedFilename()).toContain(`${displayedFrame[1]}-${displayedFrame[2]}.png`)

  await page.getByRole("button", { name: "Reset terminal to launch size (120 × 40)" }).click()
  await expect(page.locator("#geometry")).toContainText("120 × 40 cells")
  await expect(page.locator("#columns")).toHaveValue("120")
  await expect(page.locator("#rows")).toHaveValue("40")
  await expect(page.getByRole("button", { name: "Reset terminal to launch size (120 × 40)" })).toBeDisabled()

  await page.getByRole("button", { name: "Return to agent" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: automation")
  await mcp("terminal_type", { session_id: sessionId, text: "from agent" })
  await expect(page.locator("#accessible-screen")).toContainText("Typed: __dedupe__browser typedcompose-漢字from agent")
  await expect(observer.locator("#accessible-screen")).toContainText("Typed: __dedupe__browser typedcompose-漢字from agent")

  await page.getByRole("button", { name: "Take control" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: browser")
  await page.reload()
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#owner")).toHaveText("Input owner: automation")
  await mcp("terminal_type", { session_id: sessionId, text: "after reconnect" })
  await expect(page.locator("#accessible-screen")).toContainText("after reconnect")

  const runBeforeRestart = (await page.locator("#frame-identity").textContent()).split(" · ")[1]
  const restarted = await mcp("terminal_restart", { session_id: sessionId })
  expect(restarted.run_id).not.toBe(originalRunId)
  await page.waitForFunction(
    (runId) => document.querySelector("#frame-identity").textContent.includes(` · ${runId} · `),
    restarted.run_id,
  )
  await expect(page.locator("#accessible-screen")).toContainText("READY")
  const runAfterRestart = (await page.locator("#frame-identity").textContent()).split(" · ")[1]
  expect(runAfterRestart).not.toBe(runBeforeRestart)

  const status = await mcp("terminal_control_status", { session_id: sessionId })
  expect(status.owner).toBe("automation")
  const inertPayload = '<img src=x onerror="window.__terminalInjected=true">'
  await mcp("terminal_type", { session_id: sessionId, text: inertPayload })
  await expect(page.locator("#accessible-screen")).toContainText(inertPayload)
  expect(await page.evaluate(() => window.__terminalInjected)).toBeUndefined()
  expect(await page.locator("#accessible-screen img").count()).toBe(0)
  await observer.close()
})

test("application log panel polls scoped sources, keeps focus, bounds text, and resets on restart", async ({ page }) => {
  // Each test context needs a fresh single-redemption viewing capability.
  const logView = await mcp("terminal_view_start", {
    session_id: sessionId,
    allow_browser_control: true,
    idempotency_key: `${runTag}-logs`,
  })
  extraViews.push(logView.view_id)
  await page.addInitScript(() => {
    const NativeWebSocket = window.WebSocket
    window.__viewerSockets = []
    window.__viewerMessages = []
    window.__logsSent = []
    window.__logReplyQueue = { app: [], startup: [] }
    window.__logCursors = {}
    window.WebSocket = new Proxy(NativeWebSocket, {
      construct(target, args) {
        const socket = new target(...args)
        window.__viewerSockets.push(socket)
        socket.addEventListener("message", (event) => {
          if (typeof event.data !== "string") return
          try { window.__viewerMessages.push(JSON.parse(event.data)) } catch {}
        })
        const nativeSend = socket.send.bind(socket)
        socket.send = (raw) => {
          let message
          try { message = JSON.parse(raw) } catch {}
          if (message?.type === "logs" && ["app", "startup"].includes(message.source)) {
            const offset = window.__logsSent.length + 1
            window.__logsSent.push(message)
            const queuedText = window.__logReplyQueue[message.source].shift()
            const text = typeof queuedText === "string" ? queuedText : `mock ${message.source} log\n`
            const cursor = { source: message.source, offset }
            window.__logCursors[message.source] = cursor
            setTimeout(() => socket.dispatchEvent(new MessageEvent("message", {
              data: JSON.stringify({
                type: "logs",
                data: { text, cursor, gap: null, pending: false },
              }),
            })), 0)
            return
          }
          nativeSend(raw)
        }
        return socket
      },
    })
  })

  await page.goto(logView.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  await page.evaluate(() => window.__viewerSockets[0].send(JSON.stringify({ type: "logs", source: "daemon" })))
  await expect.poll(() => page.evaluate(() => window.__viewerMessages.some((message) =>
    message.type === "logs_error" && message.message.includes("only this session's app/startup logs"),
  ))).toBe(true)

  await expect(page.locator("#log-source option")).toHaveCount(2)
  const logSources = await page.locator("#log-source option").evaluateAll((options) => options.map((option) => option.value))
  expect(logSources).toEqual(["app", "startup"])

  await page.evaluate(() => window.__logReplyQueue.app.push("app first page\n"))
  await page.locator("#expand-logs").check()
  await expect.poll(() => page.evaluate(() => window.__logsSent.length)).toBeGreaterThan(0)
  await expect(page.locator("#logs-output")).toContainText("app first page")
  const firstAppRequest = await page.evaluate(() => window.__logsSent.find((request) => request.source === "app"))
  expect(firstAppRequest.cursor).toBeUndefined()

  await page.evaluate(() => window.__logReplyQueue.startup.push("startup trace page\n"))
  await page.locator("#log-source").selectOption("startup")
  await expect.poll(() => page.evaluate(() => window.__logsSent.some((request) => request.source === "startup")))
    .toBe(true)
  await expect(page.locator("#logs-output")).toContainText("startup trace page")
  const firstStartupRequest = await page.evaluate(() => window.__logsSent.find((request) => request.source === "startup"))
  expect(firstStartupRequest.cursor).toBeUndefined()

  const countBeforeAppAgain = await page.evaluate(() => window.__logsSent.filter((request) => request.source === "app").length)
  const appCursorBeforeSwitch = await page.evaluate(() => window.__logCursors.app)
  await page.locator("#log-source").selectOption("app")
  await expect.poll(() => page.evaluate((count) =>
    window.__logsSent.filter((request) => request.source === "app").length > count,
  countBeforeAppAgain)).toBe(true)
  const appAgain = await page.evaluate(() => window.__logsSent.filter((request) => request.source === "app").at(-1))
  expect(appAgain.cursor).toEqual(appCursorBeforeSwitch)

  await page.getByRole("button", { name: "Take control" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: browser")
  await page.locator("#log-source").focus()
  const screenBeforeLogTyping = await page.locator("#accessible-screen").textContent()
  const lease = await page.evaluate(() => window.__viewerMessages
    .filter((message) => message.type === "control_changed" && message.lease)
    .at(-1).lease)
  const viewId = new URL(logView.url).pathname.split("/").filter(Boolean).at(-1)
  await page.evaluate(({ lease, viewId }) => {
    window.__viewerSockets[0].dispatchEvent(new MessageEvent("message", {
      data: JSON.stringify({ type: "control_changed", owner: "browser", lease: { ...lease, view_id: viewId } }),
    }))
  }, { lease, viewId })
  await expect(page.locator("#log-source")).toBeFocused()
  await page.keyboard.press("z")
  await expect(page.locator("#accessible-screen")).toHaveText(screenBeforeLogTyping)

  await page.evaluate(() => window.__logReplyQueue.app.push("Z".repeat(140 * 1024)))
  await expect(page.locator("#logs-truncated")).toBeVisible()
  await expect.poll(() => page.locator("#logs-output").evaluate((node) => new TextEncoder().encode(node.textContent).byteLength))
    .toBeLessThanOrEqual(128 * 1024)

  await page.locator("#expand-logs").uncheck()
  const stoppedCount = await page.evaluate(() => window.__logsSent.length)
  await page.waitForTimeout(1200)
  expect(await page.evaluate(() => window.__logsSent.length)).toBe(stoppedCount)
  await page.getByRole("button", { name: "Return to agent" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: automation")

  const previousRunId = await page.evaluate(() => window.__viewerMessages
    .filter((message) => message.type === "frame").at(-1).data.frame.run_id)
  const restarted = await mcp("terminal_restart", { session_id: sessionId })
  expect(restarted.run_id).not.toBe(previousRunId)
  await expect.poll(() => page.locator("#frame-identity").textContent()).toContain(` · ${restarted.run_id} · `)
  await expect(page.locator("#logs-output")).toBeEmpty()
  await expect(page.locator("#logs-truncated")).toBeHidden()

  const requestsBeforeResetPoll = await page.evaluate(() => window.__logsSent.length)
  await page.locator("#expand-logs").check()
  await expect.poll(() => page.evaluate((count) => window.__logsSent.length > count, requestsBeforeResetPoll))
    .toBe(true)
  const firstAfterRestart = await page.evaluate(() => window.__logsSent.at(-1))
  expect(firstAfterRestart.source).toBe("app")
  expect(firstAfterRestart.cursor).toBeUndefined()
  await page.locator("#expand-logs").uncheck()
})

test("read-only grant cannot acquire terminal control and wrong Host is refused", async ({ page }) => {
  readOnlyView = await mcp("terminal_view_start", {
    session_id: sessionId,
    allow_browser_control: false,
    idempotency_key: `${runTag}-readonly`,
  })
  const url = new URL(readOnlyView.url)
  await page.addInitScript(() => {
    const NativeWebSocket = window.WebSocket
    window.__viewerSockets = []
    window.WebSocket = new Proxy(NativeWebSocket, {
      construct(target, args) {
        const socket = new target(...args)
        window.__viewerSockets.push(socket)
        return socket
      },
    })
  })
  await page.goto(url.href)
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.getByRole("button", { name: "Take control" })).toBeHidden()
  await page.evaluate(() => window.__viewerSockets[0].send(JSON.stringify({ type: "take_control" })))
  await expect(page.locator("#notice")).toContainText("control_not_permitted")
  const viewStatus = await mcp("terminal_view_status", { view_id: readOnlyView.view_id })
  expect(viewStatus.allow_browser_control).toBe(false)
  expect(viewStatus.active_connections).toBe(1)
  expect((await mcp("terminal_control_status", { session_id: sessionId })).owner).toBe("automation")

  const wrongHost = await page.request.get(url.href.replace("127.0.0.1", "localhost"))
  expect(wrongHost.status()).toBe(403)
  await mcp("terminal_view_stop", { view_id: readOnlyView.view_id })
  await expect(page.locator("#connection")).toHaveText("Viewing link revoked")
})

test("a foreign browser Origin cannot authenticate to the local WebSocket", async ({ page }) => {
  const viewUrl = new URL(originView.url)
  const endpoint = `ws://${viewUrl.host}/api/views/${originView.view_id}/ws`
  const token = new URLSearchParams(viewUrl.hash.slice(1)).get("token")
  await page.goto("data:text/html,<title>origin probe</title>")
  const outcome = await page.evaluate(({ endpoint, token }) => new Promise((resolve) => {
    const socket = new WebSocket(endpoint)
    const timer = setTimeout(() => {
      socket.close()
      resolve("timeout")
    }, 5000)
    socket.onopen = () => socket.send(JSON.stringify({ type: "authenticate", token }))
    socket.onerror = () => {
      clearTimeout(timer)
      resolve("blocked")
    }
    socket.onclose = () => {
      clearTimeout(timer)
      resolve("closed")
    }
  }), { endpoint, token })
  expect(["blocked", "closed"]).toContain(outcome)
  expect((await mcp("terminal_view_status", { view_id: originView.view_id })).active_connections).toBe(0)
})

test("terminal frames are withheld until the browser authenticates its view grant", async ({ page }) => {
  const grant = await mcp("terminal_view_start", {
    session_id: sessionId,
    allow_browser_control: false,
    idempotency_key: `${runTag}-auth-gate`,
  })
  extraViews.push(grant.view_id)
  await page.addInitScript(() => {
    const NativeWebSocket = window.WebSocket
    window.__authenticationSent = false
    window.__authenticationAttempted = false
    window.__preAuthenticationFrames = []
    window.WebSocket = new Proxy(NativeWebSocket, {
      construct(target, args) {
        const socket = new target(...args)
        const send = socket.send.bind(socket)
        socket.send = (data) => {
          let message
          try {
            message = JSON.parse(data)
          } catch {}
          if (message?.type === "authenticate" && !window.__authenticationSent) {
            window.__authenticationAttempted = true
            setTimeout(() => {
              window.__authenticationSent = true
              send(data)
            }, 800)
            return
          }
          send(data)
        }
        socket.addEventListener("message", (event) => {
          if (typeof event.data !== "string") return
          try {
            const received = JSON.parse(event.data)
            if (received.type === "frame" && !window.__authenticationSent) {
              window.__preAuthenticationFrames.push(received)
            }
          } catch {}
        })
        return socket
      },
    })
  })
  await page.goto(grant.url)
  await page.waitForFunction(() => window.__authenticationAttempted === true)
  expect(await page.locator("#accessible-screen").textContent()).toBe("")
  expect(await page.evaluate(() => window.__preAuthenticationFrames)).toEqual([])
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#accessible-screen")).toContainText("READY")
  expect(await page.evaluate(() => window.__preAuthenticationFrames)).toEqual([])
})

test("a view capability cannot authenticate to a different session", async ({ page }) => {
  const cwdA = path.join(emulationDir, ".runtime", `cross-session-a-${runTag}`)
  const cwdB = path.join(emulationDir, ".runtime", `cross-session-b-${runTag}`)
  mkdirSync(cwdA, { recursive: true })
  mkdirSync(cwdB, { recursive: true })
  const launchedA = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd: cwdA,
    name: "cross-session-a",
    cols: 80,
    rows: 24,
  })
  const launchedB = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd: cwdB,
    name: "cross-session-b",
    cols: 80,
    rows: 24,
  })
  extraSessions.push(launchedA.session_id, launchedB.session_id)
  await Promise.all([
    mcp("terminal_wait", {
      session_id: launchedA.session_id,
      kind: "contains",
      value: "READY",
      timeout_ms: 5000,
    }),
    mcp("terminal_wait", {
      session_id: launchedB.session_id,
      kind: "contains",
      value: "READY",
      timeout_ms: 5000,
    }),
  ])
  const viewA = await mcp("terminal_view_start", {
    session_id: launchedA.session_id,
    allow_browser_control: false,
    idempotency_key: `${runTag}-cross-a`,
  })
  const viewB = await mcp("terminal_view_start", {
    session_id: launchedB.session_id,
    allow_browser_control: false,
    idempotency_key: `${runTag}-cross-b`,
  })
  extraViews.push(viewA.view_id, viewB.view_id)

  await page.goto(viewB.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#accessible-screen")).toContainText("READY")
  const wrongSessionResult = await page.evaluate(({ endpoint, token }) => new Promise((resolve) => {
    const socket = new WebSocket(endpoint)
    const timer = setTimeout(() => {
      socket.close()
      resolve("timeout")
    }, 5000)
    socket.onopen = () => socket.send(JSON.stringify({ type: "authenticate", token }))
    socket.onmessage = (event) => {
      if (typeof event.data !== "string") return
      try {
        const message = JSON.parse(event.data)
        if (message.type === "frame") {
          clearTimeout(timer)
          socket.close()
          resolve("frame_received")
        } else if (message.type === "error") {
          clearTimeout(timer)
          socket.close()
          resolve(message.code)
        }
      } catch {}
    }
    socket.onerror = () => {
      clearTimeout(timer)
      resolve("socket_error")
    }
  }), {
    endpoint: `ws://${new URL(viewA.url).host}/api/views/${viewA.view_id}/ws`,
    token: new URLSearchParams(new URL(viewB.url).hash.slice(1)).get("token"),
  })
  expect(wrongSessionResult).toBe("view_authentication_failed")
  expect((await mcp("terminal_view_status", { view_id: viewA.view_id })).active_connections).toBe(0)
  expect((await mcp("terminal_view_status", { view_id: viewB.view_id })).active_connections).toBe(1)
  await expect(page.locator("#accessible-screen")).toContainText("READY")
})

test("mouse input is visibly unavailable when the application did not negotiate it", async ({ page }) => {
  const cwd = path.join(emulationDir, ".runtime", `no-mouse-${runTag}`)
  mkdirSync(cwd, { recursive: true })
  const launched = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: ["--no-mouse"],
    cwd,
    name: "no-mouse-fixture",
    cols: 80,
    rows: 24,
  })
  extraSessions.push(launched.session_id)
  await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "contains",
    value: "READY",
    timeout_ms: 5000,
  })
  const grant = await mcp("terminal_view_start", {
    session_id: launched.session_id,
    allow_browser_control: true,
    idempotency_key: `${runTag}-no-mouse`,
  })
  extraViews.push(grant.view_id)
  const before = await mcp("terminal_observe", { session_id: launched.session_id })

  let mouseError = ""
  try {
    await mcp("terminal_mouse", {
      session_id: launched.session_id,
      action: "click",
      row: 3,
      col: 4,
    })
  } catch (error) {
    mouseError = error.message
  }
  expect(mouseError).toContain("unsupported mouse operation")
  expect((await mcp("terminal_observe", { session_id: launched.session_id })).text).toBe(before.text)

  await page.goto(grant.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  await page.getByRole("button", { name: "Take control" }).click()
  const mouseMode = page.getByRole("button", { name: "Terminal mouse: off" })
  await expect(mouseMode).toBeDisabled()
  await expect(mouseMode).toHaveAttribute("title", "This application has not negotiated supported mouse input")
  await page.getByRole("button", { name: "Return to agent" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: automation")
})

test("mouse cell mapping survives device scale, CSS zoom, and terminal scrolling", async ({ browser }) => {
  const cwd = path.join(emulationDir, ".runtime", `mouse-geometry-${runTag}`)
  mkdirSync(cwd, { recursive: true })
  const launched = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd,
    name: "mouse-geometry-fixture",
    cols: 120,
    rows: 40,
  })
  extraSessions.push(launched.session_id)
  await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "contains",
    value: "READY",
    timeout_ms: 5000,
  })

  const scenarios = [
    { scale: 1, viewport: { width: 1280, height: 800 }, zoom: 1, scrollTop: 0, scrollLeft: 0, row: 3, col: 5 },
    { scale: 2, viewport: { width: 900, height: 640 }, zoom: 0.8, scrollTop: 80, scrollLeft: 60, row: 13, col: 16 },
    { scale: 1, viewport: { width: 720, height: 560 }, zoom: 1.15, scrollTop: 180, scrollLeft: 80, row: 18, col: 24 },
  ]

  for (const [index, scenario] of scenarios.entries()) {
    const grant = await mcp("terminal_view_start", {
      session_id: launched.session_id,
      allow_browser_control: true,
      idempotency_key: `${runTag}-mouse-geometry-${index}`,
    })
    extraViews.push(grant.view_id)
    const context = await browser.newContext({
      viewport: scenario.viewport,
      deviceScaleFactor: scenario.scale,
    })
    try {
      const page = await context.newPage()
      await page.goto(grant.url)
      await expect(page.locator("#connection")).toHaveText("Live")
      await page.getByRole("button", { name: "Take control" }).click()
      await page.getByRole("button", { name: "Terminal mouse: off" }).click()
      await page.evaluate(({ zoom, scrollTop, scrollLeft }) => {
        const canvas = document.querySelector("#terminal")
        canvas.style.zoom = String(zoom)
        const viewport = document.querySelector(".terminal-wrap")
        viewport.scrollTop = scrollTop
        viewport.scrollLeft = scrollLeft
      }, scenario)

      const bounds = await page.locator("#terminal").boundingBox()
      expect(bounds).not.toBeNull()
      const x = bounds.x + bounds.width * (scenario.col + 0.5) / 120
      const y = bounds.y + bounds.height * (scenario.row + 0.5) / 40
      await page.mouse.click(x, y)
      await expect(page.locator("#accessible-screen")).toContainText(
        `Mouse: code=3 row=${scenario.row} col=${scenario.col}`,
      )
      await page.getByRole("button", { name: "Return to agent" }).click()
      await expect(page.locator("#owner")).toHaveText("Input owner: automation")
    } finally {
      await context.close()
      await mcp("terminal_view_stop", { view_id: grant.view_id }).catch(() => {})
    }
  }
})

test("a stalled viewer is disconnected without blocking the PTY or a healthy viewer", async ({ page }) => {
  const cwd = path.join(emulationDir, ".runtime", `slow-viewer-${runTag}`)
  mkdirSync(cwd, { recursive: true })
  const launched = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd,
    name: "slow-viewer-fixture",
    cols: 120,
    rows: 40,
  })
  extraSessions.push(launched.session_id)
  await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "contains",
    value: "READY",
    timeout_ms: 5000,
  })
  await mcp("terminal_resize", { session_id: launched.session_id, cols: 300, rows: 120 })

  const slowGrant = await mcp("terminal_view_start", {
    session_id: launched.session_id,
    allow_browser_control: false,
    idempotency_key: `${runTag}-slow-observer`,
  })
  extraViews.push(slowGrant.view_id)
  const healthyGrant = await mcp("terminal_view_start", {
    session_id: launched.session_id,
    allow_browser_control: false,
    idempotency_key: `${runTag}-healthy-observer`,
  })
  extraViews.push(healthyGrant.view_id)

  await page.goto(healthyGrant.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#geometry")).toContainText("300 × 120 cells")

  const parsed = new URL(slowGrant.url)
  const slowViewId = parsed.pathname.split("/").filter(Boolean).at(-1)
  const token = new URLSearchParams(parsed.hash.slice(1)).get("token")
  const slowSocket = createConnection({ host: "127.0.0.1", port: Number(parsed.port) })
  slowSocket.setNoDelay(true)
  let settled = false
  try {
    const upgrade = await new Promise((resolve, reject) => {
      let response = Buffer.alloc(0)
      slowSocket.on("error", (error) => {
        if (!settled) reject(error)
      })
      slowSocket.once("connect", () => {
        const key = randomBytes(16).toString("base64")
        slowSocket.write(
          `GET /api/views/${slowViewId}/ws HTTP/1.1\r\n` +
            `Host: ${parsed.host}\r\n` +
            `Origin: ${parsed.origin}\r\n` +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            `Sec-WebSocket-Key: ${key}\r\n` +
            "Sec-WebSocket-Version: 13\r\n\r\n",
        )
      })
      const onData = (chunk) => {
        response = Buffer.concat([response, chunk])
        const end = response.indexOf("\r\n\r\n")
        if (end < 0) return
        slowSocket.removeListener("data", onData)
        resolve(response.subarray(0, end).toString("latin1"))
      }
      slowSocket.on("data", onData)
    })
    expect(upgrade).toContain("HTTP/1.1 101")

    const payload = Buffer.from(JSON.stringify({ type: "authenticate", token }))
    const mask = randomBytes(4)
    const maskedPayload = Buffer.alloc(payload.length)
    for (let index = 0; index < payload.length; index++) {
      maskedPayload[index] = payload[index] ^ mask[index % mask.length]
    }
    slowSocket.pause()
    slowSocket.write(Buffer.concat([Buffer.from([0x81, 0x80 | payload.length]), mask, maskedPayload]))
    settled = true

    await expect
      .poll(async () => (await mcp("terminal_view_status", { view_id: slowGrant.view_id })).active_connections)
      .toBe(1)

    for (let index = 0; index < 120; index++) {
      await mcp("terminal_type", { session_id: launched.session_id, text: "x" })
    }
    await mcp("terminal_type", { session_id: launched.session_id, text: "SLOWTAIL" })

    await expect(page.locator("#accessible-screen")).toContainText("SLOWTAIL", { timeout: 10_000 })
    const observed = await mcp("terminal_observe", { session_id: launched.session_id })
    expect(observed.text).toContain("SLOWTAIL")
    await expect
      .poll(async () => (await mcp("terminal_view_status", { view_id: slowGrant.view_id })).active_connections, {
        timeout: 10_000,
      })
      .toBe(0)
    expect((await mcp("terminal_view_status", { view_id: healthyGrant.view_id })).active_connections).toBe(1)
  } finally {
    slowSocket.destroy()
  }
})

test("full and half block characters fill the intended parts of their terminal cells", async ({ page }) => {
  const cwd = path.join(emulationDir, ".runtime", `block-glyph-${runTag}`)
  mkdirSync(cwd, { recursive: true })
  const launched = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd,
    name: "block-glyph-fixture",
    cols: 120,
    rows: 40,
  })
  extraSessions.push(launched.session_id)
  await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "contains",
    value: "READY",
    timeout_ms: 5000,
  })
  const grant = await mcp("terminal_view_start", {
    session_id: launched.session_id,
    allow_browser_control: false,
    idempotency_key: `${runTag}-block-glyph`,
  })
  extraViews.push(grant.view_id)

  await page.goto(grant.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  const glyphs = "█▀▄▌▐"
  await mcp("terminal_type", { session_id: launched.session_id, text: glyphs })
  await expect(page.locator("#accessible-screen")).toContainText(`Typed: ${glyphs}`)

  const samples = await page.evaluate(() => {
    const canvas = document.querySelector("#terminal")
    const bounds = canvas.getBoundingClientRect()
    const lines = document.querySelector("#accessible-screen").textContent.split("\n")
    const row = lines.findIndex((line) => line.includes("Typed: █▀▄▌▐"))
    const col = lines[row].indexOf("█▀▄▌▐")
    const cellWidth = bounds.width / 120
    const cellHeight = bounds.height / 40
    const scaleX = canvas.width / bounds.width
    const scaleY = canvas.height / bounds.height
    const context = canvas.getContext("2d")
    const pixel = (xFraction, yFraction) => {
      const x = Math.floor((col + xFraction) * cellWidth * scaleX)
      const y = Math.floor((row + yFraction) * cellHeight * scaleY)
      return Array.from(context.getImageData(x, y, 1, 1).data.slice(0, 3))
    }
    return {
      fullBlock: pixel(0.1, 0.5),
      upperHalf: [pixel(1.5, 0.25), pixel(1.5, 0.75)],
      lowerHalf: [pixel(2.5, 0.25), pixel(2.5, 0.75)],
      leftHalf: [pixel(3.25, 0.5), pixel(3.75, 0.5)],
      rightHalf: [pixel(4.25, 0.5), pixel(4.75, 0.5)],
    }
  })
  const foreground = (color) => {
    const [red, green, blue] = color
    expect(red).toBeGreaterThan(100)
    expect(green).toBeGreaterThan(100)
    expect(blue).toBeGreaterThan(100)
  }
  const background = (color) => color.forEach((channel) => expect(channel).toBeLessThan(30))
  foreground(samples.fullBlock)
  foreground(samples.upperHalf[0])
  background(samples.upperHalf[1])
  background(samples.lowerHalf[0])
  foreground(samples.lowerHalf[1])
  foreground(samples.leftHalf[0])
  background(samples.leftHalf[1])
  background(samples.rightHalf[0])
  foreground(samples.rightHalf[1])
})

test("a closed session keeps its final frame available to its existing view", async ({ page }) => {
  const cwd = path.join(emulationDir, ".runtime", `close-view-${runTag}`)
  mkdirSync(cwd, { recursive: true })
  const launched = await mcp("terminal_launch", {
    program: fixtureProgram,
    args: [],
    cwd,
    name: "closed-view-fixture",
    cols: 80,
    rows: 24,
  })
  extraSessions.push(launched.session_id)
  const ready = await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "contains",
    value: "READY",
    timeout_ms: 5000,
  })
  expect(ready.matched).toBe(true)
  const view = await mcp("terminal_view_start", {
    session_id: launched.session_id,
    allow_browser_control: false,
    idempotency_key: `${runTag}-closed-view`,
  })
  extraViews.push(view.view_id)
  await page.goto(view.url)
  await expect(page.locator("#accessible-screen")).toContainText("READY")

  await mcp("terminal_close", { session_id: launched.session_id })
  await expect(page.locator("#geometry")).toContainText("application closed")
  await expect(page.locator("#accessible-screen")).toContainText("READY")
  await page.reload()
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#geometry")).toContainText("application closed")
  await expect(page.locator("#accessible-screen")).toContainText("READY")

  await mcp("terminal_view_stop", { view_id: view.view_id })
  await expect(page.locator("#connection")).toHaveText("Viewing link revoked")
})

test("the same viewer edits and saves a second terminal application", async ({ page }) => {
  const nvim = process.env.NVIM_BIN || "/usr/bin/nvim"
  expect(existsSync(nvim)).toBe(true)
  const cwd = path.join(emulationDir, ".runtime", `nvim-${runTag}`)
  mkdirSync(cwd, { recursive: true })
  const launched = await mcp("terminal_launch", {
    program: nvim,
    args: ["--clean", "browser-notes.md"],
    cwd,
    name: "neovim-browser-fixture",
    cols: 100,
    rows: 30,
  })
  extraSessions.push(launched.session_id)
  const ready = await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "stable",
    duration_ms: 300,
    timeout_ms: 5000,
  })
  expect(ready.matched).toBe(true)
  const view = await mcp("terminal_view_start", {
    session_id: launched.session_id,
    allow_browser_control: true,
    idempotency_key: `${runTag}-nvim`,
  })
  extraViews.push(view.view_id)

  await page.goto(view.url)
  await expect(page.locator("#connection")).toHaveText("Live")
  await expect(page.locator("#geometry")).toContainText("100 × 30 cells")
  await page.getByRole("button", { name: "Take control" }).click()
  await expect(page.locator("#owner")).toHaveText("Input owner: browser")
  await page.locator("#keyboard-input").focus()
  await page.keyboard.type("i")
  await page.waitForFunction(() => document.querySelector("#accessible-screen").textContent.includes("-- INSERT --"))
  await page.keyboard.type("Automated viewer edit")
  await expect(page.locator("#accessible-screen")).toContainText("Automated viewer edit")
  await page.screenshot({ path: path.join(artifactDir, "neovim-edited.png"), fullPage: true })
  await page.keyboard.press("Escape")
  await page.keyboard.type(":wq")
  await page.keyboard.press("Enter")

  const exited = await mcp("terminal_wait", {
    session_id: launched.session_id,
    kind: "process_exited",
    timeout_ms: 10000,
  })
  expect(exited.matched).toBe(true)
  expect(readFileSync(path.join(cwd, "browser-notes.md"), "utf8")).toContain("Automated viewer edit")
  await expect(page.locator("#geometry")).toContainText("application exited")
  await expect(page.locator("#owner")).toHaveText("Input owner: automation")
})

test("browser filters and opens a seeded worker in the real Silicogen Subagents TUI", async ({ page, context }) => {
  const port = await freeLoopbackPort()
  const demo = spawnSync("bash", [path.join(emulationDir, "fixtures/run-silicogen-subagents-demo.sh")], {
    cwd: emulationDir,
    env: { ...process.env, SILICOGEN_FIXTURE_PORT: String(port) },
    encoding: "utf8",
    maxBuffer: 4 * 1024 * 1024,
  })
  if (demo.status !== 0) {
    throw new Error(`Silicogen display fixture failed: ${demo.stderr || demo.stdout}`)
  }
  const output = `${demo.stdout}\n${demo.stderr}`
  const sessionId = output.match(/^TUI session:\s+(\S+)$/m)?.[1]
  const runtimeDirectory = output.match(/^Runtime data:\s+(.+)$/m)?.[1]
  expect(sessionId).toBeTruthy()
  expect(runtimeDirectory).toBeTruthy()

  const fixtureClient = new Client({ name: "silicogen-subagents-browser-test", version: "1.0.0" })
  const transport = new StdioClientTransport({
    command: path.join(emulationDir, "target/release/silicogen-emulation"),
    args: ["--runtime-dir", runtimeDirectory, "mcp"],
    cwd: emulationDir,
    env: { ...process.env },
    stderr: "pipe",
  })
  let view
  try {
    await fixtureClient.connect(transport)
    view = await callClient(fixtureClient, "terminal_view_start", {
      session_id: sessionId,
      allow_browser_control: true,
      idempotency_key: `${runTag}-silicogen` ,
    })
    await context.grantPermissions(["clipboard-read", "clipboard-write"], {
      origin: new URL(view.url).origin,
    })
    await page.goto(view.url)
    await expect(page.locator("#connection")).toHaveText("Live")
    await expect(page.locator("#accessible-screen")).toContainText("clarification_waiter")
    await expect(page.locator("#accessible-screen")).toContainText("rtl_width_audit")
    await expect(page.locator("#accessible-screen")).toContainText("questa_flow_audit")
    await page.getByRole("button", { name: "Take control" }).click()

    const initialScreen = (await page.locator("#accessible-screen").textContent()).split("\n")
    const filterRow = initialScreen.findIndex((line) => line.includes("Filter subagents"))
    expect(filterRow).toBeGreaterThanOrEqual(0)
    const filterCol = initialScreen[filterRow].indexOf("Filter subagents") + 3
    await page.getByRole("button", { name: "Terminal mouse: off" }).click()
    const canvas = page.locator("#terminal")
    const bounds = await canvas.boundingBox()
    await page.mouse.click(
      bounds.x + bounds.width * (filterCol + 0.5) / 120,
      bounds.y + bounds.height * (filterRow + 0.5) / 40,
    )
    await page.evaluate(() => navigator.clipboard.writeText("rtl_width_audit"))
    await page.locator("#keyboard-input").focus()
    await page.keyboard.press("Control+V")
    await page.waitForFunction(() => {
      const text = document.querySelector("#accessible-screen").textContent
      return text.includes("rtl_width_audit") && !text.includes("questa_flow_audit")
    }, null, { timeout: 10000 })

    const filtered = (await page.locator("#accessible-screen").textContent()).split("\n")
    const workerRow = filtered.findIndex((line) =>
      line.includes("◇") && line.includes("rtl_width_audit") && line.includes("Reviewing"),
    )
    expect(workerRow).toBeGreaterThanOrEqual(0)
    const workerCol = filtered[workerRow].indexOf("rtl_width_audit") + 3
    await page.mouse.click(
      bounds.x + bounds.width * (workerCol + 0.5) / 120,
      bounds.y + bounds.height * (workerRow + 0.5) / 40,
    )
    await page.locator("#keyboard-input").focus()
    await page.keyboard.press("Enter")
    await expect(page.locator("#accessible-screen")).toContainText("Fixture worker · rtl_width_audit")
    await page.screenshot({
      path: path.join(artifactDir, "silicogen-subagents-browser.png"),
      fullPage: true,
    })
    expect((await callClient(fixtureClient, "terminal_status", { session_id: sessionId })).state).toBe("running")
  } finally {
    await page.close().catch(() => {})
    if (view?.view_id) {
      await callClient(fixtureClient, "terminal_view_stop", { view_id: view.view_id }).catch(() => {})
    }
    const sessions = await callClient(fixtureClient, "terminal_list", {}).catch(() => [])
    for (const session of Array.isArray(sessions) ? sessions : []) {
      if (session.state === "running") {
        await callClient(fixtureClient, "terminal_close", { session_id: session.session_id }).catch(() => {})
      }
    }
    await fixtureClient.close().catch(() => {})
    spawnSync(
      path.join(emulationDir, "target/release/silicogen-emulation"),
      ["--runtime-dir", runtimeDirectory, "shutdown"],
      { cwd: emulationDir, env: { ...process.env }, encoding: "utf8" },
    )
  }
})
