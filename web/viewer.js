const byId = (id) => document.getElementById(id)
const canvas = byId("terminal")
const ctx = canvas.getContext("2d", { alpha: false })
const input = byId("keyboard-input")
const connection = byId("connection")
const notice = byId("notice")
const viewId = location.pathname.split("/").filter(Boolean).at(-1)
const storageKey = `terminal-view:${viewId}:reconnect`

let frame = null
let permission = false
let browserLease = null
let mouseMode = false
let socket = null
let reconnectDelay = 250
let reconnectTimer = null
let snapshotPending = null
let terminalClosed = false
let streamFailed = false
let cellWidth = 9
let cellHeight = 18
let cellZoom = 1
let launchSize = null
let lastPointerAt = 0
let lastGeometryKey = null
let frameRunId = null
let viewerAuthenticated = false
let logsInterval = null
let logsInFlight = null
let logsGeneration = 0
let logCursors = Object.create(null)
let logGaps = Object.create(null)
let logChunks = []
let logBufferBytes = 0
let logBufferTruncated = false
const LOG_BUFFER_LIMIT = 128 * 1024
const logEncoder = new TextEncoder()

let bootstrap = new URLSearchParams(location.hash.slice(1)).get("token")
if (location.hash) history.replaceState(null, "", location.pathname)

function setNotice(message, tone = "") {
  notice.textContent = message || ""
  notice.dataset.tone = tone
}

function setConnection(state, message) {
  connection.dataset.state = state
  connection.textContent = message
}

function send(message) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    setNotice("Viewer is reconnecting. No input was sent.", "error")
    return false
  }
  socket.send(JSON.stringify(message))
  return true
}

function connect() {
  const token = bootstrap || sessionStorage.getItem(storageKey)
  if (!token) {
    setConnection("error", "Viewing link expired")
    setNotice("Ask the agent to create a new local viewing link.", "error")
    return
  }
  const endpoint = new URL(`/api/views/${encodeURIComponent(viewId)}/ws`, location.href)
  endpoint.protocol = location.protocol === "https:" ? "wss:" : "ws:"
  const ws = new WebSocket(endpoint)
  socket = ws
  setConnection("connecting", "Connecting…")
  ws.addEventListener("open", () => {
    if (socket !== ws) return
    reconnectDelay = 250
    send({ type: "authenticate", token })
  })
  ws.addEventListener("message", (event) => {
    if (socket === ws) onMessage(event)
  })
  ws.addEventListener("close", () => {
    if (socket !== ws) return
    viewerAuthenticated = false
    logsInFlight = null
    syncLogPolling()
    browserLease = null
    updateControls()
    if (terminalClosed) return
    setConnection("connecting", "Connection lost · retrying")
    clearTimeout(reconnectTimer)
    reconnectTimer = setTimeout(connect, reconnectDelay)
    reconnectDelay = Math.min(5000, reconnectDelay * 2)
  })
  ws.addEventListener("error", () => {
    if (socket === ws) setConnection("error", "Connection error")
  })
}

function onMessage(event) {
  if (event.data instanceof Blob) {
    saveSnapshot(event.data)
    return
  }
  let message
  try {
    message = JSON.parse(event.data)
  } catch {
    setNotice("The viewer received an invalid server message.", "error")
    return
  }
  switch (message.type) {
    case "authenticated":
      viewerAuthenticated = true
      permission = message.permissions?.request_control === true
      if (message.reconnect_secret) {
        sessionStorage.setItem(storageKey, message.reconnect_secret)
        bootstrap = null
      }
      setConnection("connected", "Live")
      updateControls()
      syncLogPolling()
      break
    case "hello":
      byId("session-name").textContent = message.session?.name || message.session?.program || "Terminal session"
      byId("session-path").textContent = `${message.session?.cwd || ""} · isolated app state · local process`
      if (Number.isInteger(message.initial_size?.cols) && Number.isInteger(message.initial_size?.rows)) {
        launchSize = { cols: message.initial_size.cols, rows: message.initial_size.rows }
      }
      updateControls()
      break
    case "frame":
      streamFailed = false
      setConnection("connected", "Live")
      const incomingRunId = message.data?.frame?.run_id
      if (incomingRunId && incomingRunId !== frameRunId) resetLogsForRun(incomingRunId)
      frame = message.data
      render(frame)
      updateControls()
      setNotice(frame.frame.capture_complete ? "" : (frame.frame.capture_warning || "Terminal capture is incomplete."), frame.frame.capture_complete ? "" : "error")
      break
    case "logs":
      handleLogs(message.data)
      break
    case "logs_error":
      handleLogsError(message)
      break
    case "stream_error":
      streamFailed = true
      setConnection("error", "Capture error")
      setNotice(message.message || message.code || "Terminal capture failed.", "error")
      updateControls()
      break
    case "heartbeat":
      send({
        type: "heartbeat_ack",
        lease_id: browserLease?.lease_id || null,
        generation: browserLease?.generation ?? null,
      })
      break
    case "control_changed":
      if (message.owner === "automation" || message.status?.owner === "automation") browserLease = null
      if (message.lease?.view_id === viewId) {
        browserLease = message.lease
        if (!logsUiHasFocus()) input.focus()
      }
      setNotice(message.reason || "Input ownership changed.")
      updateControls()
      break
    case "control_renewed":
      break
    case "action_result":
      if (!message.accepted) setNotice(message.error || "Input was rejected.", "error")
      break
    case "control_error":
      setNotice(message.message || message.code || "Control request was rejected.", "error")
      break
    case "snapshot_ready":
      snapshotPending = message
      setNotice(`Saving frame ${message.frame_id}…`)
      break
    case "snapshot_error":
      snapshotPending = null
      setNotice(message.code === "frame_not_retained" ? "That displayed frame has expired from the short snapshot cache." : message.code, "error")
      break
    case "grant_revoked":
      terminalClosed = true
      setConnection("error", "Viewing link revoked")
      setNotice("This view was closed by the session owner.", "error")
      socket.close()
      break
    case "grant_expired":
      terminalClosed = true
      sessionStorage.removeItem(storageKey)
      setConnection("error", "Viewing grant expired")
      setNotice("Ask the agent to create a new local viewing link.", "error")
      socket.close()
      break
    case "error":
      setNotice(message.message || message.code || "Viewer error", "error")
      if (["view_authentication_failed", "view_grant_expired"].includes(message.code)) {
        terminalClosed = true
        sessionStorage.removeItem(storageKey)
        setConnection("error", "Viewing link expired")
        socket.close()
      }
      break
    default:
      break
  }
}

function logsUiHasFocus() {
  const active = document.activeElement
  return active === byId("expand-logs") || byId("logs-panel").contains(active)
}

function resetLogsForRun(runId) {
  frameRunId = runId
  logsGeneration += 1
  logCursors = Object.create(null)
  logGaps = Object.create(null)
  logChunks = []
  logBufferBytes = 0
  logBufferTruncated = false
  byId("logs-truncated").hidden = true
  byId("logs-gap").hidden = true
  byId("logs-state").dataset.tone = ""
  byId("logs-state").textContent = `New run · following ${byId("log-source").value} logs.`
  renderLogs()
}

function syncLogPolling() {
  const expanded = byId("expand-logs").checked
  byId("logs-panel").hidden = !expanded
  byId("terminal-workspace").classList.toggle("logs-expanded", expanded)
  const live = Boolean(viewerAuthenticated && socket && socket.readyState === WebSocket.OPEN)
  if (expanded && live) {
    if (logsInterval === null) logsInterval = window.setInterval(pollLogs, 1000)
    if (!logsInFlight) setLogStatus(`Following ${logSourceLabel(byId("log-source").value)} · polling every second.`)
  } else {
    if (logsInterval !== null) window.clearInterval(logsInterval)
    logsInterval = null
    if (expanded) setLogStatus("Logs pause while the viewer is disconnected.")
    else setLogStatus("Log polling paused.")
  }
}

function pollLogs() {
  if (!byId("expand-logs").checked || !viewerAuthenticated || !socket || socket.readyState !== WebSocket.OPEN || logsInFlight) return
  const source = byId("log-source").value
  const request = { type: "logs", source }
  if (Object.hasOwn(logCursors, source)) request.cursor = logCursors[source]
  logsInFlight = { source, runId: frameRunId, generation: logsGeneration, socket }
  try {
    socket.send(JSON.stringify(request))
    setLogStatus(`Reading ${logSourceLabel(source)}…`)
  } catch {
    logsInFlight = null
    setLogStatus("Could not request logs. The viewer will retry on the next poll.", "error")
  }
}

function handleLogs(data) {
  const request = logsInFlight
  if (!request) return
  logsInFlight = null
  if (request.socket !== socket || request.generation !== logsGeneration || request.runId !== frameRunId) return
  if (!data || typeof data !== "object") {
    setLogStatus("The log response was empty or invalid.", "error")
    return
  }
  const source = request.source
  if (Object.hasOwn(data, "cursor")) {
    if (data.cursor == null) delete logCursors[source]
    else logCursors[source] = data.cursor
  }
  if (typeof data.text === "string" && data.text.length) appendLogText(source, data.text)
  if (data.gap) {
    const detail = typeof data.gap === "string" ? data.gap : "the log file changed"
    logGaps[source] = detail
    renderLogs()
    setLogStatus(`Log gap detected (${detail}); continuing from the new file.`)
  } else if (data.pending) {
    setLogStatus(`Waiting for ${logSourceLabel(source)} output…`)
  } else {
    setLogStatus(`Following ${logSourceLabel(source)} · polling every second.`)
  }
}

function handleLogsError(message) {
  const request = logsInFlight
  if (!request) return
  logsInFlight = null
  if (request.socket !== socket || request.generation !== logsGeneration || request.runId !== frameRunId) return
  setLogStatus(message.message || "The server could not read this log source.", "error")
}

function appendLogText(source, text) {
  let bytes = logEncoder.encode(text).byteLength
  if (bytes > LOG_BUFFER_LIMIT) {
    const trimmed = trimUtf8Prefix(text, bytes - LOG_BUFFER_LIMIT)
    text = trimmed.text
    bytes -= trimmed.removedBytes
    logBufferTruncated = true
  }
  logChunks.push({ source, text, bytes })
  logBufferBytes += bytes
  while (logBufferBytes > LOG_BUFFER_LIMIT && logChunks.length) {
    const oldest = logChunks[0]
    const trimmed = trimUtf8Prefix(oldest.text, logBufferBytes - LOG_BUFFER_LIMIT)
    if (trimmed.removedBytes === 0) {
      const firstCharacter = Array.from(oldest.text)[0]
      if (!firstCharacter) {
        logChunks.shift()
        continue
      }
      const removed = logEncoder.encode(firstCharacter).byteLength
      oldest.text = oldest.text.slice(firstCharacter.length)
      oldest.bytes -= removed
      logBufferBytes -= removed
    } else {
      oldest.text = trimmed.text
      oldest.bytes -= trimmed.removedBytes
      logBufferBytes -= trimmed.removedBytes
    }
    if (!oldest.text) logChunks.shift()
    logBufferTruncated = true
  }
  byId("logs-truncated").hidden = !logBufferTruncated
  renderLogs()
}

function trimUtf8Prefix(text, targetBytes) {
  let index = 0
  let removedBytes = 0
  for (const character of text) {
    const characterBytes = logEncoder.encode(character).byteLength
    if (removedBytes + characterBytes > targetBytes) break
    removedBytes += characterBytes
    index += character.length
  }
  return { text: text.slice(index), removedBytes }
}

function renderLogs() {
  const output = byId("logs-output")
  const stickToBottom = output.scrollHeight - output.scrollTop - output.clientHeight < 12
  const source = byId("log-source").value
  output.textContent = logChunks.filter((chunk) => chunk.source === source).map((chunk) => chunk.text).join("")
  byId("logs-truncated").hidden = !logBufferTruncated
  const gap = logGaps[source]
  byId("logs-gap").hidden = !gap
  byId("logs-gap").textContent = gap ? `Log stream reported a gap (${gap}); new entries continue below.` : ""
  if (stickToBottom) output.scrollTop = output.scrollHeight
}

function setLogStatus(message, tone = "") {
  const status = byId("logs-state")
  status.textContent = message
  status.dataset.tone = tone
}

function logSourceLabel(source) {
  return source === "startup" ? "startup trace" : "app logs"
}

byId("expand-logs").addEventListener("change", syncLogPolling)
byId("log-source").addEventListener("change", () => {
  renderLogs()
  if (byId("expand-logs").checked && viewerAuthenticated) {
    setLogStatus(`Following ${logSourceLabel(byId("log-source").value)} · polling every second.`)
  } else {
    setLogStatus("Log polling paused.")
  }
})

function saveSnapshot(blob) {
  if (!snapshotPending) return
  const snapshot = snapshotPending
  snapshotPending = false
  const file = new File([blob], `terminal-${snapshot.session_id || "session"}-${snapshot.run_id || "run"}-${snapshot.frame_id || "frame"}.png`, { type: "image/png" })
  const url = URL.createObjectURL(file)
  const link = document.createElement("a")
  link.href = url
  link.download = file.name
  link.click()
  URL.revokeObjectURL(url)
  setNotice("Saved the displayed terminal frame as PNG.")
}

function render(nextFrame) {
  if (!nextFrame?.frame || !Array.isArray(nextFrame.cells)) return
  const terminal = nextFrame.frame
  const deviceScale = Math.max(1, window.devicePixelRatio || 1)
  const family = 'EmulationMono, monospace'
  const fontSize = 14 * cellZoom
  ctx.font = `${fontSize}px ${family}`
  const metrics = ctx.measureText("M")
  cellWidth = Math.max(1, metrics.width)
  cellHeight = Math.max(16 * cellZoom, (metrics.actualBoundingBoxAscent || 11) + (metrics.actualBoundingBoxDescent || 4) + 3 * cellZoom)
  const cssWidth = Math.ceil(terminal.cols * cellWidth)
  const cssHeight = Math.ceil(terminal.rows * cellHeight)
  const maxBackingPixels = 12_000_000
  const requestedScale = deviceScale * 2
  const boundedScale = Math.sqrt(maxBackingPixels / (cssWidth * cssHeight))
  const pixelScale = Math.max(1, Math.min(requestedScale, boundedScale))
  canvas.width = Math.ceil(cssWidth * pixelScale)
  canvas.height = Math.ceil(cssHeight * pixelScale)
  canvas.style.width = `${cssWidth}px`
  canvas.style.height = `${cssHeight}px`
  canvas.dataset.pixelScale = pixelScale.toFixed(2)
  ctx.setTransform(pixelScale, 0, 0, pixelScale, 0, 0)
  ctx.font = `${fontSize}px ${family}`
  ctx.textBaseline = "alphabetic"
  ctx.fillStyle = terminal.default_background
  ctx.fillRect(0, 0, cssWidth, cssHeight)

  const styles = nextFrame.styles
  for (let row = 0; row < nextFrame.cells.length; row++) {
    const cells = nextFrame.cells[row]
    for (let col = 0; col < cells.length; col++) {
      const [text, width, styleIndex, continuation] = cells[col]
      const style = styles[styleIndex] || nextFrame.frame.base_style
      const span = Math.max(1, width)
      const x = col * cellWidth
      const y = row * cellHeight
      ctx.fillStyle = style.background
      ctx.fillRect(x, y, cellWidth * span + 0.25, cellHeight)
      if (continuation || !text) continue
      ctx.fillStyle = style.foreground
      ctx.font = `${style.italic ? "italic " : ""}${style.bold ? "bold " : ""}${fontSize}px ${family}`
      ctx.save()
      ctx.beginPath()
      ctx.rect(x, y, cellWidth * span, cellHeight)
      ctx.clip()
      const baseline = y + Math.floor((cellHeight - ((metrics.actualBoundingBoxAscent || 11) + (metrics.actualBoundingBoxDescent || 4))) / 2) + (metrics.actualBoundingBoxAscent || 11)
      ctx.fillText(text, x, baseline)
      if (style.underline !== "none") {
        ctx.strokeStyle = style.underline_color ? colorValue(style.underline_color, nextFrame.frame.palette) : style.foreground
        ctx.lineWidth = style.underline === "double" ? 2 : 1
        ctx.beginPath()
        ctx.moveTo(x, y + cellHeight - 2)
        ctx.lineTo(x + cellWidth * span, y + cellHeight - 2)
        ctx.stroke()
      }
      if (style.strike) {
        ctx.beginPath()
        ctx.moveTo(x, y + cellHeight / 2)
        ctx.lineTo(x + cellWidth * span, y + cellHeight / 2)
        ctx.stroke()
      }
      ctx.restore()
    }
  }

  drawCursor(terminal.cursor)
  byId("accessible-screen").textContent = terminal.text
  byId("geometry").textContent = `${terminal.cols} × ${terminal.rows} cells · application ${nextFrame.session_state}`
  byId("frame-identity").textContent = `${terminal.session_id} · ${terminal.run_id} · ${terminal.frame_id} · r${nextFrame.view_revision}`
  const geometryKey = `${terminal.run_id}:${nextFrame.viewport_revision}:${terminal.cols}x${terminal.rows}`
  if (geometryKey !== lastGeometryKey) {
    byId("columns").value = terminal.cols
    byId("rows").value = terminal.rows
    lastGeometryKey = geometryKey
  }
  byId("save-frame").disabled = false
  byId("scale-value").textContent = `${Math.round(cellZoom * 100)}%`
  if (!launchSize) launchSize = { cols: terminal.cols, rows: terminal.rows }
}

function colorValue(color, palette) {
  if (typeof color === "string") return color
  if (color?.kind === "rgb" && Array.isArray(color.value)) return `rgb(${color.value.join(",")})`
  if (color?.kind === "indexed") return palette[color.value] || "#ffffff"
  return "#ffffff"
}

function drawCursor(cursor) {
  if (!cursor?.visible || !frame) return
  const x = cursor.col * cellWidth
  const y = cursor.row * cellHeight
  ctx.strokeStyle = cursor.color || "#ffffff"
  ctx.lineWidth = 1
  if (cursor.shape === "bar") {
    ctx.fillStyle = cursor.color || "#ffffff"
    ctx.fillRect(x, y, 2, cellHeight)
  } else if (cursor.shape === "underline") {
    ctx.fillStyle = cursor.color || "#ffffff"
    ctx.fillRect(x, y + cellHeight - 2, cellWidth, 2)
  } else {
    ctx.strokeRect(x + .5, y + .5, cellWidth - 1, cellHeight - 1)
  }
}

function updateControls() {
  const hasFrame = Boolean(frame) && !streamFailed
  const appRunning = hasFrame && frame.session_state === "running" && frame.frame.process_state === "running"
  const canControl = permission && appRunning && Boolean(socket && socket.readyState === WebSocket.OPEN)
  byId("take-control").hidden = !canControl || Boolean(browserLease)
  byId("release-control").hidden = !browserLease
  const mouseCapable = hasFrame && frame.frame.mouse_tracking !== "none" && frame.frame.mouse_encoding === "sgr"
  byId("mouse-mode").disabled = !browserLease || !appRunning || !mouseCapable
  byId("mouse-mode").title = mouseCapable ? "Send terminal mouse events while enabled" : "This application has not negotiated supported mouse input"
  byId("resize-terminal").disabled = !browserLease || !appRunning
  const atLaunchSize = launchSize && frame?.frame.cols === launchSize.cols && frame?.frame.rows === launchSize.rows
  byId("reset-size").disabled = !browserLease || !appRunning || atLaunchSize
  if (launchSize) {
    byId("reset-size").setAttribute("aria-label", `Reset terminal to launch size (${launchSize.cols} × ${launchSize.rows})`)
    byId("reset-size").title = `Restore the launch dimensions: ${launchSize.cols} × ${launchSize.rows}`
  }
  byId("owner").textContent = `Input owner: ${frame?.control?.owner || "automation"}`
  byId("owner").dataset.owner = frame?.control?.owner || "automation"
}

function sendInput(inputMessage) {
  if (!browserLease || !frame) return setNotice("Take control before sending terminal input.", "error")
  if (streamFailed) return setNotice("Terminal capture failed; wait for a valid frame before sending input.", "error")
  if (frame.session_state !== "running" || frame.frame.process_state !== "running") {
    return setNotice("The application has exited; restart it through the agent or CLI.", "error")
  }
  send({
    type: "input",
    action_id: crypto.randomUUID(),
    run_id: frame.frame.run_id,
    frame_id: frame.frame.frame_id,
    view_revision: frame.view_revision,
    viewport_revision: frame.viewport_revision,
    lease_id: browserLease.lease_id,
    generation: browserLease.generation,
    input: inputMessage,
  })
}

byId("take-control").addEventListener("click", () => send({ type: "take_control" }))
byId("release-control").addEventListener("click", () => {
  if (browserLease) send({ type: "release_control", lease_id: browserLease.lease_id, generation: browserLease.generation })
})
byId("mouse-mode").addEventListener("click", () => {
  mouseMode = !mouseMode
  byId("mouse-mode").setAttribute("aria-pressed", String(mouseMode))
  byId("mouse-mode").textContent = `Terminal mouse: ${mouseMode ? "on" : "off"}`
})
byId("copy-screen").addEventListener("click", async () => {
  if (!frame) return
  try {
    await navigator.clipboard.writeText(frame.frame.text)
    setNotice("Copied visible terminal text.")
  } catch {
    setNotice("Clipboard access was denied by the browser.", "error")
  }
})
byId("save-frame").addEventListener("click", () => {
  if (!frame || snapshotPending) return
  snapshotPending = { ...frame.frame, view_revision: frame.view_revision }
  send({ type: "snapshot", run_id: frame.frame.run_id, frame_id: frame.frame.frame_id, view_revision: frame.view_revision })
})
byId("resize-terminal").addEventListener("click", () => {
  sendInput({ kind: "resize", cols: Number(byId("columns").value), rows: Number(byId("rows").value) })
})
byId("reset-size").addEventListener("click", () => {
  if (!launchSize) return
  byId("columns").value = launchSize.cols
  byId("rows").value = launchSize.rows
  sendInput({ kind: "resize", cols: launchSize.cols, rows: launchSize.rows })
})

const zoomLevels = [0.75, 0.9, 1, 1.15, 1.3, 1.5]
function setCellZoom(nextZoom) {
  const nearest = zoomLevels.reduce((best, level) =>
    Math.abs(level - nextZoom) < Math.abs(best - nextZoom) ? level : best, zoomLevels[0])
  cellZoom = nearest
  byId("scale-down").disabled = cellZoom === zoomLevels[0]
  byId("scale-up").disabled = cellZoom === zoomLevels.at(-1)
  byId("scale-value").textContent = `${Math.round(cellZoom * 100)}%`
  if (frame) render(frame)
}
byId("scale-down").addEventListener("click", () => {
  const index = zoomLevels.indexOf(cellZoom)
  setCellZoom(zoomLevels[Math.max(0, index - 1)])
})
byId("scale-up").addEventListener("click", () => {
  const index = zoomLevels.indexOf(cellZoom)
  setCellZoom(zoomLevels[Math.min(zoomLevels.length - 1, index + 1)])
})
byId("scale-reset").addEventListener("click", () => setCellZoom(1))

canvas.addEventListener("click", () => {
  if (browserLease) input.focus()
})
canvas.addEventListener("mousemove", (event) => {
  if (!mouseMode || !browserLease || !frame) return
  if (frame.frame.mouse_tracking !== "motion") return
  if (socket.bufferedAmount > 256 * 1024) return
  const now = performance.now()
  if (now - lastPointerAt < 35) return
  lastPointerAt = now
  const point = cellAt(event)
  if (point) sendInput({ kind: "mouse", input: { kind: "move", ...point } })
})
canvas.addEventListener("click", (event) => {
  if (!mouseMode || !browserLease || !frame) return
  const point = cellAt(event)
  if (point) sendInput({ kind: "mouse", input: { kind: "click", ...point, button: "left" } })
})
canvas.addEventListener("wheel", (event) => {
  if (!mouseMode || !browserLease || !frame) return
  event.preventDefault()
  const point = cellAt(event)
  if (!point) return
  const steps = Math.max(1, Math.min(20, Math.round(Math.abs(event.deltaY) / 100)))
  sendInput({ kind: "mouse", input: { kind: "wheel", ...point, direction: event.deltaY < 0 ? "up" : "down", steps } })
}, { passive: false })

function cellAt(event) {
  const bounds = canvas.getBoundingClientRect()
  const cols = frame.frame.cols
  const rows = frame.frame.rows
  const col = Math.floor((event.clientX - bounds.left) / bounds.width * cols)
  const row = Math.floor((event.clientY - bounds.top) / bounds.height * rows)
  if (row < 0 || col < 0 || row >= rows || col >= cols) return null
  return { row, col }
}

const keyNames = new Map([
  ["ArrowUp", "Up"], ["ArrowDown", "Down"], ["ArrowLeft", "Left"], ["ArrowRight", "Right"],
  ["Escape", "Escape"], ["Enter", "Enter"], ["Tab", "Tab"], ["Backspace", "Backspace"],
  ["Delete", "Delete"], ["Insert", "Insert"], ["Home", "Home"], ["End", "End"],
  ["PageUp", "PageUp"], ["PageDown", "PageDown"],
])
input.addEventListener("keydown", (event) => {
  if (!browserLease) return
  if (event.isComposing) return
  if (event.ctrlKey || event.altKey || event.metaKey) {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "v") return
    const prefix = [event.ctrlKey ? "Ctrl" : "", event.altKey ? "Alt" : "", event.shiftKey ? "Shift" : "", event.metaKey ? "Meta" : ""].filter(Boolean).join("+")
    if (event.key.length === 1) {
      event.preventDefault()
      sendInput({ kind: "key", key: `${prefix}+${event.key.toUpperCase()}` })
      return
    }
  }
  const mapped = keyNames.get(event.key)
  if (mapped) {
    event.preventDefault()
    sendInput({ kind: "key", key: event.shiftKey && event.key === "Tab" ? "Shift+Tab" : mapped })
  } else if (/^F([1-9]|1[0-2])$/.test(event.key)) {
    event.preventDefault()
    sendInput({ kind: "key", key: event.key })
  } else if (Array.from(event.key).length === 1) {
    event.preventDefault()
    sendInput({ kind: "type", text: event.key })
  }
})
input.addEventListener("beforeinput", (event) => {
  if (!browserLease || event.isComposing || event.inputType !== "insertText" || !event.data) return
  event.preventDefault()
  sendInput({ kind: "type", text: event.data })
})
input.addEventListener("compositionend", (event) => {
  if (browserLease && event.data) sendInput({ kind: "type", text: event.data })
  input.value = ""
})
input.addEventListener("paste", (event) => {
  if (!browserLease) return
  const text = event.clipboardData?.getData("text/plain")
  if (text === undefined) return
  event.preventDefault()
  sendInput({ kind: "paste", text })
})

document.fonts.load('14px EmulationMono').then(connect).catch(() => {
  setNotice("The bundled monospace font failed to load; layout may vary.", "error")
  connect()
})
