import fs from "node:fs"
import path from "node:path"
import net from "node:net"
import { fileURLToPath, pathToFileURL } from "node:url"

const [projectDirectory, ...remaining] = process.argv.slice(2)
if (!projectDirectory || remaining.length === 0) {
  throw new Error("usage: silicogen-runtime-wrapper.mjs <project-directory> [--fixture-auth] <entry> [args...]")
}

process.chdir(projectDirectory)
process.env.PWD = projectDirectory

const fixtureDirectory = path.dirname(fileURLToPath(import.meta.url))
const sourceRoot = path.resolve(
  process.env.SILICOGEN_SOURCE_DIR || path.resolve(fixtureDirectory, "../../Silicogen-AI"),
)
const authModule = path.join(sourceRoot, "packages/silicogen/src/auth/index.ts")
if (!fs.existsSync(authModule)) {
  throw new Error(`Silicogen auth module not found under ${sourceRoot}; set SILICOGEN_SOURCE_DIR to the source checkout`)
}

if (remaining[0] === "--fixture-auth") {
  remaining.shift()
  const { Auth } = await import(pathToFileURL(authModule).href)
  await Auth.set("silicogenai", {
    type: "api",
    key: "emulation-fixture-token-not-valid-for-network-use",
  })
  const realFetch = globalThis.fetch.bind(globalThis)
  globalThis.fetch = async (input, init) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url
    if (url === "https://www.silicogenai.space/api/user/me") {
      return new Response(JSON.stringify({ email: "fixture@example.invalid" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      })
    }
    return realFetch(input, init)
  }
}

let provider
let bridgeTimer
let bridgeDirectory
let providerReportTimer
let bridgeBusy = false
let shuttingDown

function within(directory, candidate) {
  const relative = path.relative(directory, candidate)
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative))
}

async function startAcceptanceProvider() {
  const configuredScript = process.env.SILICOGEN_FIXTURE_PROVIDER_SCRIPT
  if (!configuredScript) return
  const script = path.resolve(configuredScript)
  const scriptsRoot = path.resolve(fixtureDirectory, "provider-scripts")
  if (!within(scriptsRoot, script) || path.extname(script) !== ".json" || !fs.existsSync(script) || !fs.statSync(script).isFile()) {
    throw new Error("acceptance provider script must be a JSON file directly under fixtures/provider-scripts")
  }

  const { startProviderFixture } = await import(pathToFileURL(path.join(fixtureDirectory, "scripted-provider.mjs")))
  provider = await startProviderFixture({
    scriptPath: script,
    host: "127.0.0.1",
    port: 0,
    reportDir: ".runtime/provider",
  })
  const providerReport = path.join(projectDirectory, ".runtime", "provider", "provider-report.json")
  const flushProviderReport = () => {
    fs.mkdirSync(path.dirname(providerReport), { recursive: true, mode: 0o700 })
    fs.writeFileSync(providerReport, `${JSON.stringify(provider.getReport(), null, 2)}\n`, { mode: 0o600 })
  }
  flushProviderReport()
  providerReportTimer = setInterval(flushProviderReport, 250)
  providerReportTimer.unref()

  const providerPort = Number(new URL(provider.baseURL).port)
  if (Number(process.env.SILICOGEN_ACCEPTANCE_API_PORT) === providerPort) {
    const reservation = net.createServer()
    await new Promise((resolve, reject) => reservation.once("error", reject).listen(0, "127.0.0.1", resolve))
    const appPort = reservation.address().port
    await new Promise((resolve) => reservation.close(resolve))
    process.env.SILICOGEN_ACCEPTANCE_API_PORT = String(appPort)
    const portFlag = remaining.indexOf("--port")
    if (portFlag >= 0) remaining[portFlag + 1] = String(appPort)
  }

  const configPath = path.join(projectDirectory, "silicogen.json")
  const project = path.resolve(projectDirectory)
  if (!within(project, path.resolve(configPath))) throw new Error("fixture config path escaped the copied workspace")
  const config = JSON.parse(fs.readFileSync(configPath, "utf8"))
  const fixture = config.provider?.["emulation-fixture"]
  if (!fixture || fixture.id !== "emulation-fixture") throw new Error("copied project has no dedicated emulation-fixture provider")
  fixture.api = provider.baseURL
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 })

  const readyPath = path.join(projectDirectory, ".runtime", "fixture-provider-ready.json")
  fs.mkdirSync(path.dirname(readyPath), { recursive: true, mode: 0o700 })
  fs.writeFileSync(readyPath, `${JSON.stringify({ baseURL: provider.baseURL, protocol: "openai-chat-completions" })}\n`, { mode: 0o600 })
}

function routeAllowed(method, pathname) {
  if (method === "GET" && pathname === "/session") return true
  if (!/^\/session\/[A-Za-z0-9_-]+(?:\/(?:message|recap))?$/.test(pathname)) return false
  if (method === "GET") return true
  return method === "POST" && pathname.endsWith("/recap")
}

async function handleApiBridgeRequest(fileName) {
  const match = fileName.match(/^([0-9a-f-]{36})\.request\.json$/)
  if (!match || !bridgeDirectory) return
  const requestId = match[1]
  const requestPath = path.join(bridgeDirectory, fileName)
  const responsePath = path.join(bridgeDirectory, `${requestId}.response.json`)
  const temporaryPath = `${responsePath}.${process.pid}.tmp`
  let result
  try {
    const stat = fs.lstatSync(requestPath)
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 64 * 1024) throw new Error("invalid or oversized bridge request file")
    const input = JSON.parse(fs.readFileSync(requestPath, "utf8"))
    fs.unlinkSync(requestPath)
    if (!input || input.id !== requestId || typeof input.method !== "string" || typeof input.route !== "string") {
      throw new Error("invalid bridge request envelope")
    }
    const url = new URL(input.route, "http://bridge.invalid")
    if (!input.route.startsWith("/") || url.origin !== "http://bridge.invalid" ||
        !routeAllowed(input.method, url.pathname) ||
        [...url.searchParams.keys()].some((key) => key !== "directory") ||
        url.searchParams.get("directory") !== projectDirectory || input.directory !== projectDirectory) {
      throw new Error("bridge request is outside the route/directory allowlist")
    }
    if (input.body !== null && typeof input.body !== "string") throw new Error("bridge body must be a JSON string or null")
    const body = input.body === null ? undefined : Buffer.from(input.body)
    if (body && body.length > 64 * 1024) throw new Error("bridge request body exceeds 64 KiB")
    const port = Number(process.env.SILICOGEN_ACCEPTANCE_API_PORT)
    const upstream = await fetch(`http://127.0.0.1:${port}${url.pathname}${url.search}`, {
      method: input.method,
      headers: { "x-silicogen-directory": projectDirectory, ...(body ? { "content-type": "application/json" } : {}) },
      ...(body ? { body } : {}),
    })
    const chunks = []
    let size = 0
    for await (const chunk of upstream.body) {
      size += chunk.length
      if (size > 2 * 1024 * 1024) throw new Error("upstream response exceeds 2 MiB bridge limit")
      chunks.push(chunk)
    }
    result = { id: requestId, status: upstream.status, text: Buffer.concat(chunks, size).toString("utf8") }
  } catch (error) {
    result = { id: requestId, status: 502, text: JSON.stringify({ error: String(error?.message || error) }) }
  }
  fs.writeFileSync(temporaryPath, `${JSON.stringify(result)}\n`, { flag: "wx", mode: 0o600 })
  fs.renameSync(temporaryPath, responsePath)
}

async function pollApiBridge() {
  if (bridgeBusy || !bridgeDirectory) return
  bridgeBusy = true
  try {
    for (const name of fs.readdirSync(bridgeDirectory).filter((item) => item.endsWith(".request.json")).sort()) {
      await handleApiBridgeRequest(name)
    }
  } finally {
    bridgeBusy = false
  }
}

async function startApiBridge() {
  const portText = process.env.SILICOGEN_ACCEPTANCE_API_PORT
  if (!portText) return
  const port = Number(portText)
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("invalid acceptance API port")

  bridgeDirectory = path.join(projectDirectory, ".runtime", "acceptance-api")
  if (!within(path.resolve(projectDirectory), path.resolve(bridgeDirectory))) throw new Error("acceptance bridge escaped the copied workspace")
  fs.mkdirSync(bridgeDirectory, { recursive: true, mode: 0o700 })
  fs.writeFileSync(path.join(bridgeDirectory, "ready"), "file-queue-v1\n", { mode: 0o600 })
  bridgeTimer = setInterval(() => { void pollApiBridge() }, 20)
  bridgeTimer.unref()
}

async function shutdown(signal) {
  if (shuttingDown) return shuttingDown
  shuttingDown = (async () => {
    if (providerReportTimer) clearInterval(providerReportTimer)
    if (bridgeTimer) clearInterval(bridgeTimer)
    if (provider) await provider.close()
    process.exit(signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : signal === "SIGHUP" ? 129 : 0)
  })()
  return shuttingDown
}

await startAcceptanceProvider()
await startApiBridge()
process.once("SIGINT", () => { void shutdown("SIGINT") })
process.once("SIGTERM", () => { void shutdown("SIGTERM") })
process.once("SIGHUP", () => { void shutdown("SIGHUP") })

const [entry, ...args] = remaining
if (!entry) throw new Error("missing Silicogen entry point")
process.argv = [process.execPath, entry, ...args]
await import(entry)
