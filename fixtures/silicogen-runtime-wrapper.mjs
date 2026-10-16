import fs from "node:fs"
import path from "node:path"
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

const [entry, ...args] = remaining
if (!entry) throw new Error("missing Silicogen entry point")
process.argv = [process.execPath, entry, ...args]
await import(entry)
