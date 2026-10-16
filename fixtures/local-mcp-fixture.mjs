// Dependency-free, display-only MCP server: no network/model calls or secrets.
import readline from "node:readline"
const lines = readline.createInterface({ input: process.stdin })
for await (const line of lines) {
  let request
  try { request = JSON.parse(line) } catch { continue }
  if (request.id === undefined) continue
  let result
  if (request.method === "initialize") result = { protocolVersion: request.params.protocolVersion, capabilities: { tools: {} }, serverInfo: { name: "terminal-display-fixture", version: "1" } }
  else if (request.method === "tools/list") result = { tools: [] }
  else if (request.method === "ping") result = {}
  else {
    console.log(JSON.stringify({ jsonrpc: "2.0", id: request.id, error: { code: -32601, message: "Fixture method not supported" } }))
    continue
  }
  console.log(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }))
}
