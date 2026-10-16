import { defineConfig } from "@playwright/test"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const shortTempRoot = path.join(projectRoot, ".runtime/t")
fs.mkdirSync(shortTempRoot, { recursive: true })
const tempDirectoryFd = fs.openSync(shortTempRoot, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY)
process.env.TMPDIR = `/proc/${process.pid}/fd/${tempDirectoryFd}`

export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.mjs",
  fullyParallel: false,
  workers: 1,
  timeout: 30_000,
  expect: { timeout: 5_000 },
  outputDir: path.join(projectRoot, ".runtime/viewer-tests/results"),
  reporter: [
    ["list"],
    ["html", { outputFolder: path.join(projectRoot, ".runtime/viewer-tests/report"), open: "never" }],
  ],
  use: {
    baseURL: "http://127.0.0.1",
    headless: true,
    launchOptions: {
      executablePath: process.env.CHROME_BIN || "/usr/bin/google-chrome",
      args: ["--no-sandbox"],
    },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
})
