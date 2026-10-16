import path from "node:path"
import fs from "node:fs"
import { fileURLToPath, pathToFileURL } from "node:url"

const [runtimeRootArg, serverSessionId, projectDirectoryArg] = process.argv.slice(2)
if (!runtimeRootArg || !serverSessionId || !projectDirectoryArg) {
  throw new Error("usage: seed-silicogen-subagents.ts <runtime-root> <server-session-id> <project-directory>")
}

const runtimeRoot = path.resolve(runtimeRootArg)
const projectDirectory = path.resolve(projectDirectoryArg)
const fixtureDirectory = path.dirname(fileURLToPath(import.meta.url))
const sourceRoot = path.resolve(
  process.env.SILICOGEN_SOURCE_DIR || path.resolve(fixtureDirectory, "../../Silicogen-AI"),
)
const sourceFile = (relativePath: string) => {
  const resolved = path.join(sourceRoot, "packages/silicogen/src", relativePath)
  if (!fs.existsSync(resolved)) {
    throw new Error(`Silicogen source module not found: ${resolved}; set SILICOGEN_SOURCE_DIR to the source checkout`)
  }
  return pathToFileURL(resolved).href
}
const stateRoot = path.join(runtimeRoot, "sessions", serverSessionId, "state")
for (const [variable, child] of [
  ["HOME", "home"],
  ["XDG_DATA_HOME", "data"],
  ["XDG_CONFIG_HOME", "config"],
  ["XDG_CACHE_HOME", "cache"],
  ["XDG_STATE_HOME", "state"],
  ["TMPDIR", "tmp"],
] as const) {
  process.env[variable] = path.join(stateRoot, child)
}
process.chdir(projectDirectory)
process.env.PWD = projectDirectory

const [{ Instance }, { Session }, { AgentControl }, { BackgroundTaskManager }, { TaskGroupManager }, { SubagentEvent }] =
  await Promise.all([
    import(sourceFile("project/instance.ts")),
    import(sourceFile("session/index.ts")),
    import(sourceFile("task/agent-control.ts")),
    import(sourceFile("task/background-task-manager.ts")),
    import(sourceFile("task/task-group.ts")),
    import(sourceFile("task/subagent-event.ts")),
  ])

const model = { providerID: "emulation-fixture", modelID: "display-only" }

const fixture = await Instance.provide({
  directory: projectDirectory,
  fn: async () => {
    const session = await Session.createNext({
      directory: projectDirectory,
      title: "Terminal Runtime Subagents Fixture",
    })
    const group = TaskGroupManager.createOrGet({
      parentSessionId: session.id,
      originMessageId: `emulation-fixture-${Date.now()}`,
      description: "Static UI fixture — no model execution",
      policy: "user_confirmed",
    })
    const runtime = AgentControl.createRuntimeSnapshot({
      directory: projectDirectory,
      defaultDirectory: projectDirectory,
      defaultWorktree: Instance.worktree,
      worktree: Instance.worktree,
      isolation: "none",
      model,
      role: "general",
      tools: {},
    })

    const reviewWorkers = []
    for (const worker of [
      { taskName: "rtl_width_audit", description: "Audit RX byte-index width", result: "Fixture result: width audit complete." },
      { taskName: "questa_flow_audit", description: "Trace Questa regression flow", result: "Fixture result: regression flow documented." },
    ]) {
      const childSession = await Session.createNext({
        directory: projectDirectory,
        parentID: session.id,
        title: `Fixture worker · ${worker.taskName}`,
      })
      const task = BackgroundTaskManager.register({
        sessionId: childSession.id,
        parentSessionId: session.id,
        groupId: group.id,
        description: worker.description,
        name: worker.taskName,
        taskName: worker.taskName,
        agentName: "general",
        model,
        tools: {},
        runtime,
      })
      TaskGroupManager.attachTask(group.id, task.id)
      BackgroundTaskManager.complete(task.id, worker.result)
      TaskGroupManager.recordTerminalEvent({
        groupId: group.id,
        taskId: task.id,
        parentSessionId: session.id,
        kind: "completed",
      })
      reviewWorkers.push({ id: task.id, agentId: task.agentId, name: worker.taskName })
    }

    const waitingSession = await Session.createNext({
      directory: projectDirectory,
      parentID: session.id,
      title: "Fixture worker · clarification_waiter",
    })
    const waiting = BackgroundTaskManager.register({
      sessionId: waitingSession.id,
      parentSessionId: session.id,
      description: "Waiting for a parent decision",
      name: "clarification_waiter",
      taskName: "clarification_waiter",
      agentName: "general",
      model,
      tools: {},
      runtime,
    })
    BackgroundTaskManager.enterWaiting(waiting.id, "input", session.id)

    const review = TaskGroupManager.beginReview({ groupId: group.id, mode: "manual" })
    if (!review.ok) throw new Error(`could not prepare fixture review: ${review.reason}`)
    SubagentEvent.append({
      rootSessionId: session.id,
      agentId: reviewWorkers[0]!.agentId!,
      runId: reviewWorkers[0]!.id,
      groupId: group.id,
      kind: "review_started",
      payload: { mode: "manual", fixture: true },
    })

    return {
      sessionId: session.id,
      groupId: group.id,
      groupStatus: review.group.status,
      workers: [...reviewWorkers, { id: waiting.id, agentId: waiting.agentId, name: "clarification_waiter" }],
      fixtureOnly: true,
      modelCalls: 0,
    }
  },
})

console.log(JSON.stringify(fixture, null, 2))
process.exit(0)
