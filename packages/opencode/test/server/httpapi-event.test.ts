import { afterEach, describe, expect, test } from "bun:test"
import { Server } from "../../src/server/server"
import { EventPaths } from "../../src/server/routes/instance/httpapi/event"
import * as Log from "@opencode-ai/core/util/log"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, provideTestInstance, tmpdir } from "../fixture/fixture"
import { Bus } from "../../src/bus"
import { Event } from "../../src/server/event"
import { GlobalBus } from "../../src/bus/global"
import { Effect } from "effect"
import { AppRuntime } from "../../src/effect/app-runtime"

void Log.init({ print: false })

const EVENT_READ_TIMEOUT_MS = 5_000
const CONNECTED = "server.connected"
const HEARTBEAT = "server.heartbeat"
const MISSED = "server.stream.missed"

function app() {
  return Server.Default().app
}

async function readFirstChunk(response: Response) {
  if (!response.body) throw new Error("missing response body")
  const reader = response.body.getReader()
  const result = await Promise.race([
    reader.read(),
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timed out waiting for event")), EVENT_READ_TIMEOUT_MS)),
  ])
  await reader.cancel()
  return new TextDecoder().decode(result.value)
}

async function readFirstEvent(response: Response) {
  return JSON.parse((await readFirstChunk(response)).replace(/^data: /, "")) as {
    id?: string
    type: string
    properties: Record<string, unknown>
  }
}

async function readEvents(response: Response, count: number) {
  if (!response.body) throw new Error("missing response body")
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  const events: Array<{
    id?: string
    data: {
      id?: string
      type?: string
      properties?: Record<string, unknown>
      payload?: { id?: string; type?: string; properties?: Record<string, unknown> }
    }
  }> = []
  let buf = ""
  while (events.length < count) {
    const result = await Promise.race([
      reader.read(),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timed out waiting for event")), EVENT_READ_TIMEOUT_MS)),
    ])
    if (result.done) break
    buf += decoder.decode(result.value)
    while (events.length < count && buf.includes("\n\n")) {
      const index = buf.indexOf("\n\n")
      const raw = buf.slice(0, index)
      buf = buf.slice(index + 2)
      const lines = raw.split("\n")
      const id = lines.find((line) => line.startsWith("id: "))?.slice(4)
      const data = lines.find((line) => line.startsWith("data: "))?.slice(6)
      if (data) events.push({ id, data: JSON.parse(data) })
    }
  }
  await reader.cancel()
  return events
}

async function replayInstance(dir: string, last = "evt_test_instance_1") {
  return provideTestInstance({
    directory: dir,
    fn: async () => {
      await readFirstEvent(await app().request(EventPaths.event, { headers: { "x-opencode-directory": dir } }))
      await AppRuntime.runPromise(
        Bus.Service.use((bus) =>
          Effect.gen(function* () {
            yield* bus.publish(Event.Heartbeat, {}, { id: "evt_test_instance_1" })
            yield* bus.publish(Event.Heartbeat, {}, { id: "evt_test_instance_2" })
          }),
        ),
      )
      return readEvents(
        await app().request(EventPaths.event, {
          headers: { "x-opencode-directory": dir, "Last-Event-ID": last },
        }),
        2,
      )
    },
  })
}

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("event HttpApi", () => {
  test("serves event stream", async () => {
    await using tmp = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const response = await app().request(EventPaths.event, { headers: { "x-opencode-directory": tmp.path } })

    expect(response.status).toBe(200)
    expect(response.headers.get("content-type")).toContain("text/event-stream")
    expect(response.headers.get("cache-control")).toBe("no-cache, no-transform")
    expect(response.headers.get("x-accel-buffering")).toBe("no")
    expect(response.headers.get("x-content-type-options")).toBe("nosniff")
    expect(await readFirstEvent(response)).toMatchObject({ type: CONNECTED, properties: {} })
  })

  test("serves the initial server connected event", async () => {
    await using tmp = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const headers = { "x-opencode-directory": tmp.path }
    const response = await app().request(EventPaths.event, { headers })

    expect(await readFirstEvent(response)).toMatchObject({ type: CONNECTED, properties: {} })
  })

  test("replays instance events after Last-Event-ID", async () => {
    await using tmp = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const events = await replayInstance(tmp.path)

    expect(events[0].data.type).toBe(CONNECTED)
    expect(events[0].id).toBeUndefined()
    expect(events[1].id).toBe("evt_test_instance_2")
    expect(events[1].data.type).toBe(HEARTBEAT)
  })

  test("reports missed instance replay cursors", async () => {
    await using tmp = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const events = await replayInstance(tmp.path, "evt_missing_instance")

    expect(events[0].data.type).toBe(CONNECTED)
    expect(events[1].data.type).toBe(MISSED)
    expect(events[1].data.properties).toEqual({ id: "evt_missing_instance" })
  })

  test("replays global events after Last-Event-ID", async () => {
    await readFirstEvent(await app().request("/global/event"))
    GlobalBus.emit("event", {
      payload: { id: "evt_test_global_1", type: Event.Heartbeat.type, properties: {} },
    })
    GlobalBus.emit("event", {
      payload: { id: "evt_test_global_2", type: Event.Heartbeat.type, properties: {} },
    })

    const response = await app().request("/global/event", { headers: { "Last-Event-ID": "evt_test_global_1" } })
    const events = await readEvents(response, 2)

    expect(events[0].data.payload?.type).toBe(CONNECTED)
    expect(events[0].id).toBeUndefined()
    expect(events[1].id).toBe("evt_test_global_2")
    expect(events[1].data.payload?.type).toBe(HEARTBEAT)
  })

  test("reports missed global replay cursors", async () => {
    await readFirstEvent(await app().request("/global/event"))
    GlobalBus.emit("event", {
      payload: { id: "evt_test_global_missed", type: Event.Heartbeat.type, properties: {} },
    })

    const response = await app().request("/global/event", { headers: { "Last-Event-ID": "evt_missing_global" } })
    const events = await readEvents(response, 2)

    expect(events[0].data.payload?.type).toBe(CONNECTED)
    expect(events[1].data.payload?.type).toBe(MISSED)
    expect(events[1].data.payload?.properties).toEqual({ id: "evt_missing_global" })
  })
})
