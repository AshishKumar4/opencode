import type { Context } from "hono"
import { streamSSE } from "hono/streaming"
import { Cause, Effect, Queue } from "effect"
import * as Stream from "effect/Stream"
import * as Sse from "effect/unstable/encoding/Sse"
import { AsyncQueue } from "@/util/queue"
import type { ReplayResult } from "@/util/replay-buffer"

type Frame = {
  data: string
  id?: string
}

type Spec<T> = {
  id: (item: T) => string | undefined
  replay: (id?: string) => ReplayResult<T>
  missed: (id: string) => unknown
  connected: () => unknown
  heartbeat: () => unknown
  end?: (item: T) => boolean
}

type EffectSpec<T> = Omit<Spec<T>, "replay"> & {
  replay: (id?: string) => Effect.Effect<ReplayResult<T>>
}

const CLIENT_QUEUE_MAX = 1024
const HEARTBEAT_INTERVAL_MS = 10_000

function frame(data: unknown, id?: string): Frame {
  return { data: JSON.stringify(data), id }
}

function event(data: unknown, id?: string): Sse.Event {
  return { _tag: "Event", event: "message", id, data: JSON.stringify(data) }
}

export async function hono<T>(c: Context, spec: Spec<T>, subscribe: (push: (item: T) => void) => () => void) {
  return streamSSE(c, async (stream) => {
    const q = new AsyncQueue<Frame | null>(CLIENT_QUEUE_MAX)
    const live: T[] = []
    const sent = new Set<string>()
    let end = false
    let replaying = true
    let done = false

    const emit = (item: Frame | null) => {
      if (q.push(item)) return
      q.push(null, true)
    }

    const push = (item: T) => {
      const f = frame(item, spec.id(item))
      if (replaying) live.push(item)
      else emit(f)
      if (spec.end?.(item)) end = true
    }

    emit(frame(spec.connected()))

    const heartbeat = setInterval(() => emit(frame(spec.heartbeat())), HEARTBEAT_INTERVAL_MS)
    const stop = () => {
      if (done) return
      done = true
      clearInterval(heartbeat)
      unsub()
      q.push(null, true)
    }
    const unsub = subscribe(push)

    const last = c.req.header("Last-Event-ID")
    const replay = spec.replay(last)
    if (last && replay.missed) emit(frame(spec.missed(last)))
    for (const item of replay.items) {
      const id = spec.id(item)
      if (id) sent.add(id)
      emit(frame(item, id))
    }
    replaying = false
    for (const item of live) {
      const id = spec.id(item)
      if (id && sent.has(id)) continue
      push(item)
    }
    if (end) emit(null)
    sent.clear()

    stream.onAbort(stop)
    try {
      for await (const item of q) {
        if (item === null) return
        await stream.writeSSE(item)
      }
    } finally {
      stop()
    }
  })
}

export function effect<T>(last: string | undefined, spec: EffectSpec<T>, subscribe: (push: (item: T) => void) => Effect.Effect<() => void>) {
  return Stream.unwrap(Effect.gen(function* () {
    const queue = yield* Queue.bounded<Sse.Event, Cause.Done>(CLIENT_QUEUE_MAX)
    const live: T[] = []
    const sent = new Set<string>()
    let replaying = true
    let end = false
    const offer = (item: Sse.Event) => {
      if (Queue.offerUnsafe(queue, item)) return
      Queue.endUnsafe(queue)
    }
    const push = (item: T) => {
      if (replaying) live.push(item)
      else offer(event(item, spec.id(item)))
      if (spec.end?.(item)) end = true
    }
    const unsub = yield* subscribe(push)

    offer(event(spec.connected()))
    const replay = yield* spec.replay(last)
    if (last && replay.missed) offer(event(spec.missed(last)))
    for (const item of replay.items) {
      const id = spec.id(item)
      if (id) sent.add(id)
      offer(event(item, id))
    }
    replaying = false
    for (const item of live) {
      const id = spec.id(item)
      if (id && sent.has(id)) continue
      push(item)
    }
    if (end) Queue.endUnsafe(queue)
    sent.clear()

    const heartbeat = setInterval(() => offer(event(spec.heartbeat())), HEARTBEAT_INTERVAL_MS)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        clearInterval(heartbeat)
        unsub()
      }),
    )
    return Stream.fromQueue(queue)
  }))
}
