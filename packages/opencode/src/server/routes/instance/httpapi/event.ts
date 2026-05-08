import { Bus } from "@/bus"
import * as Log from "@opencode-ai/core/util/log"
import { Effect, Schema } from "effect"
import * as Stream from "effect/Stream"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import * as Sse from "effect/unstable/encoding/Sse"
import { Event } from "@/server/event"
import * as SseStream from "@/server/sse"
import { Instance, type InstanceContext } from "@/project/instance"
import { InstanceRef } from "@/effect/instance-ref"

const log = Log.create({ service: "server" })

export const EventPaths = {
  event: "/event",
} as const

export const EventApi = HttpApi.make("event").add(
  HttpApiGroup.make("event")
    .add(
      HttpApiEndpoint.get("subscribe", EventPaths.event, {
        success: Schema.String.pipe(HttpApiSchema.asText({ contentType: "text/event-stream" })),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "event.subscribe",
          summary: "Subscribe to events",
          description: "Get events",
        }),
      ),
    )
    .annotateMerge(OpenApi.annotations({ title: "event", description: "Instance event stream route." })),
)

function missed(id: string) {
  return {
    id: Bus.createID(),
    type: Event.StreamMissed.type,
    properties: { id },
  }
}

function spec(ctx: InstanceContext) {
  return {
    id: (event: Bus.Payload) => event.id,
    replay: (id?: string) => Effect.sync(() => Instance.restore(ctx, () => Bus.replay(id))),
    missed,
    connected: () => ({ id: Bus.createID(), type: Event.Connected.type, properties: {} }),
    heartbeat: () => ({ id: Bus.createID(), type: Event.Heartbeat.type, properties: {} }),
    end: (event: Bus.Payload) => event.type === Bus.InstanceDisposed.type,
  }
}

function eventResponse(ctx: InstanceContext, last?: string) {
  log.info("event connected")
  return HttpServerResponse.stream(
    SseStream.effect(last, spec(ctx), (push) => Effect.sync(() => Instance.restore(ctx, () => Bus.subscribeAll(push)))).pipe(
      Stream.pipeThroughChannel(Sse.encode()),
      Stream.encodeText,
      Stream.ensuring(Effect.sync(() => log.info("event disconnected"))),
    ),
    {
      contentType: "text/event-stream",
      headers: {
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "X-Content-Type-Options": "nosniff",
      },
    },
  )
}

export const eventHandlers = HttpApiBuilder.group(EventApi, "event", (handlers) =>
  Effect.gen(function* () {
    return handlers.handleRaw(
      "subscribe",
      Effect.fn("EventHttpApi.subscribe")(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const last = request.headers["last-event-id"]
        return eventResponse((yield* InstanceRef) ?? Instance.current, last)
      }),
    )
  }),
)
