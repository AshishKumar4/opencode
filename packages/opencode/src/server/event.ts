import { BusEvent } from "@/bus/bus-event"
import { Schema } from "effect"

export const Event = {
  Connected: BusEvent.define("server.connected", Schema.Struct({})),
  Heartbeat: BusEvent.define("server.heartbeat", Schema.Struct({})),
  StreamMissed: BusEvent.define(
    "server.stream.missed",
    Schema.Struct({
      id: Schema.String,
    }),
  ),
  Disposed: BusEvent.define("global.disposed", Schema.Struct({})),
}
