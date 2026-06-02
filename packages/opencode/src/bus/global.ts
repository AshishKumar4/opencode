import { EventEmitter } from "events"
import { Identifier } from "@/id/id"
import { ReplayBuffer, REPLAY_MAX_BYTES, REPLAY_MAX_ITEMS } from "@/util/replay-buffer"

export type GlobalEvent = {
  directory?: string
  project?: string
  workspace?: string
  payload: any
}

class GlobalBusEmitter extends EventEmitter<{
  event: [GlobalEvent]
}> {
  private replayBuffer?: ReplayBuffer<GlobalEvent>

  private replayStore() {
    return (this.replayBuffer ??= new ReplayBuffer<GlobalEvent>({
      maxItems: REPLAY_MAX_ITEMS,
      maxBytes: REPLAY_MAX_BYTES,
      id: (event) => (typeof event.payload?.id === "string" ? event.payload.id : undefined),
    }))
  }

  override emit(eventName: "event", event: GlobalEvent): boolean {
    if (event.payload && typeof event.payload === "object" && !("id" in event.payload)) {
      event.payload.id = event.payload.syncEvent?.id ?? Identifier.create("evt", "ascending")
    }
    this.replayBuffer?.push(event)
    return super.emit(eventName, event)
  }

  replay(id?: string) {
    return this.replayStore().after(id)
  }
}

export const GlobalBus = new GlobalBusEmitter()
