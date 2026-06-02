export type ReplayResult<T> = {
  items: T[]
  missed: boolean
}

export const REPLAY_MAX_ITEMS = 512
export const REPLAY_MAX_BYTES = 1_000_000

export class ReplayBuffer<T> {
  private items: Array<{ item: T; bytes: number }> = []
  private bytes = 0

  constructor(private readonly opts: {
    maxItems: number
    maxBytes: number
    id: (item: T) => string | undefined
    size?: (item: T) => number
  }) {}

  push(item: T) {
    if (!this.opts.id(item)) return
    const bytes = this.opts.size?.(item) ?? JSON.stringify(item).length
    if (bytes > this.opts.maxBytes) {
      this.items = []
      this.bytes = 0
      return
    }
    this.items.push({ item, bytes })
    this.bytes += bytes
    while (this.items.length > this.opts.maxItems || this.bytes > this.opts.maxBytes) {
      this.bytes -= this.items.shift()?.bytes ?? 0
    }
  }

  after(id: string | undefined): ReplayResult<T> {
    if (!id || id === "0") return { items: [], missed: false }
    const index = this.items.findIndex((entry) => this.opts.id(entry.item) === id)
    if (index >= 0) return { items: this.items.slice(index + 1).map((entry) => entry.item), missed: false }
    return { items: this.items.map((entry) => entry.item), missed: true }
  }
}
