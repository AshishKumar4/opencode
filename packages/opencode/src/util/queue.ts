export class AsyncQueue<T> implements AsyncIterable<T> {
  private queue: T[] = []
  private resolvers: ((value: T) => void)[] = []

  constructor(private readonly max = Infinity) {}

  push(item: T, force = false) {
    const resolve = this.resolvers.shift()
    if (resolve) {
      resolve(item)
      return true
    }
    if (this.queue.length >= this.max) {
      if (!force) return false
      this.queue.shift()
    }
    this.queue.push(item)
    return true
  }

  async next(): Promise<T> {
    if (this.queue.length > 0) return this.queue.shift()!
    return new Promise((resolve) => this.resolvers.push(resolve))
  }

  async *[Symbol.asyncIterator]() {
    while (true) yield await this.next()
  }
}

export async function work<T>(concurrency: number, items: T[], fn: (item: T) => Promise<void>) {
  const pending = [...items]
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const item = pending.pop()
        if (item === undefined) return
        await fn(item)
      }
    }),
  )
}
