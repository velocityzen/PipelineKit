/// Internal helpers for bounded-concurrency, async element mapping over an
/// `AsyncSequence`. Used by every `Async*` stage when `concurrency > 1`.
///
/// All three helpers share the same skeleton — prime up to N tasks from the source,
/// then refill as results complete. They differ only in emission order:
/// - `mapAsyncUnordered`     — yields each result the moment its task finishes.
/// - `compactMapAsyncUnordered` — same, plus drops `nil` results.
/// - `mapAsyncKeepOrderBounded`  — yields in strict source order, draining the head
///                                 of a sliding window of pending tasks.

func mapAsyncUnordered<Source, T>(
    _ source: Source,
    concurrency: Int,
    _ transform: @escaping @Sendable (Source.Element) async -> T,
) -> AsyncStream<T>
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    AsyncStream<T> { continuation in
        let task = Task {
            await withTaskGroup(of: T.self) { group in
                var iter = source.makeAsyncIterator()

                // Prime up to N tasks.
                for _ in 0..<concurrency {
                    guard let element = try? await iter.next() else { break }
                    group.addTask { await transform(element) }
                }

                // Drain: every completion emits a result and pulls the next source element.
                while let result = await group.next() {
                    continuation.yield(result)
                    if Task.isCancelled { break }
                    if let element = try? await iter.next() {
                        group.addTask { await transform(element) }
                    }
                }
            }
            continuation.finish()
        }
        continuation.onTermination = { _ in task.cancel() }
    }
}

func compactMapAsyncUnordered<Source, T>(
    _ source: Source,
    concurrency: Int,
    _ transform: @escaping @Sendable (Source.Element) async -> T?,
) -> AsyncStream<T>
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    AsyncStream<T> { continuation in
        let task = Task {
            await withTaskGroup(of: T?.self) { group in
                var iter = source.makeAsyncIterator()

                for _ in 0..<concurrency {
                    guard let element = try? await iter.next() else { break }
                    group.addTask { await transform(element) }
                }

                while let result = await group.next() {
                    if let value = result {
                        continuation.yield(value)
                    }
                    if Task.isCancelled { break }
                    if let element = try? await iter.next() {
                        group.addTask { await transform(element) }
                    }
                }
            }
            continuation.finish()
        }
        continuation.onTermination = { _ in task.cancel() }
    }
}

func mapAsyncKeepOrderBounded<Source, T>(
    _ source: Source,
    concurrency: Int,
    _ transform: @escaping @Sendable (Source.Element) async -> T,
) -> AsyncStream<T>
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    AsyncStream<T> { continuation in
        let task = Task {
            var iter = source.makeAsyncIterator()
            var window: [Task<T, Never>] = []

            // Prime the sliding window with up to N tasks.
            for _ in 0..<concurrency {
                guard let element = try? await iter.next() else { break }
                window.append(Task { await transform(element) })
            }

            // Drain head, refill back, preserving source order.
            while !window.isEmpty {
                let head = window.removeFirst()
                continuation.yield(await head.value)
                if Task.isCancelled { break }
                if let element = try? await iter.next() {
                    window.append(Task { await transform(element) })
                }
            }

            for pending in window { pending.cancel() }
            continuation.finish()
        }
        continuation.onTermination = { _ in task.cancel() }
    }
}
