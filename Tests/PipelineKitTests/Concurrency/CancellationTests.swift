@testable import PipelineKit
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// MARK: - Outer-task cancellation propagates into in-flight transforms

@Test
func cancellingOuterTaskShortCircuitsAsyncMapConcurrent() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<100)
        AsyncMap(concurrency: 4) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            // Cooperative sleep — observes cancellation and throws CancellationError, which
            // we coerce to a value so the async closure stays non-throwing.
            try? await Task.sleep(nanoseconds: 50_000_000)
            completed.withLock { $0 += 1 }
            return n
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }

    let emitted = await task.value
    #expect(emitted == 2)
    // We let the in-flight batch (up to 4) finish; we should NOT have completed all 100.
    let totalCompleted = completed.withLock { $0 }
    #expect(totalCompleted < 100)
    #expect(totalCompleted >= 2)
}

@Test
func cancellingOuterTaskShortCircuitsAsyncMapKeepOrder() async {
    let started = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<100)
        AsyncMapKeepOrder(concurrency: 4) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 50_000_000)
            return n
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }

    let emitted = await task.value
    #expect(emitted == 2)
    // Sliding window pre-cancels pending tasks on observed cancellation; we shouldn't
    // have started anywhere near all 100.
    let totalStarted = started.withLock { $0 }
    #expect(totalStarted < 100)
}

@Test
func cancellingOuterTaskShortCircuitsAsyncFlatMap() async {
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From(0..<100)
        AsyncFlatMap(concurrency: 4) { (n: Int) async -> Result<Int, E> in
            try? await Task.sleep(nanoseconds: 50_000_000)
            completed.withLock { $0 += 1 }
            return .success(n)
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }

    _ = await task.value
    #expect(completed.withLock { $0 } < 100)
}

@Test
func breakingOutOfForAwaitTearsDownPipe() async {
    let totalProduced = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        Defer { (0..<1_000).lazy }
        AsyncMap(concurrency: 1) { (n: Int) async -> Int in
            totalProduced.withLock { $0 += 1 }
            return n
        }
    }

    var seen = 0
    for await _ in pipe {
        seen += 1
        if seen == 5 { break }
    }
    // After break, the iterator deinits and the underlying Task is cancelled. We may have
    // produced a few extra (in-flight buffering), but nowhere near 1000.
    #expect(seen == 5)
    let total = totalProduced.withLock { $0 }
    #expect(total < 1_000)
}
