@testable import PipelineKit
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// MARK: - Outer-task cancellation propagates into in-flight transforms
//
// Pattern: the closure increments `started` *before* the cooperative `Task.sleep`, then —
// only if cancellation hasn't fired — increments `completed` after. A working cancellation
// path means the in-flight slot bails between the sleep and the second counter, so
// `completed` stays bounded by the concurrency window plus what was emitted before the
// break. A regression that lets cancellation be ignored would let `completed` climb close
// to the source size.

@Test
func cancellingOuterTaskShortCircuitsAsyncMapConcurrent() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<1_000)
        AsyncMap(concurrency: 4) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return -1 }
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
    _ = await task.value

    // Tight bounds: at most the concurrency window (4) plus one refill per emit (2) ran to
    // completion. A regression that ignored cancellation would push this toward 1000.
    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(totalStarted <= 8, "started \(totalStarted) — pipeline kept priming after cancel")
    #expect(
        totalCompleted <= 6,
        "completed \(totalCompleted) — in-flight tasks didn't bail on cancel"
    )
}

@Test
func cancellingOuterTaskShortCircuitsAsyncMapKeepOrder() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<1_000)
        AsyncMapKeepOrder(concurrency: 4) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return -1 }
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
    _ = await task.value

    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(
        totalStarted <= 8,
        "started \(totalStarted) — keep-order pipeline kept priming after cancel"
    )
    #expect(totalCompleted <= 6, "completed \(totalCompleted) — keep-order in-flight didn't bail")
}

@Test
func cancellingOuterTaskShortCircuitsAsyncFlatMap() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From(0..<1_000)
        AsyncFlatMap(concurrency: 4) { (n: Int) async -> Result<Int, E> in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return .success(-1) }
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

    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(
        totalStarted <= 8,
        "started \(totalStarted) — flat-map pipeline kept priming after cancel"
    )
    #expect(totalCompleted <= 6, "completed \(totalCompleted) — flat-map in-flight didn't bail")
}

// MARK: - Time-bounded cancellation
//
// Stronger guarantee: the consumer never waits longer than the slowest cooperative
// transform that's already in flight. With per-task sleep of 200ms and concurrency=4,
// the consumer should observe two emissions and break in well under 500ms total — even
// with three pending tasks that would otherwise sleep for 200ms each.

@Test
func cancellationDoesNotBlockConsumerOnPendingInFlight() async {
    let pipe = Pipe<Int, Never> {
        From(0..<100)
        AsyncMap(concurrency: 4) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: 200_000_000)
            return n
        }
    }

    let clock = ContinuousClock()
    let elapsed = await clock.measure {
        for await _ in pipe { break }  // bail on the very first element
    }

    // First emit takes ~200ms. After that we should return immediately, well under the
    // 800ms it would take if we had to wait for the other three pending sleeps.
    #expect(
        elapsed < .milliseconds(500),
        "consumer waited \(elapsed) — cancellation didn't unblock"
    )
}

// MARK: - Iterator deinit teardown

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
    #expect(seen == 5)
    // The sequential path uses fp-swift's element-by-element mapAsync. After break, the
    // iterator deinits and no further elements are produced. We allow a small overshoot
    // for buffering but we should be nowhere near 1000.
    let total = totalProduced.withLock { $0 }
    #expect(total <= 6, "produced \(total) — sequential pipe kept producing after break")
}
