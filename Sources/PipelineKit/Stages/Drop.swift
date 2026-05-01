/// Drops the first `count` elements of the pipeline (successes and failures alike).
struct DropStage: PipelineForwardingStage {
    private let count: Int

    init(_ count: Int) {
        precondition(count >= 0, "Drop count must be non-negative")
        self.count = count
    }

    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipeline<V, F>) -> Pipeline<V, F> {
        let count = self.count
        return .erased { AnyAsyncSequence(upstream.upstream().dropFirst(count)) }
    }
}

/// DSL: `Drop(2)`.
public func Drop(_ count: Int) -> some PipelineForwardingStage {
    DropStage(count)
}
