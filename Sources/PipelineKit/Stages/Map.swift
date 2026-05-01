import FP

/// Maps the success channel via a pure function. Failures pass through unchanged.
///
/// `Map` is failure-polymorphic — it works with any failure type the upstream defines.
struct MapStage<Input: Sendable, Output: Sendable>: PipelinePolyStage {
    private let transform: @Sendable (Input) -> Output

    init(_ transform: @escaping @Sendable (Input) -> Output) {
        self.transform = transform
    }

    func attach<F: Error & Sendable>(_ upstream: Pipeline<Input, F>) -> Pipeline<Output, F> {
        let transform = self.transform
        return .erased { AnyAsyncSequence(upstream.upstream().map(transform)) }
    }
}

/// DSL: `Map { $0 + 1 }`.
public func Map<Input: Sendable, Output: Sendable>(
    _ transform: @escaping @Sendable (Input) -> Output,
) -> some PipelinePolyStage<Input, Output> {
    MapStage(transform)
}
