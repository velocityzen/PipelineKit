import FP

/// Transforms the failure channel from one error type to another. Successes pass through.
struct MapErrorStage<
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipelinePolyValueStage {
    private let transform: @Sendable (InputFailure) -> OutputFailure

    init(_ transform: @escaping @Sendable (InputFailure) -> OutputFailure) {
        self.transform = transform
    }

    func attach<V: Sendable>(_ upstream: Pipeline<V, InputFailure>) -> Pipeline<V, OutputFailure> {
        let transform = self.transform
        return .erased { AnyAsyncSequence(upstream.upstream().mapFailure(transform)) }
    }
}

/// DSL: `MapError { (e: NetworkError) in AppError.network(e) }`.
public func MapError<InputFailure: Error & Sendable, OutputFailure: Error & Sendable>(
    _ transform: @escaping @Sendable (InputFailure) -> OutputFailure,
) -> some PipelinePolyValueStage<InputFailure, OutputFailure> {
    MapErrorStage(transform)
}
