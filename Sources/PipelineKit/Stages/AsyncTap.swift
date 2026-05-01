import FP

/// Asynchronously observes successful values and passes them through unchanged.
/// Failures are not observed by this stage.
struct AsyncTapStage<Value: Sendable>: PipelinePolyStage {
    typealias Input = Value
    typealias Output = Value

    private let action: @Sendable (Value) async -> Void

    init(_ action: @escaping @Sendable (Value) async -> Void) {
        self.action = action
    }

    func attach<F: Error & Sendable>(_ upstream: Pipeline<Value, F>) -> Pipeline<Value, F> {
        let action = self.action
        return .erased { AnyAsyncSequence(upstream.upstream().tapAsync(action)) }
    }
}

/// DSL: `AsyncTap { value in await log(value) }`.
public func AsyncTap<Value: Sendable>(
    _ action: @escaping @Sendable (Value) async -> Void,
) -> some PipelinePolyStage<Value, Value> {
    AsyncTapStage(action)
}
