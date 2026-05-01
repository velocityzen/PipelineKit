import FP

/// Replace each `.failure` with the `Result` produced by the closure. The output
/// failure type may differ from the input — yield `.success` to recover, `.failure`
/// to re-fail with a (possibly different) error. Internally delegates to fp-swift's
/// `Result.orElse`.
struct FlatMapErrorStage<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipelineFlatErrorStage {
    private let transform: @Sendable (InputFailure) -> Result<Value, OutputFailure>

    init(_ transform: @escaping @Sendable (InputFailure) -> Result<Value, OutputFailure>) {
        self.transform = transform
    }

    func attach(_ upstream: Pipeline<Value, InputFailure>) -> Pipeline<Value, OutputFailure> {
        let transform = self.transform
        return .erased {
            AnyAsyncSequence(
                upstream.upstream().map {
                    (element: Result<Value, InputFailure>) -> Result<Value, OutputFailure> in
                    element.orElse(transform)
                },
            )
        }
    }
}

/// DSL: `FlatMapError { (e: NetError) -> Result<Item, AppError> in … }`.
public func FlatMapError<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable
>(
    _ transform: @escaping @Sendable (InputFailure) -> Result<Value, OutputFailure>,
) -> some PipelineFlatErrorStage<Value, InputFailure, OutputFailure> {
    FlatMapErrorStage(transform)
}
