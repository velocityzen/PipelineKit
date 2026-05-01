/// Result builder for declaring pipelines as a sequence of a source followed by stages.
///
/// The builder uses `buildPartialBlock` to left-fold heterogeneous steps:
/// the first step must be a `PipelineSource`, and each subsequent step is a
/// `PipelineStage` whose `Input` matches the upstream's `Output`.
@resultBuilder
public enum PipelineBuilder {
    // MARK: - First step (the source)

    public static func buildPartialBlock<S: PipelineSource>(
        first source: S,
    ) -> Pipeline<S.Output, S.Failure> {
        source.produce()
    }

    // MARK: - Subsequent steps (stages)

    public static func buildPartialBlock<U: Sendable, St: PipelineStage>(
        accumulated: Pipeline<U, St.Failure>,
        next stage: St,
    ) -> Pipeline<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<U: Sendable, F: Error & Sendable, St: PipelinePolyStage>(
        accumulated: Pipeline<U, F>,
        next stage: St,
    ) -> Pipeline<St.Output, F> where St.Input == U {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<V: Sendable, St: PipelinePolyValueStage>(
        accumulated: Pipeline<V, St.InputFailure>,
        next stage: St,
    ) -> Pipeline<V, St.OutputFailure> {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<
        V: Sendable,
        F: Error & Sendable,
    >(
        accumulated: Pipeline<V, F>,
        next stage: some PipelineForwardingStage,
    ) -> Pipeline<V, F> {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<St: PipelineFlatErrorStage>(
        accumulated: Pipeline<St.Value, St.InputFailure>,
        next stage: St,
    ) -> Pipeline<St.Value, St.OutputFailure> {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<St: PipelineFoldStage>(
        accumulated: Pipeline<St.Input, St.InputFailure>,
        next stage: St,
    ) -> Pipeline<St.Output, Never> {
        stage.attach(accumulated)
    }

    /// Widening overload: when the upstream cannot fail (`Failure == Never`), allow
    /// attaching a failure-fixed stage by lifting the failure channel into `St.Failure`.
    /// This makes `From([…]) → FlatMap { … }` work without an explicit `MapError` step.
    public static func buildPartialBlock<U: Sendable, St: PipelineStage>(
        accumulated: Pipeline<U, Never>,
        next stage: St,
    ) -> Pipeline<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated.widenFailure(to: St.Failure.self))
    }

    /// Widening overload for value-polymorphic failure-transforming stages (e.g. `MapError`).
    public static func buildPartialBlock<V: Sendable, St: PipelinePolyValueStage>(
        accumulated: Pipeline<V, Never>,
        next stage: St,
    ) -> Pipeline<V, St.OutputFailure> where St.InputFailure == Never {
        stage.attach(accumulated)
    }
}

// MARK: - Pipeline initializer

public extension Pipeline {
    /// Build a pipeline from a sequence of a source and stages.
    init(@PipelineBuilder _ build: () -> Pipeline<Success, Failure>) {
        self = build()
    }
}
