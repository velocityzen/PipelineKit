/// A node that seeds a pipeline with values.
public protocol PipelineSource<Output, Failure>: Sendable {
    associatedtype Output: Sendable
    associatedtype Failure: Error & Sendable

    func produce() -> Pipeline<Output, Failure>
}
