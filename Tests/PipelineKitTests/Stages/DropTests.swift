@testable import PipelineKit
import Testing

@Test
func dropSkipsLeadingElements() async {
    let pipe = Pipeline<Int, Never> {
        From([1, 2, 3, 4, 5])
        Drop(2)
    }
    let result = await pipe.toResult()
    #expect(result == .success([3, 4, 5]))
}
