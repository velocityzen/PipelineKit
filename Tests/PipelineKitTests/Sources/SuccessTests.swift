@testable import PipelineKit
import Testing

@Test
func successEmitsOneSuccess() async {
    let result = await Pipeline<Int, Never> { Success(7) }.toResult()
    #expect(result == .success([7]))
}

@Test
func ofIsAliasForSuccess() async {
    let result = await Pipeline<Int, Never> { Of(42) }.toResult()
    #expect(result == .success([42]))
}
