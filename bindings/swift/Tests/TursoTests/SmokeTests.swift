import Testing
@testable import Turso

@Test func versionIsSemver() {
    let version = TursoRuntime.version
    #expect(!version.isEmpty)
    #expect(version.split(separator: ".").count >= 3)
}
