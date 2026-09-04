import CTurso

public enum TursoRuntime {
    /// Turso engine version (sem-ver string).
    public static var version: String {
        String(cString: turso_version())
    }
}
