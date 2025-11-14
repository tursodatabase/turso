pub fn normalize_ident(identifier: &str) -> String {
    // quotes normalization already happened in the parser layer (see Name ast node implementation)
    // so, we only need to convert identifier string to lowercase
    identifier.to_lowercase()
}
