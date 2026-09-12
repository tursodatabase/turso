/// `LimboError` rides in the `Result` of every opcode call and every
/// cursor operation, so its size is copied around once per executed
/// instruction. A fat new variant (see the boxed `LexerError`) silently
/// taxes the whole hot path.
#[test]
fn limbo_error_stays_small() {
    assert!(std::mem::size_of::<crate::LimboError>() <= 40);
    // The niche-packed boxed-error result returns in registers; anything
    // past 16 bytes goes back through memory on every executed insn.
    assert!(std::mem::size_of::<super::execute::InsnResult>() <= 16);
}
