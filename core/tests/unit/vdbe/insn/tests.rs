#[test]
fn test_insn_size_does_not_grow() {
    // Interpreter dispatch is sensitive to instruction size. Widening a
    // variant past the current largest one silently degrades every query;
    // grow this bound only deliberately.
    assert!(
        std::mem::size_of::<super::Insn>() <= 96,
        "Insn grew to {} bytes",
        std::mem::size_of::<super::Insn>()
    );
}
