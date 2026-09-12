use super::*;

#[test]
fn key_change_check_treats_nulls_as_equal() {
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 2));
    let skip = program.allocate_label();
    let changed = program.allocate_label();

    emit_key_change_check(&mut program, 1, 2, 1, skip, changed);

    assert!(
        program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Eq {
                lhs: 1,
                rhs: 2,
                flags,
                ..
            } if flags.has_nulleq()
        )),
        "FK UPDATE action guard must use IS semantics for OLD/NEW key comparison"
    );
}
