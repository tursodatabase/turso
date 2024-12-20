use limbo_macros::Explain;

#[derive(Explain)]
pub enum Insn {
    Foo {
        #[p1]
        val1: usize,
        #[p3]
        val2: usize,
    },
}

fn main() {
    let op = Insn::Foo { val1: 5, val2: 7 };

    assert_eq!(op.explain_p1(), 5);
    assert_eq!(op.explain_p2(), 0);
    assert_eq!(op.explain_p3(), 7);
}
