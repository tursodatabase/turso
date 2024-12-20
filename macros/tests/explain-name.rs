use limbo_macros::Explain;

#[derive(Explain)]
pub enum Insn {
    Foo,
}

fn main() {
    let op = Insn::Foo;
    assert_eq!(op.explain_name(), "Foo");
}
