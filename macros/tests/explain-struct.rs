use limbo_macros::Explain;

#[derive(Explain)]
pub enum Insn {
    Foo,
    #[desc = "A description of Bar."]
    Bar,
}

fn main() {
    assert_eq!(
        Insn::Foo.explain_desc(),
        "Missing desc attribute for variant"
    );
    assert_eq!(Insn::Bar.explain_desc(), "A description of Bar.");
}
