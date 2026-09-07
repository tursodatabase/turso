use super::Error;
use super::Inst;
use regex_syntax::hir::{Class, ClassUnicode, ClassUnicodeRange};
use regex_syntax::hir::{Hir, HirKind};
use utf8_ranges::{Utf8Sequence, Utf8Sequences};

pub struct Compiler {
    size_limit: usize,
    insts: Vec<Inst>,
}

impl Compiler {
    pub fn new(size_limit: usize) -> Compiler {
        Compiler {
            size_limit,
            insts: vec![],
        }
    }

    pub fn compile(mut self, hir: &Hir) -> Result<Vec<Inst>, Error> {
        self.c(hir)?;
        self.insts.push(Inst::Match);
        Ok(self.insts)
    }

    fn c(&mut self, hir: &Hir) -> Result<(), Error> {
        match hir.kind() {
            HirKind::Literal(_literal) => {
                for byte in _literal.0.iter().cloned() {
                    self.push(Inst::Range(byte, byte));
                }
            }
            HirKind::Class(class) => match class {
                Class::Bytes(_) => return Err(Error::NoBytes),
                Class::Unicode(class_unicode) => self.compile_class(class_unicode)?,
            },
            HirKind::Empty => {}
            HirKind::Capture(capture) => self.c(&capture.sub)?,
            HirKind::Concat(hirs) => {
                for hir in hirs {
                    self.c(hir)?;
                }
            }
            HirKind::Alternation(es) => {
                if es.is_empty() {
                    return Ok(());
                }
                let mut jmps_to_end = vec![];
                for e in &es[0..es.len() - 1] {
                    let split = self.push_empty_split();
                    let j1 = self.insts.len();
                    self.c(e)?;
                    jmps_to_end.push(self.push_empty_jump());
                    let j2 = self.insts.len();
                    self.set_split(split, j1, j2);
                }
                self.c(&es[es.len() - 1])?;
                let end = self.insts.len();
                for jmp_to_end in jmps_to_end {
                    self.set_jump(jmp_to_end, end);
                }
            }
            HirKind::Repetition(repetition) => {
                if repetition.greedy == false {
                    return Err(Error::NoLazy);
                }
                if let Some(max) = repetition.max {
                    // Min / max
                    let min = repetition.min;
                    for _ in 0..min {
                        self.c(&repetition.sub)?;
                    }
                    let (mut splits, mut starts) = (vec![], vec![]);
                    for _ in min..max {
                        splits.push(self.push_empty_split());
                        starts.push(self.insts.len());
                        self.c(&repetition.sub)?;
                    }
                    let end = self.insts.len();
                    for (split, start) in splits.into_iter().zip(starts) {
                        self.set_split(split, start, end);
                    }
                } else {
                    // At least
                    for _ in 0..repetition.min {
                        self.c(&repetition.sub)?;
                    }
                    let j1 = self.insts.len();
                    let split = self.push_empty_split();
                    let j2 = self.insts.len();
                    self.c(&repetition.sub)?;
                    let jmp = self.push_empty_jump();
                    let j3 = self.insts.len();

                    self.set_jump(jmp, j1);
                    self.set_split(split, j2, j3);
                }
            }
            HirKind::Look(_) => return Err(Error::NoEmpty),
        }
        self.check_size()
    }

    fn compile_class(&mut self, class: &ClassUnicode) -> Result<(), Error> {
        if class.ranges().is_empty() {
            return Ok(());
        }
        let mut jmps = vec![];
        for &r in &class.ranges()[0..class.ranges().len() - 1] {
            let split = self.push_empty_split();
            let j1 = self.insts.len();
            self.compile_class_range(r)?;
            jmps.push(self.push_empty_jump());
            let j2 = self.insts.len();
            self.set_split(split, j1, j2);
        }
        self.compile_class_range(*class.ranges().last().unwrap())?;
        let end = self.insts.len();
        for jmp in jmps {
            self.set_jump(jmp, end);
        }
        Ok(())
    }

    fn compile_class_range(&mut self, char_range: ClassUnicodeRange) -> Result<(), Error> {
        let mut it = Utf8Sequences::new(char_range.start(), char_range.end()).peekable();
        let mut jmps = vec![];
        let mut utf8_ranges = it.next().expect("non-empty char class");
        while it.peek().is_some() {
            let split = self.push_empty_split();
            let j1 = self.insts.len();
            self.compile_utf8_ranges(&utf8_ranges);
            jmps.push(self.push_empty_jump());
            let j2 = self.insts.len();
            self.set_split(split, j1, j2);
            utf8_ranges = it.next().unwrap(); // because peek says so
        }
        self.compile_utf8_ranges(&utf8_ranges);
        let end = self.insts.len();
        for jmp in jmps {
            self.set_jump(jmp, end);
        }
        Ok(())
    }

    fn compile_utf8_ranges(&mut self, utf8_ranges: &Utf8Sequence) {
        for r in utf8_ranges {
            self.push(Inst::Range(r.start, r.end));
        }
    }

    fn check_size(&self) -> Result<(), Error> {
        use std::mem::size_of;

        if self.insts.len() * size_of::<Inst>() > self.size_limit {
            Err(Error::CompiledTooBig(self.size_limit))
        } else {
            Ok(())
        }
    }

    /// Appends the given instruction to the program.
    #[inline]
    fn push(&mut self, x: Inst) {
        self.insts.push(x)
    }

    /// Appends an *empty* `Split` instruction to the program and returns
    /// the index of that instruction. (The index can then be used to "patch"
    /// the actual locations of the split in later.)
    #[inline]
    fn push_empty_split(&mut self) -> usize {
        self.insts.push(Inst::Split(0, 0));
        self.insts.len() - 1
    }

    /// Sets the left and right locations of a `Split` instruction at index
    /// `i` to `pc1` and `pc2`, respectively.
    /// If the instruction at index `i` isn't a `Split` instruction, then
    /// `panic!` is called.
    #[inline]
    fn set_split(&mut self, i: usize, pc1: usize, pc2: usize) {
        let split = &mut self.insts[i];
        match *split {
            Inst::Split(_, _) => *split = Inst::Split(pc1, pc2),
            _ => panic!("BUG: Invalid split index."),
        }
    }

    /// Appends an *empty* `Jump` instruction to the program and returns the
    /// index of that instruction.
    #[inline]
    fn push_empty_jump(&mut self) -> usize {
        self.insts.push(Inst::Jump(0));
        self.insts.len() - 1
    }

    /// Sets the location of a `Jump` instruction at index `i` to `pc`.
    /// If the instruction at index `i` isn't a `Jump` instruction, then
    /// `panic!` is called.
    #[inline]
    fn set_jump(&mut self, i: usize, pc: usize) {
        let jmp = &mut self.insts[i];
        match *jmp {
            Inst::Jump(_) => *jmp = Inst::Jump(pc),
            _ => panic!("BUG: Invalid jump index."),
        }
    }
}
