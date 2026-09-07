use std::collections::{HashMap, HashSet};
use std::fmt;

use super::sparse::SparseSet;
use super::Error;
use super::Inst;

const STATE_LIMIT: usize = 1_000; // currently at least 2MB >_<

pub struct DfaBuilder {
    dfa: Dfa,
    cache: HashMap<Vec<usize>, usize>,
}

pub struct Dfa {
    insts: Vec<Inst>,
    states: Vec<State>,
}

struct State {
    insts: Vec<usize>,
    next: [Option<usize>; 256],
    is_match: bool,
}

impl DfaBuilder {
    pub fn new(insts: Vec<Inst>) -> Self {
        DfaBuilder {
            dfa: Dfa {
                insts,
                states: Vec::with_capacity(16),
            },
            cache: HashMap::with_capacity(1024),
        }
    }

    pub fn build(mut self) -> Result<Dfa, Error> {
        let mut cur = SparseSet::new(self.dfa.insts.len());
        let mut next = SparseSet::new(self.dfa.insts.len());

        self.dfa.add(&mut cur, 0);
        let mut states = vec![self.cached_state(&cur).unwrap()];
        let mut seen = HashSet::new();
        while let Some(s) = states.pop() {
            for b in 0..256 {
                let ns = self.run_state(&mut cur, &mut next, s, b as u8);
                if let Some(ns) = ns {
                    if !seen.contains(&ns) {
                        seen.insert(ns);
                        states.push(ns);
                    }
                }
                if self.dfa.states.len() > STATE_LIMIT {
                    return Err(Error::TooManyStates(STATE_LIMIT));
                }
            }
        }
        Ok(self.dfa)
    }

    fn run_state(
        &mut self,
        cur: &mut SparseSet,
        next: &mut SparseSet,
        state: usize,
        byte: u8,
    ) -> Option<usize> {
        cur.clear();
        for &ip in &self.dfa.states[state].insts {
            cur.add(ip);
        }
        self.dfa.run(cur, next, byte);
        let next_state = self.cached_state(next);
        self.dfa.states[state].next[byte as usize] = next_state;
        next_state
    }

    fn cached_state(&mut self, set: &SparseSet) -> Option<usize> {
        use super::Inst::*;
        use std::collections::hash_map::Entry;

        // There are probably many ways to optimize this routine. ---AG

        let mut insts = vec![];
        let mut is_match = false;
        for i in 0..set.len() {
            let ip = set.get(i);
            match self.dfa.insts[ip] {
                Jump(_) | Split(_, _) => {}
                Range(_, _) => insts.push(ip),
                Match => {
                    is_match = true;
                    insts.push(ip);
                }
            }
        }
        if insts.is_empty() {
            return None;
        }
        Some(match self.cache.entry(insts.clone()) {
            Entry::Occupied(v) => *v.get(),
            Entry::Vacant(v) => {
                self.dfa.states.push(State {
                    insts,
                    next: [None; 256],
                    is_match,
                });
                *v.insert(self.dfa.states.len() - 1)
            }
        })
    }
}

impl Dfa {
    pub fn is_match(&self, si: usize) -> bool {
        self.states[si].is_match
    }

    pub fn accept(&self, si: usize, byte: u8) -> Option<usize> {
        self.states[si].next[byte as usize]
    }

    fn add(&self, set: &mut SparseSet, ip: usize) {
        use super::Inst::*;

        if set.contains(ip) {
            return;
        }
        set.add(ip);
        match self.insts[ip] {
            Match | Range(_, _) => {}
            Jump(ip) => self.add(set, ip),
            Split(ip1, ip2) => {
                self.add(set, ip1);
                self.add(set, ip2);
            }
        }
    }

    fn run(&self, from: &SparseSet, to: &mut SparseSet, byte: u8) -> bool {
        use super::Inst::*;
        to.clear();
        let mut is_match = false;
        for i in 0..from.len() {
            let ip = from.get(i);
            match self.insts[ip] {
                Jump(_) | Split(_, _) => {}
                Match => is_match = true,
                Range(s, e) => {
                    if s <= byte && byte <= e {
                        self.add(to, ip + 1);
                    }
                }
            }
        }
        is_match
    }
}

impl fmt::Debug for Dfa {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, inst) in self.insts.iter().enumerate() {
            writeln!(f, "{:03} {:?}", i, inst)?;
        }
        writeln!(f, "------------")?;
        for (i, state) in self.states.iter().enumerate() {
            if state.is_match {
                writeln!(f, "{:03}* {:?}", i, state.insts)?;
            } else {
                writeln!(f, "{:03}  {:?}", i, state.insts)?;
            }
            for j in 0..256 {
                if let Some(si) = state.next[j] {
                    writeln!(f, "{:03}   {:X} => {}", i, j, si)?;
                }
            }
        }
        Ok(())
    }
}
