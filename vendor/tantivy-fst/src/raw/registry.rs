use crate::raw::build::BuilderNode;
use crate::raw::{CompiledAddr, NONE_ADDRESS};

#[derive(Debug)]
pub struct Registry {
    table: Vec<RegistryCell>,
    table_size: usize, // number of rows
    mru_size: usize,   // number of columns
}

#[derive(Debug)]
struct RegistryCache<'a> {
    cells: &'a mut [RegistryCell],
}

#[derive(Clone, Debug)]
pub struct RegistryCell {
    addr: CompiledAddr,
    node: BuilderNode,
}

#[derive(Debug)]
pub enum RegistryEntry<'a> {
    Found(CompiledAddr),
    NotFound(&'a mut RegistryCell),
    Rejected,
}

impl Registry {
    #[inline]
    pub fn new(table_size: usize, mru_size: usize) -> Registry {
        let empty_cell = RegistryCell::none();
        let ncells = table_size.checked_mul(mru_size).unwrap();
        Registry {
            table: vec![empty_cell; ncells],
            table_size,
            mru_size,
        }
    }

    #[inline]
    pub fn entry(&mut self, node: &BuilderNode) -> RegistryEntry {
        if self.table.is_empty() {
            return RegistryEntry::Rejected;
        }
        let bucket = self.hash(node);
        let start = self.mru_size * bucket;
        let end = start + self.mru_size;
        RegistryCache {
            cells: &mut self.table[start..end],
        }
        .entry(node)
    }

    #[inline]
    fn hash(&self, node: &BuilderNode) -> usize {
        // Basic FNV-1a hash as described:
        // https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
        //
        // In unscientific experiments, this provides the same compression
        // as `std::hash::SipHasher` but is much much faster.
        const FNV_PRIME: u64 = 1_099_511_628_211;
        let mut h = 14_695_981_039_346_656_037;
        h = (h ^ (node.is_final as u64)).wrapping_mul(FNV_PRIME);
        h = (h ^ node.final_output.value()).wrapping_mul(FNV_PRIME);
        for t in &node.trans {
            h = (h ^ (t.inp as u64)).wrapping_mul(FNV_PRIME);
            h = (h ^ t.out.value()).wrapping_mul(FNV_PRIME);
            h = (h ^ (t.addr as u64)).wrapping_mul(FNV_PRIME);
        }
        (h as usize) % self.table_size
    }
}

impl<'a> RegistryCache<'a> {
    #[inline]
    fn entry(mut self, node: &BuilderNode) -> RegistryEntry<'a> {
        if self.cells.len() == 1 {
            let cell = &mut self.cells[0];
            if !cell.is_none() && &cell.node == node {
                RegistryEntry::Found(cell.addr)
            } else {
                cell.node.clone_from(node);
                RegistryEntry::NotFound(cell)
            }
        } else {
            let find = |c: &RegistryCell| !c.is_none() && &c.node == node;
            if let Some(i) = self.cells.iter().position(find) {
                let addr = self.cells[i].addr;
                self.promote(i); // most recently used
                RegistryEntry::Found(addr)
            } else {
                let last = self.cells.len() - 1;
                self.cells[last].node.clone_from(node); // discard LRU
                self.promote(last);
                RegistryEntry::NotFound(&mut self.cells[0])
            }
        }
    }

    #[inline]
    fn promote(&mut self, mut i: usize) {
        assert!(i < self.cells.len());
        while i > 0 {
            self.cells.swap(i - 1, i);
            i -= 1;
        }
    }
}

impl RegistryCell {
    fn none() -> RegistryCell {
        RegistryCell {
            addr: NONE_ADDRESS,
            node: BuilderNode::default(),
        }
    }

    fn is_none(&self) -> bool {
        self.addr == NONE_ADDRESS
    }

    pub fn insert(&mut self, addr: CompiledAddr) {
        self.addr = addr;
    }
}

#[cfg(test)]
mod tests {
    use super::{Registry, RegistryCache, RegistryCell, RegistryEntry};
    use crate::raw::build::BuilderNode;
    use crate::raw::{Output, Transition};

    fn assert_rejected(entry: RegistryEntry) {
        match entry {
            RegistryEntry::Rejected => {}
            entry => panic!("expected rejected entry, got: {:?}", entry),
        }
    }

    fn assert_not_found(entry: RegistryEntry) {
        match entry {
            RegistryEntry::NotFound(_) => {}
            entry => panic!("expected nout found entry, got: {:?}", entry),
        }
    }

    fn assert_insert_and_found(reg: &mut Registry, bnode: &BuilderNode) {
        match reg.entry(&bnode) {
            RegistryEntry::NotFound(cell) => cell.insert(1234),
            entry => panic!("unexpected not found entry, got: {:?}", entry),
        }
        match reg.entry(&bnode) {
            RegistryEntry::Found(addr) => assert_eq!(addr, 1234),
            entry => panic!("unexpected found entry, got: {:?}", entry),
        }
    }

    #[test]
    fn empty_is_ok() {
        let mut reg = Registry::new(0, 0);
        let bnode = BuilderNode {
            is_final: false,
            final_output: Output::zero(),
            trans: vec![],
        };
        assert_rejected(reg.entry(&bnode));
    }

    #[test]
    fn one_final_is_ok() {
        let mut reg = Registry::new(1, 1);
        let bnode = BuilderNode {
            is_final: true,
            final_output: Output::zero(),
            trans: vec![],
        };
        assert_insert_and_found(&mut reg, &bnode);
    }

    #[test]
    fn one_with_trans_is_ok() {
        let mut reg = Registry::new(1, 1);
        let bnode = BuilderNode {
            is_final: false,
            final_output: Output::zero(),
            trans: vec![Transition {
                addr: 0,
                inp: b'a',
                out: Output::zero(),
            }],
        };
        assert_insert_and_found(&mut reg, &bnode);
        assert_not_found(reg.entry(&BuilderNode {
            is_final: true,
            ..bnode.clone()
        }));
        assert_not_found(reg.entry(&BuilderNode {
            trans: vec![Transition {
                addr: 0,
                inp: b'b',
                out: Output::zero(),
            }],
            ..bnode.clone()
        }));
        assert_not_found(reg.entry(&BuilderNode {
            trans: vec![Transition {
                addr: 0,
                inp: b'a',
                out: Output::new(1),
            }],
            ..bnode.clone()
        }));
    }

    #[test]
    fn cache_works() {
        let mut reg = Registry::new(1, 1);

        let bnode1 = BuilderNode {
            is_final: true,
            ..BuilderNode::default()
        };
        assert_insert_and_found(&mut reg, &bnode1);

        let bnode2 = BuilderNode {
            final_output: Output::new(1),
            ..bnode1.clone()
        };
        assert_insert_and_found(&mut reg, &bnode2);
        assert_not_found(reg.entry(&bnode1));
    }

    #[test]
    fn promote() {
        let bn = BuilderNode::default();
        let mut bnodes = vec![
            RegistryCell {
                addr: 1,
                node: bn.clone(),
            },
            RegistryCell {
                addr: 2,
                node: bn.clone(),
            },
            RegistryCell {
                addr: 3,
                node: bn.clone(),
            },
            RegistryCell {
                addr: 4,
                node: bn.clone(),
            },
        ];
        let mut cache = RegistryCache { cells: &mut bnodes };

        cache.promote(0);
        assert_eq!(cache.cells[0].addr, 1);
        assert_eq!(cache.cells[1].addr, 2);
        assert_eq!(cache.cells[2].addr, 3);
        assert_eq!(cache.cells[3].addr, 4);

        cache.promote(1);
        assert_eq!(cache.cells[0].addr, 2);
        assert_eq!(cache.cells[1].addr, 1);
        assert_eq!(cache.cells[2].addr, 3);
        assert_eq!(cache.cells[3].addr, 4);

        cache.promote(3);
        assert_eq!(cache.cells[0].addr, 4);
        assert_eq!(cache.cells[1].addr, 2);
        assert_eq!(cache.cells[2].addr, 1);
        assert_eq!(cache.cells[3].addr, 3);

        cache.promote(2);
        assert_eq!(cache.cells[0].addr, 1);
        assert_eq!(cache.cells[1].addr, 4);
        assert_eq!(cache.cells[2].addr, 2);
        assert_eq!(cache.cells[3].addr, 3);
    }
}
