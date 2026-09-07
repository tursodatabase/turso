#![feature(test)]

extern crate fnv;
extern crate tantivy_fst;
#[macro_use]
extern crate lazy_static;
extern crate test;

const STR_WORDS: &'static str = include_str!("./../data/words-100000");
const STR_WIKI_URLS: &'static str = include_str!("./../data/wiki-urls-100000");

fn get_keys(s: &'static str) -> Vec<String> {
    s.lines().map(str::to_owned).collect()
}

lazy_static! {
    static ref WORDS: Vec<String> = get_keys(STR_WORDS);
    static ref WIKI_URLS: Vec<String> = get_keys(STR_WIKI_URLS);
}

macro_rules! search {
    ($name:ident, $keys:expr) => {
        mod $name {
            use std::collections::{BTreeSet, HashSet};
            use std::hash::BuildHasherDefault;

            use fnv::FnvHasher;
            use tantivy_fst::raw::{Builder, Fst};
            use test::Bencher;

            #[bench]
            fn fst_contains(b: &mut Bencher) {
                lazy_static! {
                    static ref FST: Fst = {
                        let mut bfst = Builder::memory();
                        for word in $keys.iter() {
                            bfst.add(word).unwrap();
                        }
                        let bytes = bfst.into_inner().unwrap();
                        Fst::new(bytes).unwrap()
                    };
                }
                let mut i = 0;
                b.iter(|| {
                    i = (i + 1) % $keys.len();
                    assert!(FST.contains_key(&$keys[i]));
                })
            }

            #[bench]
            fn fst_streams(b: &mut Bencher) {
                use tantivy_fst::{IntoStreamer, Streamer};
                lazy_static! {
                    static ref FST: Fst = {
                        let mut bfst = Builder::memory();
                        for word in $keys.iter() {
                            bfst.add(word).unwrap();
                        }
                        let bytes = bfst.into_inner().unwrap();
                        Fst::new(bytes).unwrap()
                    };
                }
                b.iter(|| {
                    let start = 1000;
                    let stop = 2000;
                    let mut stream = FST.range().ge(&$keys[start]).lt(&$keys[stop]).into_stream();
                    let mut count = 0;
                    while stream.next().is_some() {
                        count += 1;
                    }
                    assert_eq!(count, stop - start);
                })
            }

            #[bench]
            fn hash_fnv_contains(b: &mut Bencher) {
                type Fnv = BuildHasherDefault<FnvHasher>;
                lazy_static! {
                    static ref SET: HashSet<String, Fnv> = { $keys.clone().into_iter().collect() };
                }
                let mut i = 0;
                b.iter(|| {
                    i = (i + 1) % $keys.len();
                    assert!(SET.contains(&$keys[i]));
                })
            }

            #[bench]
            fn hash_sip_contains(b: &mut Bencher) {
                lazy_static! {
                    static ref SET: HashSet<String> = { $keys.clone().into_iter().collect() };
                }
                let mut i = 0;
                b.iter(|| {
                    i = (i + 1) % $keys.len();
                    assert!(SET.contains(&$keys[i]));
                })
            }

            #[bench]
            fn btree_contains(b: &mut Bencher) {
                lazy_static! {
                    static ref SET: BTreeSet<String> = { $keys.clone().into_iter().collect() };
                }
                let mut i = 0;
                b.iter(|| {
                    i = (i + 1) % $keys.len();
                    assert!(SET.contains(&$keys[i]));
                })
            }
        }
    };
}

search!(words, crate::WORDS);
search!(wiki_urls, crate::WIKI_URLS);
