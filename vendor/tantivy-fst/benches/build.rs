#![feature(test)]

extern crate tantivy_fst;
extern crate test;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use tantivy_fst::raw::{Builder, Fst};
use test::Bencher;

const WORDS: &'static str = include_str!("./../data/words-10000");

fn get_words() -> Vec<String> {
    WORDS.lines().map(|s| s.to_owned()).collect()
}

fn get_words_outputs() -> Vec<(String, u64)> {
    WORDS
        .lines()
        .map(|s| (s.to_owned(), s.len() as u64))
        .collect()
}

#[bench]
fn build_fst_set(b: &mut Bencher) {
    let words = get_words();
    b.bytes = WORDS.len() as u64;
    b.iter(|| {
        let mut bfst = Builder::memory();
        for word in &words {
            bfst.add(word).unwrap();
        }
        Fst::new(bfst.into_inner().unwrap()).unwrap();
    });
}

#[bench]
fn build_fst_map(b: &mut Bencher) {
    let words = get_words_outputs();
    b.bytes = WORDS.len() as u64;
    b.iter(|| {
        let mut bfst = Builder::memory();
        for &(ref word, len) in &words {
            bfst.insert(word, len).unwrap();
        }
        Fst::new(bfst.into_inner().unwrap()).unwrap();
    });
}

#[bench]
fn build_hash_set(b: &mut Bencher) {
    let words = get_words();
    b.bytes = WORDS.len() as u64;
    b.iter(|| {
        let mut set = HashSet::new();
        for word in &words {
            set.insert(word);
        }
    });
}

#[bench]
fn build_hash_map(b: &mut Bencher) {
    let words = get_words_outputs();
    b.bytes = WORDS.len() as u64;
    b.iter(|| {
        let mut set = HashMap::new();
        for &(ref word, len) in &words {
            set.insert(word, len);
        }
    });
}

#[bench]
fn build_btree_set(b: &mut Bencher) {
    let words = get_words();
    b.bytes = WORDS.len() as u64;
    b.iter(|| {
        let mut set = BTreeSet::new();
        for word in &words {
            set.insert(word);
        }
    });
}

#[bench]
fn build_btree_map(b: &mut Bencher) {
    let words = get_words_outputs();
    b.bytes = WORDS.len() as u64;
    b.iter(|| {
        let mut set = BTreeMap::new();
        for &(ref word, len) in &words {
            set.insert(word, len);
        }
    });
}
