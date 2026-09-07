use std::path::PathBuf;
use std::{io, str};

use super::{TermDictionary, TermDictionaryBuilder, TermStreamer};
use crate::directory::{Directory, FileSlice, RamDirectory, TerminatingWrite};
use crate::postings::TermInfo;

const BLOCK_SIZE: usize = 1_500;

fn make_term_info(term_ord: u64) -> TermInfo {
    let offset = |term_ord: u64| (term_ord * 100 + term_ord * term_ord) as usize;
    TermInfo {
        doc_freq: term_ord as u32,
        postings_range: offset(term_ord)..offset(term_ord + 1),
        positions_range: offset(term_ord) * 2..offset(term_ord + 1) * 2,
    }
}

#[test]
fn test_empty_term_dictionary() {
    let empty = TermDictionary::empty();
    assert!(empty.stream().unwrap().next().is_none());
}

#[test]
fn test_term_ordinals() -> crate::Result<()> {
    const COUNTRIES: [&str; 7] = [
        "San Marino",
        "Serbia",
        "Slovakia",
        "Slovenia",
        "Spain",
        "Sweden",
        "Switzerland",
    ];
    let directory = RamDirectory::create();
    let path = PathBuf::from("TermDictionary");
    {
        let write = directory.open_write(&path)?;
        let mut term_dictionary_builder = TermDictionaryBuilder::create(write)?;
        for term in COUNTRIES.iter() {
            term_dictionary_builder.insert(term.as_bytes(), &make_term_info(0u64))?;
        }
        term_dictionary_builder.finish()?.terminate()?;
    }
    let term_file = directory.open_read(&path)?;
    let term_dict: TermDictionary = TermDictionary::open(term_file)?;
    for (term_ord, term) in COUNTRIES.iter().enumerate() {
        assert_eq!(term_dict.term_ord(term)?, Some(term_ord as u64));
        let mut bytes = vec![];
        assert!(term_dict.ord_to_term(term_ord as u64, &mut bytes)?);
        assert_eq!(bytes, term.as_bytes());
    }
    Ok(())
}

#[test]
fn test_term_dictionary_simple() -> crate::Result<()> {
    let directory = RamDirectory::create();
    let path = PathBuf::from("TermDictionary");
    {
        let write = directory.open_write(&path)?;
        let mut term_dictionary_builder = TermDictionaryBuilder::create(write)?;
        term_dictionary_builder.insert("abc".as_bytes(), &make_term_info(34u64))?;
        term_dictionary_builder.insert("abcd".as_bytes(), &make_term_info(346u64))?;
        term_dictionary_builder.finish()?.terminate()?;
    }
    let file = directory.open_read(&path)?;
    let term_dict: TermDictionary = TermDictionary::open(file)?;
    assert_eq!(term_dict.get("abc")?.unwrap().doc_freq, 34u32);
    assert_eq!(term_dict.get("abcd")?.unwrap().doc_freq, 346u32);
    let mut stream = term_dict.stream()?;
    {
        {
            let (k, v) = stream.next().unwrap();
            assert_eq!(k, "abc".as_bytes());
            assert_eq!(v.doc_freq, 34u32);
        }
        assert_eq!(stream.key(), "abc".as_bytes());
        assert_eq!(stream.value().doc_freq, 34u32);
    }
    {
        {
            let (k, v) = stream.next().unwrap();
            assert_eq!(k, "abcd".as_bytes());
            assert_eq!(v.doc_freq, 346u32);
        }
        assert_eq!(stream.key(), "abcd".as_bytes());
        assert_eq!(stream.value().doc_freq, 346u32);
    }
    assert!(!stream.advance());
    Ok(())
}

#[test]
fn test_term_dictionary_stream() -> crate::Result<()> {
    let ids: Vec<_> = (0u32..10_000u32)
        .map(|i| (format!("doc{i:0>6}"), i))
        .collect();
    let buffer: Vec<u8> = {
        let mut term_dictionary_builder = TermDictionaryBuilder::create(vec![]).unwrap();
        for (id, i) in &ids {
            term_dictionary_builder
                .insert(id.as_bytes(), &make_term_info(*i as u64))
                .unwrap();
        }
        term_dictionary_builder.finish()?
    };
    let term_file = FileSlice::from(buffer);
    let term_dictionary: TermDictionary = TermDictionary::open(term_file)?;
    {
        let mut streamer = term_dictionary.stream()?;
        let mut i = 0;
        while let Some((streamer_k, streamer_v)) = streamer.next() {
            let (key, v) = &ids[i];
            assert_eq!(streamer_k, key.as_bytes());
            assert_eq!(streamer_v, &make_term_info(*v as u64));
            i += 1;
        }
    }

    let (key, val) = &ids[2047];
    assert_eq!(
        term_dictionary.get(key.as_bytes())?,
        Some(make_term_info(*val as u64))
    );
    Ok(())
}

#[test]
fn test_stream_high_range_prefix_suffix() -> crate::Result<()> {
    let buffer: Vec<u8> = {
        let mut term_dictionary_builder = TermDictionaryBuilder::create(vec![]).unwrap();
        // term requires more than 16bits
        term_dictionary_builder.insert("abcdefghijklmnopqrstuvwxy", &make_term_info(1))?;
        term_dictionary_builder.insert("abcdefghijklmnopqrstuvwxyz", &make_term_info(2))?;
        term_dictionary_builder.insert("abr", &make_term_info(3))?;
        term_dictionary_builder.finish()?
    };
    let term_dict_file = FileSlice::from(buffer);
    let term_dictionary: TermDictionary = TermDictionary::open(term_dict_file)?;
    let mut kv_stream = term_dictionary.stream()?;
    assert!(kv_stream.advance());
    assert_eq!(kv_stream.key(), "abcdefghijklmnopqrstuvwxy".as_bytes());
    assert_eq!(kv_stream.value(), &make_term_info(1));
    assert!(kv_stream.advance());
    assert_eq!(kv_stream.key(), "abcdefghijklmnopqrstuvwxyz".as_bytes());
    assert_eq!(kv_stream.value(), &make_term_info(2));
    assert!(kv_stream.advance());
    assert_eq!(kv_stream.key(), "abr".as_bytes());
    assert_eq!(kv_stream.value(), &make_term_info(3));
    assert!(!kv_stream.advance());
    Ok(())
}

#[test]
fn test_stream_range() -> crate::Result<()> {
    let ids: Vec<_> = (0u32..10_000u32)
        .map(|i| (format!("doc{i:0>6}"), i))
        .collect();
    let buffer: Vec<u8> = {
        let mut term_dictionary_builder = TermDictionaryBuilder::create(vec![]).unwrap();
        for (id, i) in &ids {
            term_dictionary_builder
                .insert(id.as_bytes(), &make_term_info(*i as u64))
                .unwrap();
        }
        term_dictionary_builder.finish()?
    };

    let file = FileSlice::from(buffer);

    let term_dictionary: TermDictionary = TermDictionary::open(file)?;
    {
        for i in (0..20).chain(6000..8_000) {
            let (target_key, _) = &ids[i];
            let mut streamer = term_dictionary
                .range()
                .ge(target_key.as_bytes())
                .into_stream()?;
            for j in 0..3 {
                let (streamer_k, streamer_v) = streamer.next().unwrap();
                let (key, v) = &ids[i + j];
                assert_eq!(str::from_utf8(streamer_k).unwrap(), key);
                assert_eq!(streamer_v.doc_freq, *v);
                assert_eq!(streamer_v, &make_term_info(*v as u64));
            }
        }
    }

    {
        for i in (0..20).chain(BLOCK_SIZE - 10..BLOCK_SIZE + 10) {
            let (target_key, _) = &ids[i];
            let mut streamer = term_dictionary
                .range()
                .gt(target_key.as_bytes())
                .into_stream()?;
            for j in 0..3 {
                let (streamer_k, streamer_v) = streamer.next().unwrap();
                let (key, v) = &ids[i + j + 1];
                assert_eq!(streamer_k, key.as_bytes());
                assert_eq!(streamer_v.doc_freq, *v);
            }
        }
    }

    {
        for i in (0..20).chain(BLOCK_SIZE - 10..BLOCK_SIZE + 10) {
            for j in 0..3 {
                let (fst_key, _) = &ids[i];
                let (last_key, _) = &ids[i + j];
                let mut streamer = term_dictionary
                    .range()
                    .ge(fst_key.as_bytes())
                    .lt(last_key.as_bytes())
                    .into_stream()?;
                for _ in 0..j {
                    assert!(streamer.next().is_some());
                }
                assert!(streamer.next().is_none());
            }
        }
    }
    Ok(())
}

#[test]
fn test_empty_string() -> crate::Result<()> {
    let buffer: Vec<u8> = {
        let mut term_dictionary_builder = TermDictionaryBuilder::create(vec![]).unwrap();
        term_dictionary_builder
            .insert([], &make_term_info(1_u64))
            .unwrap();
        term_dictionary_builder
            .insert([1u8], &make_term_info(2_u64))
            .unwrap();
        term_dictionary_builder.finish()?
    };
    let file = FileSlice::from(buffer);
    let term_dictionary: TermDictionary = TermDictionary::open(file)?;
    let mut stream = term_dictionary.stream()?;
    assert!(stream.advance());
    assert!(stream.key().is_empty());
    assert!(stream.advance());
    assert_eq!(stream.key(), &[1u8]);
    assert!(!stream.advance());
    Ok(())
}

fn stream_range_test_dict() -> io::Result<TermDictionary> {
    let buffer: Vec<u8> = {
        let mut term_dictionary_builder = TermDictionaryBuilder::create(Vec::new())?;
        for i in 0u8..10u8 {
            let number_arr = [i; 1];
            term_dictionary_builder.insert(number_arr, &make_term_info(i as u64))?;
        }
        term_dictionary_builder.finish()?
    };
    let file = FileSlice::from(buffer);
    TermDictionary::open(file)
}

#[test]
fn test_stream_range_boundaries_forward() -> crate::Result<()> {
    let term_dictionary = stream_range_test_dict()?;
    let value_list = |mut streamer: TermStreamer<'_>| {
        let mut res: Vec<u32> = vec![];
        while let Some((_, v)) = streamer.next() {
            res.push(v.doc_freq);
        }
        res
    };
    {
        let range = term_dictionary.range().ge([2u8]).into_stream()?;
        assert_eq!(
            value_list(range),
            vec![2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
        );
    }
    {
        let range = term_dictionary.range().gt([2u8]).into_stream()?;
        assert_eq!(
            value_list(range),
            vec![3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
        );
    }
    {
        let range = term_dictionary.range().lt([6u8]).into_stream()?;
        assert_eq!(value_list(range), vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32]);
    }
    {
        let range = term_dictionary.range().le([6u8]).into_stream()?;
        assert_eq!(
            value_list(range),
            vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32]
        );
    }
    {
        let range = term_dictionary.range().ge([0u8]).lt([5u8]).into_stream()?;
        assert_eq!(value_list(range), vec![0u32, 1u32, 2u32, 3u32, 4u32]);
    }
    Ok(())
}

#[cfg(not(feature = "quickwit"))]
#[test]
fn test_stream_range_boundaries_backward() -> crate::Result<()> {
    let term_dictionary = stream_range_test_dict()?;
    let value_list_backward = |mut streamer: TermStreamer<'_>| {
        let mut res: Vec<u32> = vec![];
        while let Some((_, v)) = streamer.next() {
            res.push(v.doc_freq);
        }
        res.reverse();
        res
    };
    {
        let range = term_dictionary.range().backward().into_stream()?;
        assert_eq!(
            value_list_backward(range),
            vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
        );
    }
    {
        let range = term_dictionary.range().ge([2u8]).backward().into_stream()?;
        assert_eq!(
            value_list_backward(range),
            vec![2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
        );
    }
    {
        let range = term_dictionary.range().gt([2u8]).backward().into_stream()?;
        assert_eq!(
            value_list_backward(range),
            vec![3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
        );
    }
    {
        let range = term_dictionary.range().lt([6u8]).backward().into_stream()?;
        assert_eq!(
            value_list_backward(range),
            vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32]
        );
    }
    {
        let range = term_dictionary.range().le([6u8]).backward().into_stream()?;
        assert_eq!(
            value_list_backward(range),
            vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32]
        );
    }
    {
        let range = term_dictionary
            .range()
            .ge([0u8])
            .lt([5u8])
            .backward()
            .into_stream()?;
        assert_eq!(
            value_list_backward(range),
            vec![0u32, 1u32, 2u32, 3u32, 4u32]
        );
    }
    Ok(())
}

#[test]
fn test_ord_to_term() -> crate::Result<()> {
    let termdict = stream_range_test_dict()?;
    let mut bytes = vec![];
    for b in 0u8..10u8 {
        termdict.ord_to_term(b as u64, &mut bytes)?;
        assert_eq!(&bytes, &[b]);
    }
    Ok(())
}

#[test]
fn test_stream_term_ord() -> crate::Result<()> {
    let termdict = stream_range_test_dict()?;
    let mut stream = termdict.stream()?;
    for b in 0u8..10u8 {
        assert!(stream.advance());
        assert_eq!(stream.term_ord(), b as u64);
        assert_eq!(stream.key(), &[b]);
    }
    assert!(!stream.advance());
    Ok(())
}

#[test]
fn test_automaton_search() -> crate::Result<()> {
    use levenshtein_automata::LevenshteinAutomatonBuilder;

    use crate::query::DfaWrapper;

    const COUNTRIES: [&str; 7] = [
        "San Marino",
        "Serbia",
        "Slovakia",
        "Slovenia",
        "Spain",
        "Sweden",
        "Switzerland",
    ];

    let directory = RamDirectory::create();
    let path = PathBuf::from("TermDictionary");
    {
        let write = directory.open_write(&path)?;
        let mut term_dictionary_builder = TermDictionaryBuilder::create(write)?;
        for term in COUNTRIES.iter() {
            term_dictionary_builder.insert(term.as_bytes(), &make_term_info(0u64))?;
        }
        term_dictionary_builder.finish()?.terminate()?;
    }
    let file = directory.open_read(&path)?;
    let term_dict: TermDictionary = TermDictionary::open(file)?;

    // We can now build an entire dfa.
    let lev_automaton_builder = LevenshteinAutomatonBuilder::new(2, true);
    let automaton = DfaWrapper(lev_automaton_builder.build_dfa("Spaen"));

    let mut range = term_dict.search(automaton).into_stream()?;

    // get the first finding
    assert!(range.advance());
    assert_eq!("Spain".as_bytes(), range.key());
    assert!(!range.advance());
    Ok(())
}
