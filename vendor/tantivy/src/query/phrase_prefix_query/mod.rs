mod phrase_prefix_query;
mod phrase_prefix_scorer;
mod phrase_prefix_weight;

pub use phrase_prefix_query::PhrasePrefixQuery;
pub use phrase_prefix_scorer::PhrasePrefixScorer;
pub use phrase_prefix_weight::PhrasePrefixWeight;

pub(crate) fn prefix_end(prefix_start: &[u8]) -> Option<Vec<u8>> {
    let mut res = prefix_start.to_owned();
    while !res.is_empty() {
        let end = res.len() - 1;
        if res[end] == u8::MAX {
            res.pop();
        } else {
            res[end] += 1;
            return Some(res);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_end() {
        assert_eq!(prefix_end(b"aaa"), Some(b"aab".to_vec()));
        assert_eq!(prefix_end(b"aa\xff"), Some(b"ab".to_vec()));
        assert_eq!(prefix_end(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff\xff"), None);
    }
}
