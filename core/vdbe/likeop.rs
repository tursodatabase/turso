use std::collections::HashMap;

use regex::Regex;

use crate::LimboError;

// Implements GLOB pattern matching. Caches the constructed regex if a cache is provided
pub fn exec_glob(
    regex_cache: Option<&mut HashMap<String, Regex>>,
    pattern: &str,
    text: &str,
) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => match construct_glob_regex(pattern) {
                Ok(re) => {
                    let res = re.is_match(text);
                    cache.insert(pattern.to_string(), re);
                    res
                }
                Err(_) => false,
            },
        }
    } else {
        construct_glob_regex(pattern)
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }
}

fn push_char_to_regex_pattern(c: char, regex_pattern: &mut String) {
    if regex_syntax::is_meta_character(c) {
        regex_pattern.push('\\');
    }
    regex_pattern.push(c);
}

fn construct_glob_regex(pattern: &str) -> Result<Regex, LimboError> {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    let mut chars = pattern.chars();
    let mut bracket_closed = true;

    while let Some(ch) = chars.next() {
        match ch {
            '[' => {
                bracket_closed = false;
                regex_pattern.push('[');
                if let Some(next_ch) = chars.next() {
                    match next_ch {
                        ']' => {
                            // The string  enclosed by the brackets cannot be empty;
                            // therefore ']' can be allowed between the brackets,
                            // provided that it is the first  character.
                            // so this means
                            // - `[]]` will be translated to `[\]]`
                            // - `[[]` will be translated to `[\[]`
                            regex_pattern.push_str("\\]");
                        }
                        '^' => {
                            // For the most cases we can pass `^` directly to regex
                            // but in certain cases like [^][a] , `[^]` will  make regex crate
                            // throw unenclosed character class. So this means
                            // - `[^][a]` will be translated to `[^\]a]`
                            regex_pattern.push('^');
                            if let Some(next_ch_2) = chars.next() {
                                match next_ch_2 {
                                    ']' => {
                                        regex_pattern.push('\\');
                                        regex_pattern.push(']');
                                    }
                                    c => {
                                        push_char_to_regex_pattern(c, &mut regex_pattern);
                                    }
                                }
                            }
                        }
                        c => {
                            push_char_to_regex_pattern(c, &mut regex_pattern);
                        }
                    }
                };

                for next_ch in chars.by_ref() {
                    match next_ch {
                        ']' => {
                            bracket_closed = true;
                            regex_pattern.push(']');
                            break;
                        }
                        '-' => {
                            regex_pattern.push('-');
                        }
                        c => {
                            push_char_to_regex_pattern(c, &mut regex_pattern);
                        }
                    }
                }
            }
            '?' => {
                regex_pattern.push('.');
            }
            '*' => {
                regex_pattern.push_str(".*");
            }
            c => {
                push_char_to_regex_pattern(c, &mut regex_pattern);
            }
        }
    }
    regex_pattern.push('$');

    if bracket_closed {
        Regex::new(&regex_pattern)
            .map_err(|e| LimboError::InternalError(format!("invalid GLOB regex pattern: {e}")))
    } else {
        Err(LimboError::Constraint(
            "blob pattern is not closed".to_string(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_glob_no_cache() {
        assert!(exec_glob(None, r#"?*/abc/?*"#, r#"x//a/ab/abc/y"#));
        assert!(exec_glob(None, r#"a[1^]"#, r#"a1"#));
        assert!(exec_glob(None, r#"a[1^]*"#, r#"a^"#));
        assert!(!exec_glob(None, r#"a[a*"#, r#"a["#));
        assert!(!exec_glob(None, r#"a[a"#, r#"a[a"#));
        assert!(exec_glob(None, r#"a[[]"#, r#"a["#));
        assert!(exec_glob(None, r#"abc[^][*?]efg"#, r#"abcdefg"#));
        assert!(!exec_glob(None, r#"abc[^][*?]efg"#, r#"abc]efg"#));
    }
}
