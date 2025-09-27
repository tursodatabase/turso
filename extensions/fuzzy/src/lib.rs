// Adapted from sqlean fuzzy
use std::cmp;
use turso_ext::{register_extension, scalar, ResultCode, Value};
mod caver;
mod common;
mod editdist;
mod phonetic;
mod rsoundex;
mod soundex;
mod translit;

register_extension! {
    scalars: {levenshtein, damerau_levenshtein, edit_distance, hamming, jaronwin, osadist, fuzzy_soundex, fuzzy_phonetic, fuzzy_caver, fuzzy_rsoundex, fuzzy_translit, fuzzy_script}
}

/// Calculates and returns the Levenshtein distance of two non NULL strings.
#[scalar(name = "fuzzy_leven")]
fn levenshtein(args: &[Value]) -> Value {
    let Some(arg1) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(arg2) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let dist = leven(arg1, arg2);
    return Value::from_integer(dist);
}

fn leven(s1: &str, s2: &str) -> i64 {
    let mut str1: &[u8] = s1.as_bytes();
    let mut str2: &[u8] = s2.as_bytes();
    let mut str1_len = str1.len();
    let mut str2_len = str2.len();

    if str1_len == 0 {
        return str2_len as i64;
    }

    if str2_len == 0 {
        return str1_len as i64;
    }

    while str1_len > 0 && str2_len > 0 && str1[0] == str2[0] {
        str1 = &str1[1..];
        str2 = &str2[1..];
        str1_len -= 1;
        str2_len -= 1;
    }

    let mut vector: Vec<usize> = (0..=str1_len).collect();

    let mut last_diag: usize;
    let mut cur: usize;

    for row in 1..=str2_len {
        last_diag = row - 1;
        vector[0] = row;

        for col in 1..=str1_len {
            cur = vector[col];

            let cost = if str1[col - 1] == str2[row - 1] { 0 } else { 1 };

            vector[col] = std::cmp::min(
                std::cmp::min(vector[col] + 1, vector[col - 1] + 1),
                last_diag + cost,
            );

            last_diag = cur;
        }
    }
    vector[str1_len] as i64
}

/// Calculates and returns the Damerau-Levenshtein distance of two non NULL
#[scalar(name = "fuzzy_damlev")]
fn damerau_levenshtein(args: &[Value]) -> Value {
    let Some(arg1) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(arg2) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let dist = damlev(arg1, arg2);
    return Value::from_integer(dist);
}

#[allow(clippy::needless_range_loop)]
fn damlev(s1: &str, s2: &str) -> i64 {
    let str1: &[u8] = s1.as_bytes();
    let str2: &[u8] = s2.as_bytes();
    let str1_len = str1.len();
    let str2_len = str2.len();

    if str1_len == 0 {
        return str2_len as i64;
    }

    if str2_len == 0 {
        return str1_len as i64;
    }

    let mut start = 0;
    while start < str1_len && start < str2_len && str1[start] == str2[start] {
        start += 1;
    }
    let str1 = &str1[start..];
    let str2 = &str2[start..];
    let len1 = str1.len();
    let len2 = str2.len();

    const ALPHA_SIZE: usize = 255;
    let infi = len1 + len2;

    let mut dict = vec![0usize; ALPHA_SIZE];

    let rows = len1 + 2;
    let cols = len2 + 2;
    let mut matrix = vec![vec![0usize; cols]; rows];

    matrix[0][0] = infi;

    for i in 1..rows {
        matrix[i][0] = infi;
        matrix[i][1] = i - 1;
    }
    for j in 1..cols {
        matrix[0][j] = infi;
        matrix[1][j] = j - 1;
    }

    for (row, &c1) in str1.iter().enumerate() {
        let mut db = 0;
        for (col, &c2) in str2.iter().enumerate() {
            let i = dict[c2 as usize];
            let k = db;
            let cost = if c1 == c2 { 0 } else { 1 };
            if cost == 0 {
                db = col + 1;
            }

            matrix[row + 2][col + 2] = std::cmp::min(
                std::cmp::min(
                    matrix[row + 1][col + 1] + cost,
                    matrix[row + 2][col + 1] + 1,
                ),
                std::cmp::min(
                    matrix[row + 1][col + 2] + 1,
                    matrix[i][k] + (row + 1 - i - 1) + (col + 1 - k - 1) + 1,
                ),
            );
        }
        dict[c1 as usize] = row + 1;
    }

    matrix[rows - 1][cols - 1] as i64
}
//
// fuzzy_editdist(A,B)
//
// Return the cost of transforming string A into string B.  Both strings
// must be pure ASCII text.  If A ends with '*' then it is assumed to be
// a prefix of B and extra characters on the end of B have minimal additional
// cost.
//
#[scalar(name = "fuzzy_editdist")]
fn edit_distance(args: &[Value]) {
    let Some(arg1) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(arg2) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    if let Ok(res) = editdist::edit_distance(arg1, arg2) {
        return Value::from_integer(res as i64);
    } else {
        return Value::error(ResultCode::InvalidArgs);
    }
}

// returns the hamming distance between two strings
#[scalar(name = "fuzzy_hamming")]
fn hamming(args: &[Value]) {
    let Some(arg1) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(arg2) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let dist = hamming_dist(arg1, arg2);
    return Value::from_integer(dist);
}

fn hamming_dist(s1: &str, s2: &str) -> i64 {
    let str1_b = s1.as_bytes();
    let str2_b = s2.as_bytes();

    if str1_b.len() != str2_b.len() {
        return -1_i64;
    }

    let res = str1_b
        .iter()
        .zip(str2_b.iter())
        .filter(|(a, b)| a != b)
        .count();

    res as i64
}
#[scalar(name = "fuzzy_jarowin")]
fn jaronwin(args: &[Value]) {
    let Some(arg1) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(arg2) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let res = jaro_winkler(arg1, arg2);
    return Value::from_float(res);
}

/// Calculates and returns the Jaro-Winkler distance of two non NULL strings.
fn jaro_winkler(s1: &str, s2: &str) -> f64 {
    let dist = jaro(s1, s2);

    let mut prefix_len = 0;
    for (c1, c2) in s1.chars().zip(s2.chars()) {
        if c1 == c2 {
            prefix_len += 1;
        } else {
            break;
        }

        if prefix_len == 3 {
            break;
        }
    }

    dist + (prefix_len as f64) * 0.1 * (1.0 - dist)
}

/// Calculates and returns the Jaro distance of two non NULL strings.
fn jaro(s1: &str, s2: &str) -> f64 {
    if s1 == s2 {
        return 1.0;
    }

    let s1: Vec<char> = s1.chars().collect();
    let s2: Vec<char> = s2.chars().collect();

    let len1 = s1.len();
    let len2 = s2.len();

    if len1 == 0 || len2 == 0 {
        return 0.0;
    }

    let max_dist = (cmp::max(len1, len2) / 2).saturating_sub(1);
    let mut match_count = 0;

    let mut hash_s1 = vec![false; len1];
    let mut hash_s2 = vec![false; len2];

    for i in 0..len1 {
        let start = i.saturating_sub(max_dist);
        let end = cmp::min(i + max_dist + 1, len2);

        for j in start..end {
            if s1[i] == s2[j] && !hash_s2[j] {
                hash_s1[i] = true;
                hash_s2[j] = true;
                match_count += 1;
                break;
            }
        }
    }

    if match_count == 0 {
        return 0.0;
    }

    let mut t = 0;
    let mut point = 0;

    for i in 0..len1 {
        if hash_s1[i] {
            while point < len2 && !hash_s2[point] {
                point += 1;
            }
            if point < len2 && s1[i] != s2[point] {
                t += 1;
            }
            point += 1;
        }
    }

    let t = t as f64 / 2.0;
    let match_count = match_count as f64;

    (match_count / len1 as f64 + match_count / len2 as f64 + (match_count - t) / match_count) / 3.0
}

/// Computes and returns the Optimal String Alignment distance for two non NULL
#[scalar(name = "fuzzy_osadist")]
fn osadist(args: &[Value]) {
    let Some(arg1) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(arg2) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let dist = optimal_string_alignment(arg1, arg2);
    return Value::from_integer(dist as i64);
}

fn optimal_string_alignment(s1: &str, s2: &str) -> usize {
    let mut s1_chars: Vec<char> = s1.chars().collect();
    let mut s2_chars: Vec<char> = s2.chars().collect();

    let mut len1 = s1_chars.len();
    let mut len2 = s2_chars.len();

    while len1 > 0 && len2 > 0 && s1_chars[0] == s2_chars[0] {
        s1_chars.remove(0);
        s2_chars.remove(0);
        len1 -= 1;
        len2 -= 1;
    }

    if len1 == 0 {
        return len2;
    }
    if len2 == 0 {
        return len1;
    }

    let mut matrix = vec![vec![0usize; len2 + 1]; len1 + 1];

    // clippy from this
    //for i in 0..=len1 {
    //   matrix[i][0] = i;
    //}
    //for j in 0..=len2 {
    //    matrix[0][j] = j;
    //}
    // to
    for (i, row) in matrix.iter_mut().enumerate().take(len1 + 1) {
        row[0] = i;
    }

    for (j, item) in matrix[0].iter_mut().enumerate().take(len2 + 1) {
        *item = j;
    }

    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] {
                0
            } else {
                1
            };

            let deletion = matrix[i - 1][j] + 1;
            let insertion = matrix[i][j - 1] + 1;
            let substitution = matrix[i - 1][j - 1] + cost;

            matrix[i][j] = deletion.min(insertion).min(substitution);

            if i > 1
                && j > 1
                && s1_chars[i % len1] == s2_chars[j - 2]
                && s1_chars[i - 2] == s2_chars[j % len2]
            {
                matrix[i][j] = matrix[i][j].min(matrix[i - 2][j - 2] + cost);
            }
        }
    }

    matrix[len1][len2]
}

#[scalar(name = "fuzzy_soundex")]
fn fuzzy_soundex(args: &[Value]) {
    let arg1 = args[0].to_text();
    if let Some(txt) = soundex::soundex(arg1) {
        Value::from_text(txt)
    } else {
        Value::null()
    }
}

#[scalar(name = "fuzzy_phonetic")]
fn fuzzy_phonetic(args: &[Value]) {
    let arg1 = args[0].to_text();
    if let Some(txt) = phonetic::phonetic_hash_str(arg1) {
        Value::from_text(txt)
    } else {
        Value::null()
    }
}

#[scalar(name = "fuzzy_caver")]
fn fuzzy_caver(args: &[Value]) {
    let arg1 = args[0].to_text();
    if let Some(txt) = caver::caver_str(arg1) {
        Value::from_text(txt)
    } else {
        Value::null()
    }
}

#[scalar(name = "fuzzy_rsoundex")]
pub fn fuzzy_rsoundex(args: &[Value]) {
    let arg1 = args[0].to_text();
    if let Some(txt) = rsoundex::rsoundex(arg1) {
        Value::from_text(txt)
    } else {
        Value::null()
    }
}

//Convert a string that contains non-ASCII Roman characters into
//pure ASCII.
#[scalar(name = "fuzzy_translit")]
fn fuzzy_translit(args: &[Value]) {
    let Some(arg) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };
    let dist = translit::transliterate_str(arg);
    return Value::from_text(dist);
}

// Try to determine the dominant script used by the word X and return
// its ISO 15924 numeric code.
//
// The current implementation only understands the following scripts:
//
//    125  (Hebrew)
//    160  (Arabic)
//    200  (Greek)
//    215  (Latin)
//    220  (Cyrillic)
//
// This routine will return 998 if the input X contains characters from
// two or more of the above scripts or 999 if X contains no characters
// from any of the above scripts.
#[scalar(name = "fuzzy_script")]
pub fn fuzzy_script(args: &[Value]) {
    let Some(arg) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };
    let dist = translit::script_code(arg.as_bytes());
    return Value::from_integer(dist as i64);
}

//tests adapted from sqlean fuzzy
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_damlev() {
        let cases = vec![
            ("abc", "abc", 0),
            ("abc", "", 3),
            ("", "abc", 3),
            ("abc", "ab", 1),
            ("abc", "abcd", 1),
            ("abc", "acb", 1),
            ("abc", "ca", 2),
        ];

        for (s1, s2, expected) in cases {
            let got = damlev(s1, s2);
            assert_eq!(got, expected, "damlev({s1}, {s2}) failed");
        }
    }

    #[test]
    fn test_hamming() {
        let cases = vec![
            ("abc", "abc", 0),
            ("abc", "", -1),
            ("", "abc", -1),
            ("hello", "hellp", 1),
            ("hello", "heloh", 2),
        ];

        for (s1, s2, expected) in cases {
            let got = hamming_dist(s1, s2);
            assert_eq!(got, expected, "hamming({s1}, {s2}) failed");
        }
    }

    #[test]
    fn test_jaro_win() {
        let cases: Vec<(&str, &str, f64)> = vec![
            ("abc", "abc", 1.0),
            ("abc", "", 0.0),
            ("", "abc", 0.0),
            ("my string", "my tsring", 0.974),
            ("my string", "my ntrisg", 0.896),
        ];

        for (s1, s2, expected) in cases {
            let got = jaro_winkler(s1, s2);

            if (expected - 0.974).abs() < 1e-6 || (expected - 0.896).abs() < 1e-6 {
                let got_rounded = (got * 1000.0).round() / 1000.0;
                assert!(
                    (got_rounded - expected).abs() < 1e-6,
                    "jaro_winkler({s1}, {s2}) failed: got {got_rounded}, expected {expected}"
                );
            } else {
                assert!(
                    (got - expected).abs() < 1e-6,
                    "jaro_winkler({s1}, {s2}) failed: got {got}, expected {expected}"
                );
            }
        }
    }

    #[test]
    fn test_leven() {
        let cases = vec![
            ("abc", "abc", 0),
            ("abc", "", 3),
            ("", "abc", 3),
            ("abc", "ab", 1),
            ("abc", "abcd", 1),
            ("abc", "acb", 2),
            ("abc", "ca", 3),
        ];

        for (s1, s2, expected) in cases {
            let got = leven(s1, s2);
            assert_eq!(got, expected, "leven({s1}, {s2}) failed");
        }
    }

    #[test]
    fn test_edit_distance() {
        let test_cases = vec![
            ("abc", "abc", 0),
            ("abc", "", 300),
            ("", "abc", 75),
            ("abc", "ab", 100),
            ("abc", "abcd", 25),
            ("abc", "acb", 110),
            ("abc", "ca", 225),
            //more cases
            ("awesome", "aewsme", 215),
            ("kitten", "sitting", 105),
            ("flaw", "lawn", 110),
            ("rust", "trust", 100),
            ("gumbo", "gambol", 65),
        ];
        for (s1, s2, expected) in test_cases {
            let res = editdist::edit_distance(s1, s2).unwrap();
            assert_eq!(res, expected, "edit_distance({s1}, {s2}) failed");
        }
    }

    #[test]
    fn test_osadist() {
        let cases = vec![
            ("abc", "abc", 0),
            ("abc", "", 3),
            ("", "abc", 3),
            ("abc", "ab", 1),
            ("abc", "abcd", 1),
            ("abc", "acb", 2),
            ("abc", "ca", 3),
        ];

        for (s1, s2, expected) in cases {
            let got = optimal_string_alignment(s1, s2);
            assert_eq!(got, expected, "osadist({s1}, {s2}) failed");
        }
    }
    #[test]
    fn test_soundex() {
        let cases = vec![
            (None, None),
            (Some(""), Some("".to_string())),
            (Some("phonetics"), Some("P532".to_string())),
            (Some("is"), Some("I200".to_string())),
            (Some("awesome"), Some("A250".to_string())),
        ];

        for (input, expected) in cases {
            let result = soundex::soundex(input);
            assert_eq!(
                result, expected,
                "fuzzy_soundex({input:?}) failed: expected {expected:?}, got {result:?}"
            );
        }
    }
    #[test]
    fn test_phonetic() {
        let cases = vec![
            (None, None),
            (Some(""), Some("".to_string())),
            (Some("phonetics"), Some("BAMADAC".to_string())),
            (Some("is"), Some("AC".to_string())),
            (Some("awesome"), Some("ABACAMA".to_string())),
        ];

        for (input, expected) in cases {
            let result = phonetic::phonetic_hash_str(input);
            assert_eq!(
                result, expected,
                "fuzzy_phonetic({input:?}) failed: expected {expected:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn test_caver() {
        let cases = vec![
            (None, None),
            (Some(""), Some("".to_string())),
            (Some("phonetics"), Some("FNTKS11111".to_string())),
            (Some("is"), Some("AS11111111".to_string())),
            (Some("awesome"), Some("AWSM111111".to_string())),
        ];

        for (input, expected) in cases {
            let result = caver::caver_str(input);
            assert_eq!(
                result, expected,
                "fuzzy_caver({input:?}) failed: expected {expected:?}, got {result:?}"
            );
        }
    }
    #[test]
    fn test_rsoundex() {
        let cases = vec![
            (None, None),
            (Some(""), Some("".to_string())),
            (Some("phonetics"), Some("P1080603".to_string())),
            (Some("is"), Some("I03".to_string())),
            (Some("awesome"), Some("A03080".to_string())),
        ];

        for (input, expected) in cases {
            let result = rsoundex::rsoundex(input);
            assert_eq!(
                result, expected,
                "fuzzy_rsoundex({input:?}) failed: expected {expected:?}, got {result:?}"
            );
        }
    }
}
