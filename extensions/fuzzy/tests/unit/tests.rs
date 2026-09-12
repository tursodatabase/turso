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
