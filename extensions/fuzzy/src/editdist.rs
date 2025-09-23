// Adapted from SQLite spellfix.c extension and sqlean fuzzy/editdist.c
use crate::common::*;

#[derive(Debug, PartialEq)]
pub enum EditDistanceError {
    NonAsciiInput,
}

pub type EditDistanceResult = Result<i32, EditDistanceError>;

fn character_class(c_prev: u8, c: u8) -> u8 {
    if c_prev == 0 {
        INIT_CLASS[(c & 0x7f) as usize]
    } else {
        MID_CLASS[(c & 0x7f) as usize]
    }
}

/// Return the cost of inserting or deleting character c immediately
/// following character c_prev. If c_prev == 0, that means c is the first
/// character of the word.
fn insert_or_delete_cost(c_prev: u8, c: u8, c_next: u8) -> i32 {
    let class_c = character_class(c_prev, c);

    if class_c == CCLASS_SILENT {
        return 1;
    }

    if c_prev == c {
        return 10;
    }

    if class_c == CCLASS_VOWEL && (c_prev == b'r' || c_next == b'r') {
        return 20; // Insert a vowel before or after 'r'
    }

    let class_c_prev = character_class(c_prev, c_prev);
    if class_c == class_c_prev {
        if class_c == CCLASS_VOWEL {
            15
        } else {
            50
        }
    } else {
        // Any other character insertion or deletion
        100
    }
}

const FINAL_INS_COST_DIV: i32 = 4;

/// Return the cost of substituting c_to in place of c_from assuming
/// the previous character is c_prev. If c_prev == 0 then c_to is the first
/// character of the word.
fn substitute_cost(c_prev: u8, c_from: u8, c_to: u8) -> i32 {
    if c_from == c_to {
        return 0;
    }

    if c_from == (c_to ^ 0x20) && c_to.is_ascii_alphabetic() {
        return 0;
    }

    let class_from = character_class(c_prev, c_from);
    let class_to = character_class(c_prev, c_to);

    if class_from == class_to {
        40
    } else if (CCLASS_B..=CCLASS_Y).contains(&class_from)
        && (CCLASS_B..=CCLASS_Y).contains(&class_to)
    {
        75
    } else {
        100
    }
}

/// Given two strings z_a and z_b which are pure ASCII, return the cost
/// of transforming z_a into z_b. If z_a ends with '*' assume that it is
/// a prefix of z_b and give only minimal penalty for extra characters
/// on the end of z_b.
///
/// Returns cost where smaller numbers mean a closer match
///
/// Returns Err for Non-ASCII characters on input
pub fn edit_distance(z_a: &str, z_b: &str) -> EditDistanceResult {
    if z_a.is_empty() && z_b.is_empty() {
        return Ok(0);
    }

    let za_bytes = z_a.as_bytes();
    let zb_bytes = z_b.as_bytes();

    if !z_a.is_ascii() || !z_b.is_ascii() {
        return Err(EditDistanceError::NonAsciiInput);
    }

    if z_a.is_empty() {
        let mut res = 0;
        let mut c_b_prev = 0u8;
        let zb_bytes = z_b.as_bytes();

        for (i, &c_b) in zb_bytes.iter().enumerate() {
            let c_b_next = if i + 1 < zb_bytes.len() {
                zb_bytes[i + 1]
            } else {
                0
            };
            res += insert_or_delete_cost(c_b_prev, c_b, c_b_next) / FINAL_INS_COST_DIV;
            c_b_prev = c_b;
        }
        return Ok(res);
    }

    if z_b.is_empty() {
        let mut res = 0;
        let mut c_a_prev = 0u8;
        let za_bytes = z_a.as_bytes();

        for (i, &c_a) in za_bytes.iter().enumerate() {
            let c_a_next = if i + 1 < za_bytes.len() {
                za_bytes[i + 1]
            } else {
                0
            };
            res += insert_or_delete_cost(c_a_prev, c_a, c_a_next);
            c_a_prev = c_a;
        }
        return Ok(res);
    }

    let mut za_start = 0;
    let mut zb_start = 0;

    // Skip any common prefix
    while za_start < za_bytes.len()
        && zb_start < zb_bytes.len()
        && za_bytes[za_start] == zb_bytes[zb_start]
    {
        za_start += 1;
        zb_start += 1;
    }

    // If both strings are exhausted after common prefix
    if za_start >= za_bytes.len() && zb_start >= zb_bytes.len() {
        return Ok(0);
    }

    let za_remaining = &za_bytes[za_start..];
    let zb_remaining = &zb_bytes[zb_start..];
    let n_a = za_remaining.len();
    let n_b = zb_remaining.len();

    // Special processing if either remaining string is empty after prefix matching
    if n_a == 0 {
        let mut res = 0;
        let mut c_b_prev = if za_start > 0 {
            za_bytes[za_start - 1]
        } else {
            0
        };

        for (i, &c_b) in zb_remaining.iter().enumerate() {
            let c_b_next = if i + 1 < n_b { zb_remaining[i + 1] } else { 0 };
            res += insert_or_delete_cost(c_b_prev, c_b, c_b_next) / FINAL_INS_COST_DIV;
            c_b_prev = c_b;
        }
        return Ok(res);
    }

    if n_b == 0 {
        let mut res = 0;
        let mut c_a_prev = if za_start > 0 {
            za_bytes[za_start - 1]
        } else {
            0
        };

        for (i, &c_a) in za_remaining.iter().enumerate() {
            let c_a_next = if i + 1 < n_a { za_remaining[i + 1] } else { 0 };
            res += insert_or_delete_cost(c_a_prev, c_a, c_a_next);
            c_a_prev = c_a;
        }
        return Ok(res);
    }

    // Check if a is a prefix pattern
    if za_remaining.len() == 1 && za_remaining[0] == b'*' {
        return Ok(0);
    }

    let mut m = vec![0i32; n_b + 1];
    let mut cx = vec![0u8; n_b + 1];

    let dc = if za_start > 0 {
        za_bytes[za_start - 1]
    } else {
        0
    };
    m[0] = 0;
    cx[0] = dc;

    let mut c_b_prev = dc;
    for x_b in 1..=n_b {
        let c_b = zb_remaining[x_b - 1];
        let c_b_next = if x_b < n_b { zb_remaining[x_b] } else { 0 };
        cx[x_b] = c_b;
        m[x_b] = m[x_b - 1] + insert_or_delete_cost(c_b_prev, c_b, c_b_next);
        c_b_prev = c_b;
    }

    let mut c_a_prev = dc;
    for x_a in 1..=n_a {
        let last_a = x_a == n_a;
        let c_a = za_remaining[x_a - 1];
        let c_a_next = if x_a < n_a { za_remaining[x_a] } else { 0 };

        if c_a == b'*' && last_a {
            break;
        }

        let mut d = m[0];
        m[0] = d + insert_or_delete_cost(c_a_prev, c_a, c_a_next);

        for x_b in 1..=n_b {
            let c_b = zb_remaining[x_b - 1];
            let c_b_next = if x_b < n_b { zb_remaining[x_b] } else { 0 };

            // Cost to insert c_b
            let mut ins_cost = insert_or_delete_cost(cx[x_b - 1], c_b, c_b_next);
            if last_a {
                ins_cost /= FINAL_INS_COST_DIV;
            }

            // Cost to delete c_a
            let del_cost = insert_or_delete_cost(cx[x_b], c_a, c_b_next);

            // Cost to substitute c_a -> c_b
            let sub_cost = substitute_cost(cx[x_b - 1], c_a, c_b);

            // Find best cost
            let mut total_cost = ins_cost + m[x_b - 1];
            let mut ncx = c_b;

            if del_cost + m[x_b] < total_cost {
                total_cost = del_cost + m[x_b];
                ncx = c_a;
            }

            if sub_cost + d < total_cost {
                total_cost = sub_cost + d;
            }

            d = m[x_b];
            m[x_b] = total_cost;
            cx[x_b] = ncx;
        }
        c_a_prev = c_a;
    }

    let res = if za_remaining.last() == Some(&b'*') {
        let mut min_cost = m[1];

        for &val in m.iter().skip(1).take(n_b) {
            if val < min_cost {
                min_cost = val;
            }
        }

        min_cost
    } else {
        m[n_b]
    };

    Ok(res)
}
