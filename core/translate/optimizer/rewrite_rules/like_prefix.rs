use crate::translate::collate::CollationSeq;
use crate::translate::plan::{TableReferences, WhereTerm};
use crate::vdbe::affinity::Affinity;
use turso_parser::ast;

/// Rewrite `col LIKE 'prefix%'` patterns to add range constraints that enable
/// index seeks.
///
/// For each WHERE term of the form `col LIKE '<prefix>...'` where `<prefix>`
/// is the literal characters before the first wildcard (`%` or `_`), this pass
/// adds two new WHERE terms:
///
///   col >= UPPER(prefix)   AND   col < increment(LOWER(prefix))
///
/// Since LIKE is case-insensitive for ASCII, the range must cover all case
/// variants. In BINARY collation uppercase sorts before lowercase, so
/// UPPER(prefix) is the lowest match and increment(LOWER(prefix)) is one
/// past the highest. This range is intentionally wider than the LIKE — the
/// original LIKE term is kept as a residual filter for exact correctness.
///
/// Restrictions (we bail out and leave the LIKE untouched when any apply):
/// - NOT LIKE
/// - GLOB / MATCH / REGEXP
/// - ESCAPE clause present
/// - Pattern is not a string literal
/// - Prefix is empty (pattern starts with `%` or `_`)
/// - Prefix contains non-ASCII bytes
/// - Prefix contains escaped single quotes (`''`)
/// - Column does not have TEXT affinity (non-text values use native type
///   ordering which differs from the text ordering assumed by the range bounds)
pub fn rewrite_like_prefix_to_range(
    where_clause: &mut Vec<WhereTerm>,
    table_references: &TableReferences,
) {
    let original_len = where_clause.len();
    for i in 0..original_len {
        if where_clause[i].consumed {
            continue;
        }
        let Some((col_expr, prefix, suffix_is_just_percent)) =
            extract_like_prefix(&where_clause[i].expr)
        else {
            continue;
        };

        if !column_has_text_affinity(col_expr, table_references) {
            continue;
        }

        // Note: this is the *table column* collation. The index may use a
        // different collation (e.g. CREATE INDEX idx ON t(col COLLATE NOCASE)).
        // This is safe because the constraint system (constraints.rs) rejects
        // index seeks when the index collation disagrees with the table column,
        // so the range terms will only be evaluated under matching collation.
        let collation = column_collation(col_expr, table_references);
        let col_owned = col_expr.clone();
        let (ge_bound, lt_bound) = compute_quoted_bounds(prefix);
        let from_outer_join = where_clause[i].from_outer_join;

        // When the range bounds are provably exact (no false positives),
        // mark the original LIKE as consumed to eliminate the residual filter.
        // This is safe when the suffix is exactly '%' AND:
        //
        // NOCASE: The upper bound's last byte must not be an ASCII uppercase
        //   letter. Only the last byte matters because only it is incremented;
        //   earlier bytes are just lowercased, which cannot overflow.
        //   If the incremented byte is uppercase (e.g. '@'+1 = 'A'), NOCASE
        //   folds it to lowercase ('a' = 97), making the range much wider
        //   than intended and capturing false positives like '[' (91).
        //   For letter prefixes this is fine: 'c'+1 = 'd' (lowercase, no folding).
        //
        // BINARY: The prefix must have no ASCII letters, so
        //   UPPER(prefix) == LOWER(prefix) and the bounds collapse to a
        //   single exact range.
        //
        // RTRIM: Never consume. RTRIM trims trailing spaces before comparison,
        //   so `col >= ' '` under RTRIM matches `''` (empty), widening the
        //   range beyond what LIKE matches.
        //
        // prefix is guaranteed non-empty by extract_like_prefix
        let incremented_last = prefix.as_bytes().last().unwrap().to_ascii_lowercase() + 1;
        let can_consume = suffix_is_just_percent
            && match collation {
                CollationSeq::NoCase => !incremented_last.is_ascii_uppercase(),
                CollationSeq::Rtrim => false,
                _ => prefix.bytes().all(|b| !b.is_ascii_alphabetic()),
            };
        if can_consume {
            where_clause[i].consumed = true;
        }

        // col >= UPPER(prefix)
        where_clause.push(WhereTerm {
            expr: ast::Expr::Binary(
                Box::new(col_owned.clone()),
                ast::Operator::GreaterEquals,
                Box::new(ast::Expr::Literal(ast::Literal::String(ge_bound))),
            ),
            from_outer_join,
            consumed: false,
        });

        // col < increment(LOWER(prefix))
        where_clause.push(WhereTerm {
            expr: ast::Expr::Binary(
                Box::new(col_owned),
                ast::Operator::Less,
                Box::new(ast::Expr::Literal(ast::Literal::String(lt_bound))),
            ),
            from_outer_join,
            consumed: false,
        });
    }
}

/// If `expr` is `col LIKE '<literal>'` (no ESCAPE, not NOT, LIKE operator only),
/// extract the column expression, the literal prefix up to the first wildcard,
/// and whether the suffix after the prefix is exactly `%` (meaning the range
/// bounds are tight and no residual filter is needed).
/// Returns None if any precondition fails.
fn extract_like_prefix(expr: &ast::Expr) -> Option<(&ast::Expr, &str, bool)> {
    let ast::Expr::Like {
        lhs,
        not,
        op,
        rhs,
        escape,
    } = expr
    else {
        return None;
    };

    if *not || *op != ast::LikeOperator::Like || escape.is_some() {
        return None;
    }

    let ast::Expr::Literal(ast::Literal::String(pattern)) = rhs.as_ref() else {
        return None;
    };

    let (prefix, suffix) = extract_prefix_from_quoted(pattern)?;
    if prefix.is_empty() || !prefix.is_ascii() {
        return None;
    }

    let suffix_is_just_percent = suffix == "%";
    Some((lhs.as_ref(), prefix, suffix_is_just_percent))
}

/// Given a SQL quoted string like `'abc%'`, extract the literal prefix
/// (before the first wildcard) and the suffix (everything after the prefix,
/// still within the quotes) as `&str` slices of the original string.
/// Returns None if the string is malformed or the prefix region contains
/// escaped quotes (`''`), avoiding allocation in the common case.
fn extract_prefix_from_quoted(s: &str) -> Option<(&str, &str)> {
    let inner = s.strip_prefix('\'')?.strip_suffix('\'')?;
    let end = inner
        .bytes()
        .position(|b| b == b'%' || b == b'_' || b == b'\'')
        .unwrap_or(inner.len());
    // If we stopped at a quote, the prefix contains an escaped quote —
    // bail out rather than allocate for this rare case.
    if end < inner.len() && inner.as_bytes()[end] == b'\'' {
        return None;
    }
    Some((&inner[..end], &inner[end..]))
}

/// Returns true if `expr` is a Column with TEXT affinity.
/// Non-TEXT columns can store integer/real values whose native type ordering
/// differs from text ordering, making the range bounds unsound.
fn column_has_text_affinity(expr: &ast::Expr, table_references: &TableReferences) -> bool {
    let ast::Expr::Column { table, column, .. } = expr else {
        return false;
    };
    let Some((_, table_ref)) = table_references.find_table_by_internal_id(*table) else {
        return false;
    };
    let Some(col) = table_ref.get_column_at(*column) else {
        return false;
    };
    col.affinity() == Affinity::Text
}

/// Returns the collation sequence for a Column expression.
/// Defaults to Binary when the column has no explicit collation or when
/// the expression is not a Column.
fn column_collation(expr: &ast::Expr, table_references: &TableReferences) -> CollationSeq {
    let ast::Expr::Column { table, column, .. } = expr else {
        return CollationSeq::Binary;
    };
    let Some((_, table_ref)) = table_references.find_table_by_internal_id(*table) else {
        return CollationSeq::Binary;
    };
    let Some(col) = table_ref.get_column_at(*column) else {
        return CollationSeq::Binary;
    };
    col.collation()
}

/// Build the quoted `>=` and `<` bound strings from the prefix.
///
/// Returns `(ge_bound, lt_bound)` where:
/// - `ge_bound` = `'UPPER(prefix)'`  (the `>=` bound)
/// - `lt_bound` = `'increment(LOWER(prefix))'`  (the `<` bound)
///
/// Since the prefix is guaranteed ASCII, increment simply adds 1 to the last
/// byte's lowercase form (no carry possible for ASCII bytes < 0x80).
fn compute_quoted_bounds(prefix: &str) -> (String, String) {
    let bytes = prefix.as_bytes();
    let len = bytes.len();

    let mut ge = String::with_capacity(len + 2);
    ge.push('\'');
    for &b in bytes {
        ge.push(b.to_ascii_uppercase() as char);
    }
    ge.push('\'');

    let mut lt = String::with_capacity(len + 2);
    lt.push('\'');
    for &b in &bytes[..len - 1] {
        lt.push(b.to_ascii_lowercase() as char);
    }
    lt.push((bytes[len - 1].to_ascii_lowercase() + 1) as char);
    lt.push('\'');

    (ge, lt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_prefix_from_quoted() {
        assert_eq!(extract_prefix_from_quoted("'abc%'"), Some(("abc", "%")));
        assert_eq!(
            extract_prefix_from_quoted("'abc_def'"),
            Some(("abc", "_def"))
        );
        assert_eq!(extract_prefix_from_quoted("'%abc'"), Some(("", "%abc")));
        assert_eq!(extract_prefix_from_quoted("'_abc'"), Some(("", "_abc")));
        assert_eq!(extract_prefix_from_quoted("'abc'"), Some(("abc", "")));
        assert_eq!(extract_prefix_from_quoted("''"), Some(("", "")));
        assert_eq!(extract_prefix_from_quoted("'a%b%c'"), Some(("a", "%b%c")));
        assert_eq!(
            extract_prefix_from_quoted("'hello world%'"),
            Some(("hello world", "%"))
        );
        // Escaped quote in prefix region — returns None
        assert_eq!(extract_prefix_from_quoted("'it''s%'"), None);
        // Malformed
        assert_eq!(extract_prefix_from_quoted("abc"), None);
        assert_eq!(extract_prefix_from_quoted("'abc"), None);
        assert_eq!(extract_prefix_from_quoted("abc'"), None);
    }

    #[test]
    fn test_compute_quoted_bounds() {
        assert_eq!(
            compute_quoted_bounds("abc"),
            ("'ABC'".to_string(), "'abd'".to_string())
        );
        assert_eq!(
            compute_quoted_bounds("AbC"),
            ("'ABC'".to_string(), "'abd'".to_string())
        );
        assert_eq!(
            compute_quoted_bounds("a"),
            ("'A'".to_string(), "'b'".to_string())
        );
        assert_eq!(
            compute_quoted_bounds("123"),
            ("'123'".to_string(), "'124'".to_string())
        );
        assert_eq!(
            compute_quoted_bounds("az"),
            ("'AZ'".to_string(), "'a{'".to_string())
        );
        assert_eq!(
            compute_quoted_bounds("abc123"),
            ("'ABC123'".to_string(), "'abc124'".to_string())
        );
        assert_eq!(
            compute_quoted_bounds("z"),
            ("'Z'".to_string(), "'{'".to_string())
        );
    }

    #[test]
    fn test_extract_like_prefix_valid() {
        let col = ast::Expr::Column {
            database: None,
            table: 0.into(),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = ast::Expr::Like {
            lhs: Box::new(col),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(ast::Expr::Literal(ast::Literal::String(
                "'abc%'".to_string(),
            ))),
            escape: None,
        };
        let result = extract_like_prefix(&expr);
        assert!(result.is_some());
        let (_, prefix, suffix_is_just_percent) = result.unwrap();
        assert_eq!(prefix, "abc");
        assert!(suffix_is_just_percent);
    }

    #[test]
    fn test_extract_like_prefix_not_like() {
        let col = ast::Expr::Column {
            database: None,
            table: 0.into(),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = ast::Expr::Like {
            lhs: Box::new(col),
            not: true,
            op: ast::LikeOperator::Like,
            rhs: Box::new(ast::Expr::Literal(ast::Literal::String(
                "'abc%'".to_string(),
            ))),
            escape: None,
        };
        assert!(extract_like_prefix(&expr).is_none());
    }

    #[test]
    fn test_extract_like_prefix_with_escape() {
        let col = ast::Expr::Column {
            database: None,
            table: 0.into(),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = ast::Expr::Like {
            lhs: Box::new(col),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(ast::Expr::Literal(ast::Literal::String(
                "'abc%'".to_string(),
            ))),
            escape: Some(Box::new(ast::Expr::Literal(ast::Literal::String(
                "'\\''".to_string(),
            )))),
        };
        assert!(extract_like_prefix(&expr).is_none());
    }

    #[test]
    fn test_extract_like_prefix_glob() {
        let col = ast::Expr::Column {
            database: None,
            table: 0.into(),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = ast::Expr::Like {
            lhs: Box::new(col),
            not: false,
            op: ast::LikeOperator::Glob,
            rhs: Box::new(ast::Expr::Literal(ast::Literal::String(
                "'abc*'".to_string(),
            ))),
            escape: None,
        };
        assert!(extract_like_prefix(&expr).is_none());
    }

    #[test]
    fn test_extract_like_prefix_leading_wildcard() {
        let col = ast::Expr::Column {
            database: None,
            table: 0.into(),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = ast::Expr::Like {
            lhs: Box::new(col),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(ast::Expr::Literal(ast::Literal::String(
                "'%abc'".to_string(),
            ))),
            escape: None,
        };
        assert!(extract_like_prefix(&expr).is_none());
    }
}
