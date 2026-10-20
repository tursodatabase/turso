//! Compare EXPLAIN QUERY PLAN output without the parts that differ between
//! two planners that chose the same plan: the instruction addresses the rows
//! are numbered by, the ids in generated names, and where a subquery's rows
//! sit among its siblings.

use super::{ComparisonResult, exact, parse_expected_rows};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Compare rows exactly after normalizing every plan in them.
pub fn compare(actual: &[Vec<String>], expected_lines: &[String]) -> ComparisonResult {
    let actual = normalize_rows(actual);
    let expected = normalize_rows(&parse_expected_rows(expected_lines));
    let expected_lines = expected.iter().map(|row| row.join("|")).collect::<Vec<_>>();
    exact::compare(&actual, &expected_lines)
}

/// The ids in generated names (`ephemeral_t_t3`, `LIST SUBQUERY 2`).
fn ids_in_names() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"_[ts]\d+\b|SUBQUERY \d+\b").expect("a valid regex"))
}

fn strip_ids(text: &str) -> String {
    ids_in_names()
        .replace_all(text, |found: &regex::Captures| {
            if found[0].starts_with("SUBQUERY") {
                "SUBQUERY N".to_string()
            } else {
                "_N".to_string()
            }
        })
        .into_owned()
}

/// A row that explains a subquery, which a planner may list before or after
/// the loops that use it.
fn is_subquery_row(detail: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"SUBQUERY (\d+|N)$").expect("a valid regex"))
        .is_match(detail)
}

/// The order the rows of one plan take after each parent's subquery rows
/// move behind its other rows, and the new id of each old id, numbered in
/// that order from 1.
fn reorder(
    ids: &[u64],
    parents: &[Option<u64>],
    details: &[String],
) -> (Vec<usize>, HashMap<u64, u64>) {
    let known: HashMap<u64, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    let mut children: HashMap<Option<u64>, Vec<usize>> = HashMap::new();
    for (index, parent) in parents.iter().enumerate() {
        let parent = parent.filter(|parent| known.contains_key(parent));
        children.entry(parent).or_default().push(index);
    }
    let mut renumbered = HashMap::new();
    let mut sequence = Vec::with_capacity(ids.len());
    fn visit(
        node: Option<u64>,
        children: &HashMap<Option<u64>, Vec<usize>>,
        ids: &[u64],
        details: &[String],
        sequence: &mut Vec<usize>,
    ) {
        let Some(list) = children.get(&node) else {
            return;
        };
        let (plain, subqueries): (Vec<usize>, Vec<usize>) = list
            .iter()
            .partition(|index| !is_subquery_row(&details[**index]));
        for index in plain.into_iter().chain(subqueries) {
            sequence.push(index);
            visit(Some(ids[index]), children, ids, details, sequence);
        }
    }
    visit(None, &children, ids, details, &mut sequence);
    for (position, index) in sequence.iter().enumerate() {
        renumbered.insert(ids[*index], position as u64 + 1);
    }
    (sequence, renumbered)
}

/// Whether a row is one text row of EXPLAIN QUERY PLAN: id, parent, 0, detail.
fn text_plan_row(row: &[String]) -> Option<(u64, u64, &str)> {
    let [id, parent, zero, detail] = row else {
        return None;
    };
    if zero != "0" {
        return None;
    }
    Some((id.parse().ok()?, parent.parse().ok()?, detail.as_str()))
}

fn normalize_text_plan(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    let parsed = rows
        .iter()
        .map(|row| text_plan_row(row).expect("a text plan row"))
        .collect::<Vec<_>>();
    let ids = parsed.iter().map(|(id, ..)| *id).collect::<Vec<_>>();
    let parents = parsed
        .iter()
        .map(|(_, parent, _)| (*parent != 0).then_some(*parent))
        .collect::<Vec<_>>();
    let details = parsed
        .iter()
        .map(|(.., detail)| strip_ids(detail))
        .collect::<Vec<_>>();
    let (sequence, renumbered) = reorder(&ids, &parents, &details);
    sequence
        .into_iter()
        .map(|index| {
            let parent = parents[index]
                .and_then(|parent| renumbered.get(&parent).copied())
                .unwrap_or(0);
            vec![
                renumbered[&ids[index]].to_string(),
                parent.to_string(),
                "0".to_string(),
                details[index].clone(),
            ]
        })
        .collect()
}

fn strip_ids_in_json(value: &mut Value) {
    match value {
        Value::String(text) => *text = strip_ids(text),
        Value::Array(items) => items.iter_mut().for_each(strip_ids_in_json),
        Value::Object(fields) => fields.values_mut().for_each(strip_ids_in_json),
        _ => {}
    }
}

/// Normalize `EXPLAIN QUERY PLAN FORMAT=JSON` output: its `nodes`, and the
/// node ids `cte_materializations` point at.
fn normalize_json_plan(text: &str) -> Option<String> {
    let mut plan: Value = serde_json::from_str(text).ok()?;
    let nodes = plan.get("nodes")?.as_array()?.clone();
    let ids = nodes
        .iter()
        .map(|node| node.get("id")?.as_u64())
        .collect::<Option<Vec<_>>>()?;
    let parents = nodes
        .iter()
        .map(|node| node.get("parent").and_then(Value::as_u64))
        .collect::<Vec<_>>();
    let details = nodes
        .iter()
        .map(|node| {
            node.get("detail")
                .and_then(Value::as_str)
                .map(strip_ids)
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let (sequence, renumbered) = reorder(&ids, &parents, &details);
    let renumber = |id: &Value| -> Value {
        id.as_u64()
            .and_then(|id| renumbered.get(&id))
            .map_or(Value::Null, |id| Value::from(*id))
    };
    let reordered = sequence
        .into_iter()
        .map(|index| {
            let mut node = nodes[index].clone();
            if let Some(fields) = node.as_object_mut() {
                fields.insert("id".to_string(), Value::from(renumbered[&ids[index]]));
                let parent = fields.get("parent").map(&renumber).unwrap_or(Value::Null);
                fields.insert("parent".to_string(), parent);
            }
            node
        })
        .collect::<Vec<_>>();
    plan["nodes"] = Value::Array(reordered);
    if let Some(ctes) = plan
        .get_mut("cte_materializations")
        .and_then(Value::as_array_mut)
    {
        for cte in ctes {
            if let Some(node_ids) = cte.get_mut("nodes").and_then(Value::as_array_mut) {
                for id in node_ids.iter_mut() {
                    *id = renumber(id);
                }
            }
        }
    }
    strip_ids_in_json(&mut plan);
    Some(plan.to_string())
}

/// Normalize every plan among `rows`: each run of text plan rows (a new
/// plan starts where the ids stop growing) and each JSON plan row. Other
/// rows stay as they are.
pub fn normalize_rows(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut output = Vec::with_capacity(rows.len());
    let mut run: Vec<Vec<String>> = Vec::new();
    let mut last_id = 0;
    let flush = |run: &mut Vec<Vec<String>>, output: &mut Vec<Vec<String>>| {
        if !run.is_empty() {
            output.extend(normalize_text_plan(run));
            run.clear();
        }
    };
    for row in rows {
        if let Some((id, ..)) = text_plan_row(row) {
            if id <= last_id {
                flush(&mut run, &mut output);
            }
            last_id = id;
            run.push(row.clone());
            continue;
        }
        flush(&mut run, &mut output);
        last_id = 0;
        match row.as_slice() {
            [text] if text.starts_with("{\"version\":") => match normalize_json_plan(text) {
                Some(plan) => output.push(vec![plan]),
                None => output.push(row.clone()),
            },
            _ => output.push(row.clone()),
        }
    }
    flush(&mut run, &mut output);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(lines: &[&str]) -> Vec<Vec<String>> {
        parse_expected_rows(
            &lines
                .iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn ids_names_and_subquery_order_do_not_matter() {
        let legacy = rows(&[
            "1|0|0|LIST SUBQUERY 1",
            "4|1|0|SCAN users",
            "13|0|0|SEARCH u USING INDEX ephemeral_users_t3 (age=?)",
            "29|0|0|USE SORTER FOR ORDER BY",
        ]);
        let hir = rows(&[
            "1|0|0|SEARCH u USING INDEX ephemeral_users_s1 (age=?)",
            "5|0|0|LIST SUBQUERY 1",
            "8|5|0|SCAN users",
            "30|0|0|USE SORTER FOR ORDER BY",
        ]);
        assert_eq!(normalize_rows(&legacy), normalize_rows(&hir));
        assert_eq!(
            normalize_rows(&hir),
            rows(&[
                "1|0|0|SEARCH u USING INDEX ephemeral_users_N (age=?)",
                "2|0|0|USE SORTER FOR ORDER BY",
                "3|0|0|LIST SUBQUERY N",
                "4|3|0|SCAN users",
            ])
        );
    }

    #[test]
    fn data_rows_that_only_look_like_plan_rows_stay_as_they_are() {
        let data = rows(&["-1|1|goodbye", "0|1|world", "42|2|hello | duplicate"]);
        assert_eq!(normalize_rows(&data), data);
    }

    #[test]
    fn a_different_loop_order_still_differs() {
        let a = rows(&["1|0|0|SCAN t", "2|0|0|SCAN u"]);
        let b = rows(&["1|0|0|SCAN u", "2|0|0|SCAN t"]);
        assert_ne!(normalize_rows(&a), normalize_rows(&b));
    }

    #[test]
    fn two_plans_in_one_result_are_split_where_the_ids_restart() {
        let two = rows(&["1|0|0|SCAN t", "7|0|0|SCAN u", "1|0|0|SCAN v"]);
        assert_eq!(
            normalize_rows(&two),
            rows(&["1|0|0|SCAN t", "2|0|0|SCAN u", "1|0|0|SCAN v"])
        );
    }

    #[test]
    fn json_plans_are_renumbered_with_their_cte_materializations() {
        let plan = r#"{"version":1,"sql":"x","nodes":[{"id":2,"parent":null,"detail":"SCAN orders"},{"id":41,"parent":null,"detail":"SEARCH s USING INDEX ephemeral_subquery_t3 (a=?)"}],"cte_materializations":[{"cte_id":0,"name":"s","nodes":[2]}]}"#;
        let normalized = normalize_json_plan(plan).unwrap();
        assert_eq!(
            normalized,
            r#"{"cte_materializations":[{"cte_id":0,"name":"s","nodes":[1]}],"nodes":[{"detail":"SCAN orders","id":1,"parent":null},{"detail":"SEARCH s USING INDEX ephemeral_subquery_N (a=?)","id":2,"parent":null}],"sql":"x","version":1}"#
        );
    }
}
