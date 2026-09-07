//! Shared contract tests for query-side `seek_danger` implementations.
//!
//! When adding an override, add its query/scorer to `Kind`, `IMPLEMENTATIONS`, and
//! `Expr::query` (or `scorer`). Test it both alone and nested: a miss may leave a
//! child invalid even when its parent returns Found.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::OnceLock;

use proptest::prelude::*;

use super::*;
use crate::docset::SeekDangerResult;
use crate::schema::{Field, IndexRecordOption, Schema, FAST, TEXT};
use crate::{DocId, DocSet, Index, Searcher, Term, TERMINATED};

#[derive(Clone, Copy, Debug)]
enum Kind {
    Intersection,
    BufferedUnion,
    Boost,
    ConstScore,
    RequiredOptional,
    Phrase,
    PhrasePrefix,
    FastFieldRange,
    ScorerWrapper,
}

// One entry per override, not merely per source file. The inventory test below
// also catches a second override added to an already covered file.
// An empty path denotes a query using the default implementation.
macro_rules! implementations {
    ($(($path:literal, $kind:ident, $test:ident)),* $(,)?) => {
        const IMPLEMENTATIONS: &[(&str, Kind)] = &[$(($path, Kind::$kind)),*];
        $(proptest! {
            #![proptest_config(ProptestConfig::with_cases(32))]
            #[test]
            fn $test(
                left in expressions(),
                right in expressions(),
                random_targets in prop::collection::vec(0u32..12000, 0..80),
                scoring in any::<bool>(),
            ) {
                let mut targets = random_targets;
                targets.extend([0, 1, 4095, 4096, 4097, 6000, 10200, 11000, 11010, 11101, 11102, TERMINATED]);
                targets.sort_unstable();
                targets.dedup();
                // Exercise the implementation with simple, nonempty children
                // as well as with randomly nested queries.
                for (left, right) in [(Expr::Term(2, false), Expr::Term(2, true)), (left, right)] {
                    let expr = Expr::Node(Kind::$kind, Box::new(left), Box::new(right));
                    check(|| scorer(&expr, scoring), &targets);
                }
            }
        })*
    };
}

implementations! {
    ("intersection.rs", Intersection, query_seek_danger_intersection),
    ("", BufferedUnion, query_seek_danger_buffered_union),
    ("boost_query.rs", Boost, query_seek_danger_boost),
    ("", ConstScore, query_seek_danger_const_score),
    ("reqopt_scorer.rs", RequiredOptional, query_seek_danger_required_optional),
    ("phrase_query/phrase_scorer.rs", Phrase, query_seek_danger_phrase),
    ("phrase_prefix_query/phrase_prefix_scorer.rs", PhrasePrefix, query_seek_danger_phrase_prefix),
    ("", FastFieldRange, query_seek_danger_fast_field_range),
    ("disjunction.rs", ScorerWrapper, query_seek_danger_scorer_wrapper),
}

#[derive(Clone, Debug)]
enum Expr {
    Term(usize, bool),
    Node(Kind, Box<Expr>, Box<Expr>),
}

impl Expr {
    fn query(&self, fields: &[Field; 3], number: Field) -> Box<dyn Query> {
        let term =
            |field, order| Term::from_field_text(field, if order { "order" } else { "service" });
        let Expr::Node(kind, left, right) = self else {
            let Expr::Term(field, order) = self else {
                unreachable!()
            };
            return Box::new(TermQuery::new(
                term(fields[*field], *order),
                IndexRecordOption::WithFreqs,
            ));
        };
        let left = left.query(fields, number);
        let right = right.query(fields, number);
        match kind {
            Kind::Intersection => Box::new(BooleanQuery::intersection(vec![left, right])),
            Kind::BufferedUnion => Box::new(DisjunctionMaxQuery::with_tie_breaker(
                vec![left, right],
                0.3,
            )),
            Kind::Boost => Box::new(BoostQuery::new(left, 2.0)),
            Kind::ConstScore => Box::new(ConstScoreQuery::new(left, 3.0)),
            Kind::RequiredOptional => Box::new(BooleanQuery::new(vec![
                (Occur::Must, left),
                (Occur::Should, right),
            ])),
            Kind::Phrase => Box::new(PhraseQuery::new(vec![
                term(fields[2], false),
                term(fields[2], true),
            ])),
            Kind::PhrasePrefix => Box::new(PhrasePrefixQuery::new(vec![
                term(fields[2], false),
                Term::from_field_text(fields[2], "ord"),
            ])),
            Kind::FastFieldRange => Box::new(RangeQuery::new(
                Bound::Included(Term::from_field_u64(number, 3)),
                Bound::Excluded(Term::from_field_u64(number, 8)),
            )),
            // ScorerWrapper is private to Disjunction and its override is not
            // called by Disjunction's normal traversal. Exercise it directly.
            Kind::ScorerWrapper => left,
        }
    }
}

fn expressions() -> impl Strategy<Value = Expr> {
    (0usize..3, any::<bool>())
        .prop_map(|(field, order)| Expr::Term(field, order))
        .prop_recursive(3, 16, 2, |inner| {
            // ScorerWrapper is exercised directly as a root, not a Query.
            let kinds: Vec<_> = IMPLEMENTATIONS
                .iter()
                .map(|&(_, kind)| kind)
                .filter(|kind| !matches!(kind, Kind::ScorerWrapper))
                .collect();
            (prop::sample::select(kinds), inner.clone(), inner)
                .prop_map(|(kind, left, right)| Expr::Node(kind, Box::new(left), Box::new(right)))
        })
}

struct Fixture {
    searcher: Searcher,
    fields: [Field; 3],
    number: Field,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let mut schema = Schema::builder();
        let fields = [
            schema.add_text_field("content", TEXT),
            schema.add_text_field("title", TEXT),
            schema.add_text_field("mixed", TEXT),
        ];
        let number = schema.add_u64_field("number", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads::<crate::TantivyDocument>(1, 50_000_000).unwrap();
        for id in 0..11102u64 {
            // Preserve the issue #3086 layout in content/title. A third field
            // supplies dense matches, phrase misses, and prefix expansions.
            let (content, title) = match id {
                0 | 10200 | 11101 => ("service order", ""),
                6000 | 11000 => ("nothing here", "service order"),
                11010 => ("purchase order", ""),
                _ => ("service team meeting", ""),
            };
            let mixed = ["service order", "order service", "service ordering", "service", "order", "nothing"][(id % 6) as usize];
            writer.add_document(doc!(fields[0] => content, fields[1] => title, fields[2] => mixed, number => id % 17)).unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        Fixture { searcher, fields, number }
    })
}

fn scorer(expr: &Expr, scoring: bool) -> Box<dyn Scorer> {
    let fixture = fixture();
    let query = expr.query(&fixture.fields, fixture.number);
    let enable = if scoring {
        EnableScoring::enabled_from_searcher(&fixture.searcher)
    } else {
        EnableScoring::disabled_from_searcher(&fixture.searcher)
    };
    let scorer = query
        .weight(enable)
        .unwrap()
        .scorer(fixture.searcher.segment_reader(0), 1.0)
        .unwrap();
    if matches!(expr, Expr::Node(Kind::ScorerWrapper, _, _)) {
        // ConstScorer does not forward seek_danger in 0.26, so use the
        // test-only Scorer implementation to exercise ScorerWrapper directly.
        Box::new(super::disjunction::seek_danger_test_wrapper(scorer))
    } else {
        scorer
    }
}

/// The oracle uses only advance. After a miss, the tested scorer receives ONLY
/// seek_danger, with strictly increasing targets; even doc() is forbidden.
fn check(mut make: impl FnMut() -> Box<dyn Scorer>, targets: &[DocId]) {
    let mut reference = make();
    let mut expected = Vec::new();
    while reference.doc() != TERMINATED {
        expected.push((reference.doc(), reference.score()));
        reference.advance();
    }
    let mut tested = make();
    let mut lower_bound = tested.doc();
    let mut previous_target = None;
    for &requested_target in targets {
        // Respect the returned lower bound, just like an intersection driver.
        // In particular, do not probe behind a scorer that has sought ahead.
        let target = requested_target.max(lower_bound);
        if previous_target == Some(target) {
            continue;
        }
        previous_target = Some(target);
        let next = expected.partition_point(|&(doc, _)| doc < target);
        let next_doc = expected.get(next).map_or(TERMINATED, |&(doc, _)| doc);
        // Exercise both forwarding implementations in docset.rs as well:
        // &mut dyn DocSet -> Box<dyn Scorer> -> concrete scorer.
        let mut borrowed: &mut dyn DocSet = &mut tested;
        match DocSet::seek_danger(&mut borrowed, target) {
            SeekDangerResult::Found => {
                assert_ne!(target, TERMINATED);
                assert_eq!(next_doc, target, "false positive at {target}");
                assert_eq!(tested.doc(), target);
                let score = tested.score();
                let expected_score = expected[next].1;
                assert!(
                    (score - expected_score).abs() <= 1e-5 * expected_score.abs().max(1.0),
                    "score at {target}: {score} != {expected_score}"
                );
            }
            SeekDangerResult::SeekLowerBound(bound) => {
                assert!(
                    target == TERMINATED || bound > target,
                    "non-increasing bound {bound} at {target}"
                );
                assert!(bound <= next_doc, "bound {bound} skips match {next_doc}");
                assert!(
                    next_doc != target || target == TERMINATED,
                    "missed match {target}"
                );
                lower_bound = bound;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn buffered_union_invalid_child_regression(offset in 0u32..1000, extra_gap in 0u32..1000) {
        // Same state transitions as #3086, without indexing 11k documents per
        // case. Both seeks are beyond the buffered horizon. The first child
        // misses the second target and leaves a one-term-only doc in the new
        // horizon; the second child finds the target.
        let first = offset + 6000;
        let second = first + 5000 + extra_gap;
        let false_match = second + 10;
        let last = second + 101;
        check(|| {
            let intersection = |a, b| -> Box<dyn Scorer> {
                Box::new(Intersection::new(vec![
                    ConstScorer::new(VecDocSet::from(a), 1.0),
                    ConstScorer::new(VecDocSet::from(b), 1.0),
                ], last + 3))
            };
            Box::new(BufferedUnionScorer::build(vec![
                intersection(vec![offset, first + 4200, false_match, last], vec![offset, first + 4200, last, last + 1, last + 2]),
                intersection(vec![first, second], vec![first, second]),
            ], SumCombiner::default, last + 3))
        }, &[first, second, false_match, last, TERMINATED]);
    }
}

#[test]
fn seek_danger_implementation_inventory() {
    fn visit(root: &Path, dir: &Path, pattern: &regex::Regex, found: &mut BTreeMap<String, usize>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, pattern, found);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let source = std::fs::read_to_string(&path).unwrap();
                let count = pattern.find_iter(&source).count();
                if count != 0 {
                    found.insert(
                        path.strip_prefix(root)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .replace('\\', "/"),
                        count,
                    );
                }
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = BTreeMap::new();
    let pattern = regex::Regex::new(r"fn\s+seek_danger\s*\(").unwrap();
    visit(&root, &root, &pattern, &mut found);
    let mut covered = BTreeMap::new();
    for &(path, _) in IMPLEMENTATIONS {
        if !path.is_empty() {
            *covered.entry(format!("query/{path}")).or_insert(0) += 1;
        }
    }
    // Trait default (used by TermScorer) and the Box / &mut forwarding impls.
    covered.insert("docset.rs".to_string(), 3);
    assert_eq!(found, covered, "Register new seek_danger implementations in seek_danger_tests.rs and add their query/scorer to the generator");
}
