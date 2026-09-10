use std::collections::HashMap;

use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::index::SegmentReader;
use crate::postings::FreqReadingOption;
use crate::query::disjunction::Disjunction;
use crate::query::explanation::does_not_match;
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::term_query::TermScorer;
use crate::query::weight::{for_each_docset_buffered, for_each_pruning_scorer, for_each_scorer};
use crate::query::{
    intersect_scorers, AllScorer, BufferedUnionScorer, EmptyScorer, Exclude, Explanation, Occur,
    RequiredOptionalScorer, Scorer, Weight,
};
use crate::{DocId, Score};

enum SpecializedScorer {
    TermUnion(Vec<TermScorer>),
    Other(Box<dyn Scorer>),
}

fn scorer_disjunction<TScoreCombiner>(
    scorers: Vec<Box<dyn Scorer>>,
    score_combiner: TScoreCombiner,
    minimum_match_required: usize,
) -> Box<dyn Scorer>
where
    TScoreCombiner: ScoreCombiner,
{
    debug_assert!(!scorers.is_empty());
    debug_assert!(minimum_match_required > 1);
    if scorers.len() == 1 {
        return scorers.into_iter().next().unwrap(); // Safe unwrap.
    }
    Box::new(Disjunction::new(
        scorers,
        score_combiner,
        minimum_match_required,
    ))
}

/// num_docs is the number of documents in the segment.
fn scorer_union<TScoreCombiner>(
    scorers: Vec<Box<dyn Scorer>>,
    score_combiner_fn: impl Fn() -> TScoreCombiner,
    num_docs: u32,
) -> SpecializedScorer
where
    TScoreCombiner: ScoreCombiner,
{
    assert!(!scorers.is_empty());
    if scorers.len() == 1 {
        return SpecializedScorer::Other(scorers.into_iter().next().unwrap()); //< we checked the size beforehand
    }

    {
        let is_all_term_queries = scorers.iter().all(|scorer| scorer.is::<TermScorer>());
        if is_all_term_queries {
            let scorers: Vec<TermScorer> = scorers
                .into_iter()
                .map(|scorer| *(scorer.downcast::<TermScorer>().map_err(|_| ()).unwrap()))
                .collect();
            if scorers
                .iter()
                .all(|scorer| scorer.freq_reading_option() == FreqReadingOption::ReadFreq)
            {
                // Block wand is only available if we read frequencies.
                return SpecializedScorer::TermUnion(scorers);
            } else {
                return SpecializedScorer::Other(Box::new(BufferedUnionScorer::build(
                    scorers,
                    score_combiner_fn,
                    num_docs,
                )));
            }
        }
    }
    SpecializedScorer::Other(Box::new(BufferedUnionScorer::build(
        scorers,
        score_combiner_fn,
        num_docs,
    )))
}

fn into_box_scorer<TScoreCombiner: ScoreCombiner>(
    scorer: SpecializedScorer,
    score_combiner_fn: impl Fn() -> TScoreCombiner,
    num_docs: u32,
) -> Box<dyn Scorer> {
    match scorer {
        SpecializedScorer::TermUnion(term_scorers) => {
            let union_scorer =
                BufferedUnionScorer::build(term_scorers, score_combiner_fn, num_docs);
            Box::new(union_scorer)
        }
        SpecializedScorer::Other(scorer) => scorer,
    }
}

/// Returns the effective MUST scorer, accounting for removed AllScorers.
///
/// When AllScorer instances are removed from must_scorers as an optimization,
/// we must restore the "match all" semantics if the list becomes empty.
fn effective_must_scorer(
    must_scorers: Vec<Box<dyn Scorer>>,
    removed_all_scorer_count: usize,
    max_doc: DocId,
    num_docs: u32,
) -> Option<Box<dyn Scorer>> {
    if must_scorers.is_empty() {
        if removed_all_scorer_count > 0 {
            // Had AllScorer(s) only - all docs match
            Some(Box::new(AllScorer::new(max_doc)))
        } else {
            // No MUST constraint at all
            None
        }
    } else {
        Some(intersect_scorers(must_scorers, num_docs))
    }
}

/// Returns a SHOULD scorer with AllScorer union if any were removed.
///
/// For union semantics (OR): if any SHOULD clause was an AllScorer, the result
/// should include all documents. We restore this by unioning with AllScorer.
///
/// When `scoring_enabled` is false, we can just return AllScorer alone since
/// we don't need score contributions from the should_scorer.
fn effective_should_scorer_for_union<TScoreCombiner: ScoreCombiner>(
    should_scorer: SpecializedScorer,
    removed_all_scorer_count: usize,
    max_doc: DocId,
    num_docs: u32,
    score_combiner_fn: impl Fn() -> TScoreCombiner,
    scoring_enabled: bool,
) -> SpecializedScorer {
    if removed_all_scorer_count > 0 {
        if scoring_enabled {
            // Need to union to get score contributions from both
            let all_scorers: Vec<Box<dyn Scorer>> = vec![
                into_box_scorer(should_scorer, &score_combiner_fn, num_docs),
                Box::new(AllScorer::new(max_doc)),
            ];
            SpecializedScorer::Other(Box::new(BufferedUnionScorer::build(
                all_scorers,
                score_combiner_fn,
                num_docs,
            )))
        } else {
            // Scoring disabled - AllScorer alone is sufficient
            SpecializedScorer::Other(Box::new(AllScorer::new(max_doc)))
        }
    } else {
        should_scorer
    }
}

enum ShouldScorersCombinationMethod {
    // Should scorers are irrelevant.
    Ignored,
    // Only contributes to final score.
    Optional(SpecializedScorer),
    // Regardless of score, the should scorers may impact whether a document is matching or not.
    Required(SpecializedScorer),
}

/// Weight associated to the `BoolQuery`.
pub struct BooleanWeight<TScoreCombiner: ScoreCombiner> {
    weights: Vec<(Occur, Box<dyn Weight>)>,
    minimum_number_should_match: usize,
    scoring_enabled: bool,
    score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send>,
}

impl<TScoreCombiner: ScoreCombiner> BooleanWeight<TScoreCombiner> {
    /// Creates a new boolean weight.
    pub fn new(
        weights: Vec<(Occur, Box<dyn Weight>)>,
        scoring_enabled: bool,
        score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send + 'static>,
    ) -> BooleanWeight<TScoreCombiner> {
        BooleanWeight {
            weights,
            scoring_enabled,
            score_combiner_fn,
            minimum_number_should_match: 1,
        }
    }

    /// Create a new boolean weight with minimum number of required should clauses specified.
    pub fn with_minimum_number_should_match(
        weights: Vec<(Occur, Box<dyn Weight>)>,
        minimum_number_should_match: usize,
        scoring_enabled: bool,
        score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send + 'static>,
    ) -> BooleanWeight<TScoreCombiner> {
        BooleanWeight {
            weights,
            minimum_number_should_match,
            scoring_enabled,
            score_combiner_fn,
        }
    }

    fn per_occur_scorers(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<HashMap<Occur, Vec<Box<dyn Scorer>>>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<dyn Scorer>>> = HashMap::new();
        for (occur, subweight) in &self.weights {
            let sub_scorer: Box<dyn Scorer> = subweight.scorer(reader, boost)?;
            per_occur_scorers
                .entry(*occur)
                .or_default()
                .push(sub_scorer);
        }
        Ok(per_occur_scorers)
    }

    fn complex_scorer<TComplexScoreCombiner: ScoreCombiner>(
        &self,
        reader: &SegmentReader,
        boost: Score,
        score_combiner_fn: impl Fn() -> TComplexScoreCombiner,
    ) -> crate::Result<SpecializedScorer> {
        let num_docs = reader.num_docs();
        let mut per_occur_scorers = self.per_occur_scorers(reader, boost)?;

        // Indicate how should clauses are combined with must clauses.
        let mut must_scorers: Vec<Box<dyn Scorer>> =
            per_occur_scorers.remove(&Occur::Must).unwrap_or_default();
        let must_special_scorer_counts = remove_and_count_all_and_empty_scorers(&mut must_scorers);

        if must_special_scorer_counts.num_empty_scorers > 0 {
            return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
        }

        let mut should_scorers = per_occur_scorers.remove(&Occur::Should).unwrap_or_default();
        let should_special_scorer_counts =
            remove_and_count_all_and_empty_scorers(&mut should_scorers);

        let mut exclude_scorers: Vec<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .unwrap_or_default();
        let exclude_special_scorer_counts =
            remove_and_count_all_and_empty_scorers(&mut exclude_scorers);

        if exclude_special_scorer_counts.num_all_scorers > 0 {
            // We exclude all documents at one point.
            return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
        }

        let effective_minimum_number_should_match = self
            .minimum_number_should_match
            .saturating_sub(should_special_scorer_counts.num_all_scorers);

        let should_scorers: ShouldScorersCombinationMethod = {
            let num_of_should_scorers = should_scorers.len();
            if effective_minimum_number_should_match > num_of_should_scorers {
                // We don't have enough scorers to satisfy the minimum number of should matches.
                // The request will match no documents.
                return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
            }
            match effective_minimum_number_should_match {
                0 if num_of_should_scorers == 0 => ShouldScorersCombinationMethod::Ignored,
                0 => ShouldScorersCombinationMethod::Optional(scorer_union(
                    should_scorers,
                    &score_combiner_fn,
                    num_docs,
                )),
                1 => ShouldScorersCombinationMethod::Required(scorer_union(
                    should_scorers,
                    &score_combiner_fn,
                    num_docs,
                )),
                n if num_of_should_scorers == n => {
                    // When num_of_should_scorers equals the number of should clauses,
                    // they are no different from must clauses.
                    must_scorers.append(&mut should_scorers);
                    ShouldScorersCombinationMethod::Ignored
                }
                _ => ShouldScorersCombinationMethod::Required(SpecializedScorer::Other(
                    scorer_disjunction(
                        should_scorers,
                        score_combiner_fn(),
                        effective_minimum_number_should_match,
                    ),
                )),
            }
        };

        let include_scorer = match (should_scorers, must_scorers) {
            (ShouldScorersCombinationMethod::Ignored, must_scorers) => {
                // No SHOULD clauses (or they were absorbed into MUST).
                // Result depends entirely on MUST + any removed AllScorers.
                let combined_all_scorer_count = must_special_scorer_counts.num_all_scorers
                    + should_special_scorer_counts.num_all_scorers;
                let boxed_scorer: Box<dyn Scorer> = effective_must_scorer(
                    must_scorers,
                    combined_all_scorer_count,
                    reader.max_doc(),
                    num_docs,
                )
                .unwrap_or_else(|| Box::new(EmptyScorer));
                SpecializedScorer::Other(boxed_scorer)
            }
            (ShouldScorersCombinationMethod::Optional(should_scorer), must_scorers) => {
                // Optional SHOULD: contributes to scoring but not required for matching.
                match effective_must_scorer(
                    must_scorers,
                    must_special_scorer_counts.num_all_scorers,
                    reader.max_doc(),
                    num_docs,
                ) {
                    None => {
                        // No MUST constraint: promote SHOULD to required.
                        // Must preserve any removed AllScorers from SHOULD via union.
                        effective_should_scorer_for_union(
                            should_scorer,
                            should_special_scorer_counts.num_all_scorers,
                            reader.max_doc(),
                            num_docs,
                            &score_combiner_fn,
                            self.scoring_enabled,
                        )
                    }
                    Some(must_scorer) => {
                        // Has MUST constraint: SHOULD only affects scoring.
                        if self.scoring_enabled {
                            SpecializedScorer::Other(Box::new(RequiredOptionalScorer::<
                                _,
                                _,
                                TScoreCombiner,
                            >::new(
                                must_scorer,
                                into_box_scorer(should_scorer, &score_combiner_fn, num_docs),
                            )))
                        } else {
                            SpecializedScorer::Other(must_scorer)
                        }
                    }
                }
            }
            (ShouldScorersCombinationMethod::Required(should_scorer), must_scorers) => {
                // Required SHOULD: at least `minimum_number_should_match` must match.
                // Semantics: (MUST constraint) AND (SHOULD constraint)
                match effective_must_scorer(
                    must_scorers,
                    must_special_scorer_counts.num_all_scorers,
                    reader.max_doc(),
                    num_docs,
                ) {
                    None => {
                        // No MUST constraint: SHOULD alone determines matching.
                        should_scorer
                    }
                    Some(must_scorer) => {
                        // Has MUST constraint: intersect MUST with SHOULD.
                        let should_boxed =
                            into_box_scorer(should_scorer, &score_combiner_fn, num_docs);
                        SpecializedScorer::Other(intersect_scorers(
                            vec![must_scorer, should_boxed],
                            num_docs,
                        ))
                    }
                }
            }
        };
        if exclude_scorers.is_empty() {
            return Ok(include_scorer);
        }

        let include_scorer_boxed = into_box_scorer(include_scorer, &score_combiner_fn, num_docs);
        let scorer: Box<dyn Scorer> = if exclude_scorers.len() == 1 {
            let exclude_scorer = exclude_scorers.pop().unwrap();
            match exclude_scorer.downcast::<TermScorer>() {
                // Cast to TermScorer succeeded
                Ok(exclude_scorer) => Box::new(Exclude::new(include_scorer_boxed, *exclude_scorer)),
                // We get back the original Box<dyn Scorer>
                Err(exclude_scorer) => Box::new(Exclude::new(include_scorer_boxed, exclude_scorer)),
            }
        } else {
            Box::new(Exclude::new(include_scorer_boxed, exclude_scorers))
        };
        Ok(SpecializedScorer::Other(scorer))
    }
}

#[derive(Default, Copy, Clone, Debug)]
struct AllAndEmptyScorerCounts {
    num_all_scorers: usize,
    num_empty_scorers: usize,
}

fn remove_and_count_all_and_empty_scorers(
    scorers: &mut Vec<Box<dyn Scorer>>,
) -> AllAndEmptyScorerCounts {
    let mut counts = AllAndEmptyScorerCounts::default();
    scorers.retain(|scorer| {
        if scorer.is::<AllScorer>() {
            counts.num_all_scorers += 1;
            false
        } else if scorer.is::<EmptyScorer>() {
            counts.num_empty_scorers += 1;
            false
        } else {
            true
        }
    });
    counts
}

impl<TScoreCombiner: ScoreCombiner + Sync> Weight for BooleanWeight<TScoreCombiner> {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let num_docs = reader.num_docs();
        if self.weights.is_empty() {
            Ok(Box::new(EmptyScorer))
        } else if self.weights.len() == 1 {
            let &(occur, ref weight) = &self.weights[0];
            if occur == Occur::MustNot {
                Ok(Box::new(EmptyScorer))
            } else {
                weight.scorer(reader, boost)
            }
        } else if self.scoring_enabled {
            self.complex_scorer(reader, boost, &self.score_combiner_fn)
                .map(|specialized_scorer| {
                    into_box_scorer(specialized_scorer, &self.score_combiner_fn, num_docs)
                })
        } else {
            self.complex_scorer(reader, boost, DoNothingCombiner::default)
                .map(|specialized_scorer| {
                    into_box_scorer(specialized_scorer, DoNothingCombiner::default, num_docs)
                })
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        if !self.scoring_enabled {
            return Ok(Explanation::new("BooleanQuery with no scoring", 1.0));
        }

        let mut explanation = Explanation::new("BooleanClause. sum of ...", scorer.score());
        for (occur, subweight) in &self.weights {
            if is_include_occur(*occur) {
                if let Ok(child_explanation) = subweight.explain(reader, doc) {
                    explanation.add_detail(child_explanation);
                }
            }
        }
        Ok(explanation)
    }

    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score),
    ) -> crate::Result<()> {
        let scorer = self.complex_scorer(reader, 1.0, &self.score_combiner_fn)?;
        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer = BufferedUnionScorer::build(
                    term_scorers,
                    &self.score_combiner_fn,
                    reader.num_docs(),
                );
                for_each_scorer(&mut union_scorer, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_scorer(scorer.as_mut(), callback);
            }
        }
        Ok(())
    }

    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(&[DocId]),
    ) -> crate::Result<()> {
        let scorer = self.complex_scorer(reader, 1.0, || DoNothingCombiner)?;
        let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];

        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer = BufferedUnionScorer::build(
                    term_scorers,
                    &self.score_combiner_fn,
                    reader.num_docs(),
                );
                for_each_docset_buffered(&mut union_scorer, &mut buffer, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_docset_buffered(scorer.as_mut(), &mut buffer, callback);
            }
        }
        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the TopDocs collector.
    /// For all docsets, the blanket implementation has the benefit
    /// of prefiltering (doc, score) pairs, avoiding the
    /// virtual dispatch cost.
    ///
    /// More importantly, it makes it possible for scorers to implement
    /// important optimization (e.g. BlockWAND for union).
    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let scorer = self.complex_scorer(reader, 1.0, &self.score_combiner_fn)?;
        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                super::block_wand(term_scorers, threshold, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_pruning_scorer(scorer.as_mut(), threshold, callback);
            }
        }
        Ok(())
    }
}

fn is_include_occur(occur: Occur) -> bool {
    match occur {
        Occur::Must | Occur::Should => true,
        Occur::MustNot => false,
    }
}
