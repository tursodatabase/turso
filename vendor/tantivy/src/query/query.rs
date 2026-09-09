use std::fmt;

use downcast_rs::impl_downcast;

use super::bm25::Bm25StatisticsProvider;
use super::Weight;
use crate::core::searcher::Searcher;
use crate::query::Explanation;
use crate::schema::Schema;
use crate::{DocAddress, Term};

/// Argument used in `Query::weight(..)`
#[derive(Copy, Clone)]
pub enum EnableScoring<'a> {
    /// Pass this to enable scoring.
    Enabled {
        /// The searcher to use during scoring.
        searcher: &'a Searcher,

        /// A [Bm25StatisticsProvider] used to compute BM25 scores.
        ///
        /// Normally this should be the [Searcher], but you can specify a custom
        /// one to adjust the statistics.
        statistics_provider: &'a dyn Bm25StatisticsProvider,
    },
    /// Pass this to disable scoring.
    /// This can improve performance.
    Disabled {
        /// Schema is required.
        schema: &'a Schema,
        /// Searcher should be provided if available.
        searcher_opt: Option<&'a Searcher>,
    },
}

impl<'a> EnableScoring<'a> {
    /// Create using [Searcher] with scoring enabled.
    pub fn enabled_from_searcher(searcher: &'a Searcher) -> EnableScoring<'a> {
        EnableScoring::Enabled {
            searcher,
            statistics_provider: searcher,
        }
    }

    /// Create using a custom [Bm25StatisticsProvider] with scoring enabled.
    pub fn enabled_from_statistics_provider(
        statistics_provider: &'a dyn Bm25StatisticsProvider,
        searcher: &'a Searcher,
    ) -> EnableScoring<'a> {
        EnableScoring::Enabled {
            statistics_provider,
            searcher,
        }
    }

    /// Create using [Searcher] with scoring disabled.
    pub fn disabled_from_searcher(searcher: &'a Searcher) -> EnableScoring<'a> {
        EnableScoring::Disabled {
            schema: searcher.schema(),
            searcher_opt: Some(searcher),
        }
    }

    /// Create using [Schema] with scoring disabled.
    pub fn disabled_from_schema(schema: &'a Schema) -> EnableScoring<'a> {
        Self::Disabled {
            schema,
            searcher_opt: None,
        }
    }

    /// Returns the searcher if available.
    pub fn searcher(&self) -> Option<&Searcher> {
        match self {
            EnableScoring::Enabled { searcher, .. } => Some(*searcher),
            EnableScoring::Disabled { searcher_opt, .. } => searcher_opt.to_owned(),
        }
    }

    /// Returns the schema.
    pub fn schema(&self) -> &Schema {
        match self {
            EnableScoring::Enabled { searcher, .. } => searcher.schema(),
            EnableScoring::Disabled { schema, .. } => schema,
        }
    }

    /// Returns true if the scoring is enabled.
    pub fn is_scoring_enabled(&self) -> bool {
        matches!(self, EnableScoring::Enabled { .. })
    }
}

/// The `Query` trait defines a set of documents and a scoring method
/// for those documents.
///
/// The `Query` trait is in charge of defining :
///
/// - a set of documents
/// - a way to score these documents
///
/// When performing a [search](Searcher::search), these documents will then
/// be pushed to a [`Collector`](crate::collector::Collector),
/// which will in turn be in charge of deciding what to do with them.
///
/// Concretely, this scored docset is represented by the
/// [`Scorer`] trait.
///
/// Because our index is actually split into segments, the
/// query does not actually directly creates [`DocSet`](crate::DocSet) object.
/// Instead, the query creates a [`Weight`] object for a given searcher.
///
/// The weight object, in turn, makes it possible to create
/// a scorer for a specific [`SegmentReader`].
///
/// So to sum it up :
/// - a `Query` is a recipe to define a set of documents as well the way to score them.
/// - a [`Weight`] is this recipe tied to a specific [`Searcher`]. It may for instance hold
///   statistics about the different term of the query. It is created by the query.
/// - a [`Scorer`] is a cursor over the set of matching documents, for a specific [`SegmentReader`].
///   It is created by the [`Weight`].
///
/// When implementing a new type of `Query`, it is normal to implement a
/// dedicated `Query`, [`Weight`] and [`Scorer`].
///
/// [`Scorer`]: crate::query::Scorer
/// [`SegmentReader`]: crate::SegmentReader
pub trait Query: QueryClone + Send + Sync + downcast_rs::Downcast + fmt::Debug {
    /// Create the weight associated with a query.
    ///
    /// If scoring is not required, setting `scoring_enabled` to `false`
    /// can increase performances.
    ///
    /// See [`Weight`].
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>>;

    /// Returns an `Explanation` for the score of the document.
    fn explain(&self, searcher: &Searcher, doc_address: DocAddress) -> crate::Result<Explanation> {
        let weight = self.weight(EnableScoring::enabled_from_searcher(searcher))?;
        let reader = searcher.segment_reader(doc_address.segment_ord);
        weight.explain(reader, doc_address.doc_id)
    }

    /// Returns the number of documents matching the query.
    fn count(&self, searcher: &Searcher) -> crate::Result<usize> {
        let weight = self.weight(EnableScoring::disabled_from_searcher(searcher))?;
        let mut result = 0;
        for reader in searcher.segment_readers() {
            result += weight.count(reader)? as usize;
        }
        Ok(result)
    }

    /// Extract all of the terms associated with the query and pass them to the
    /// given closure.
    ///
    /// Each term is associated with a boolean indicating whether
    /// positions are required or not.
    ///
    /// Note that there can be multiple instances of any given term
    /// in a query and deduplication must be handled by the visitor.
    fn query_terms<'a>(&'a self, _visitor: &mut dyn FnMut(&'a Term, bool)) {}
}

/// Implements `box_clone`.
pub trait QueryClone {
    /// Returns a boxed clone of `self`.
    fn box_clone(&self) -> Box<dyn Query>;
}

impl<T> QueryClone for T
where T: 'static + Query + Clone
{
    fn box_clone(&self) -> Box<dyn Query> {
        Box::new(self.clone())
    }
}

impl Query for Box<dyn Query> {
    fn weight(&self, enabled_scoring: EnableScoring) -> crate::Result<Box<dyn Weight>> {
        self.as_ref().weight(enabled_scoring)
    }

    fn count(&self, searcher: &Searcher) -> crate::Result<usize> {
        self.as_ref().count(searcher)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.as_ref().query_terms(visitor);
    }
}

impl QueryClone for Box<dyn Query> {
    fn box_clone(&self) -> Box<dyn Query> {
        self.as_ref().box_clone()
    }
}

impl_downcast!(Query);
