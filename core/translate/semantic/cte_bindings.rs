//! CTE name environments and lazy definition state.

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use turso_parser::ast;

use super::{
    cte_rules::{RecursiveRefCounter, RecursiveRefScope},
    hir::{self, CteId, SourceId},
    scope::{QueryEnvironment, Scope},
};
use crate::Result;

/// CTE names visible at one point in query analysis.
///
/// Each WITH clause is one immutable frame. A binding returned from a frame
/// carries that frame with it, so delayed analysis uses the names visible
/// where the CTE was declared rather than names added around a later use.
/// Creating a frame only records syntax and shared state; unused bodies are
/// still never analyzed or assigned an arena identity.
#[derive(Clone, Debug, Default)]
pub(crate) struct CteBindings {
    frame: Option<Rc<CteBindingFrame>>,
}

impl CteBindings {
    pub(crate) fn with_clause(
        &self,
        with: Option<&ast::With>,
        enclosing_scope: Option<Scope>,
    ) -> Result<Self> {
        let Some(with) = with else {
            return Ok(self.clone());
        };

        let mut local_names = HashSet::with_capacity(with.ctes.len());
        let mut definitions = Vec::with_capacity(with.ctes.len());
        for syntax in &with.ctes {
            let name = crate::util::normalize_ident(syntax.tbl_name.as_str());
            if !local_names.insert(name.clone()) {
                crate::bail_parse_error!("duplicate WITH table name: {}", syntax.tbl_name);
            }
            definitions.push(Rc::new(CteDefinition {
                name,
                syntax: Rc::new(syntax.clone()),
                state: Rc::new(RefCell::new(CteState::Unseen)),
            }));
        }
        Ok(Self {
            frame: Some(Rc::new(CteBindingFrame {
                outer: self.clone(),
                enclosing_scope,
                definitions,
            })),
        })
    }

    pub(crate) fn find(&self, name: &str) -> Option<CteBinding> {
        let normalized = crate::util::normalize_ident(name);
        let mut visible = self.clone();
        loop {
            let frame = visible.frame.clone()?;
            if let Some(definition) = frame
                .definitions
                .iter()
                .rev()
                .find(|definition| definition.name == normalized)
            {
                return Some(CteBinding {
                    definition: definition.clone(),
                    definition_ctes: visible,
                    enclosing_scope: frame.enclosing_scope.clone(),
                });
            }
            visible = frame.outer.clone();
        }
    }

    /// Return the visible CTEs that this query can reach through table
    /// references. Unreferenced CTE bodies remain dormant.
    fn dependencies_of(&self, select: &ast::Select) -> Vec<CteBinding> {
        self.visible_bindings()
            .into_iter()
            .filter(|binding| {
                let counter = RecursiveRefCounter {
                    cte_name: binding.name(),
                    count_table_calls: false,
                };
                counter.count_select(select, &mut RecursiveRefScope::new()) > 0
            })
            .collect()
    }

    fn visible_bindings(&self) -> Vec<CteBinding> {
        let mut names = HashSet::new();
        let mut bindings = Vec::new();
        let mut visible = self.clone();
        while let Some(frame) = visible.frame.clone() {
            for definition in frame.definitions.iter().rev() {
                if names.insert(definition.name.clone()) {
                    bindings.push(CteBinding {
                        definition: definition.clone(),
                        definition_ctes: visible.clone(),
                        enclosing_scope: frame.enclosing_scope.clone(),
                    });
                }
            }
            visible = frame.outer.clone();
        }
        bindings.reverse();
        bindings
    }
}

#[derive(Debug)]
struct CteBindingFrame {
    outer: CteBindings,
    /// The query scope outside the SELECT that declared this WITH clause.
    /// Lazy analysis must retain this lexical scope without inheriting a
    /// deeper scope from whichever reference happens to demand the CTE.
    enclosing_scope: Option<Scope>,
    definitions: Vec<Rc<CteDefinition>>,
}

#[derive(Debug)]
struct CteDefinition {
    name: String,
    syntax: Rc<ast::CommonTableExpr>,
    state: Rc<RefCell<CteState>>,
}

/// One CTE definition together with its declaration-site name environment.
#[derive(Clone, Debug)]
pub(crate) struct CteBinding {
    definition: Rc<CteDefinition>,
    definition_ctes: CteBindings,
    enclosing_scope: Option<Scope>,
}

impl CteBinding {
    pub(crate) fn name(&self) -> &str {
        &self.definition.name
    }

    pub(crate) fn syntax(&self) -> &ast::CommonTableExpr {
        self.definition.syntax.as_ref()
    }

    pub(super) fn state(&self) -> CteState {
        self.definition.state.borrow().clone()
    }

    pub(super) fn set_state(&self, state: CteState) {
        *self.definition.state.borrow_mut() = state;
    }

    pub(super) fn is_same_definition(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.definition, &other.definition)
    }

    pub(super) fn dependencies(&self) -> Vec<CteBinding> {
        self.definition_ctes.dependencies_of(&self.syntax().select)
    }

    pub(super) fn query_environment(
        &self,
        reference_environment: &QueryEnvironment,
    ) -> QueryEnvironment {
        // Statement-wide permissions come from the use site, but SQL names
        // come from the frame in which this definition was declared.
        let mut environment = cte_query_environment(reference_environment);
        environment.ctes = self.definition_ctes.clone();
        environment.outer = self.enclosing_scope.clone();
        environment.query = self.enclosing_scope.as_ref().and_then(Scope::query);
        environment
    }
}

#[derive(Clone, Debug)]
pub(super) enum CteState {
    Unseen,
    Analyzing {
        id: CteId,
        recursive_columns: Option<Vec<hir::CteColumn>>,
        recursive_inputs: Vec<SourceId>,
    },
    Complete(CteId),
    Failed(String),
}

pub(super) fn cte_query_environment(environment: &QueryEnvironment) -> QueryEnvironment {
    let mut environment = environment.clone();
    environment.expected_output_types.clear();
    environment.expected_defaults.clear();
    environment
}
