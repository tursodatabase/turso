//! The custom types of a schema, by name.

use super::{type_registry_key, ResolvedType, TypeDef, TypeOrigin, ValueKinds};
use crate::alloc::vec;
use crate::alloc::*;
use crate::sync::Arc;
use crate::{LimboError, Result};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

/// The types made by CREATE TYPE and CREATE DOMAIN, and the built-in types.
///
/// A type is looked up by [`type_registry_key`] of its name. Filling the
/// registry has two parts: `insert`, `add_from_sql` and `remove` change the
/// types, and `rebuild_chains` then works out the chain of every type once,
/// so that `resolve` does not walk it at each lookup.
#[derive(Debug, Default)]
pub struct TypeRegistry {
    /// An alias of a type is a second key for the same `TypeDef`.
    types: HashMap<String, Arc<TypeDef>>,
    /// The chain of each type down to the plain SQL type it is based on, as
    /// `rebuild_chains` built it. Empty after a change of `types`, and a type
    /// whose chain has a cycle is never in it: `resolve` then walks the chain.
    resolved: HashMap<String, Arc<ResolvedType>>,
}

impl TypeRegistry {
    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }

    /// The type named `type_name`, or None when no such type exists.
    pub fn get(&self, type_name: &str) -> Option<&Arc<TypeDef>> {
        self.types.get(&type_registry_key(type_name))
    }

    /// Every key with its type. A type with aliases is listed once per key.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Arc<TypeDef>)> {
        self.types.iter()
    }

    /// Every type once, without its aliases, in the order of the names.
    pub fn types_by_name(&self) -> std::vec::Vec<&Arc<TypeDef>> {
        let mut types: std::vec::Vec<(&String, &Arc<TypeDef>)> = self
            .types
            .iter()
            .filter(|(key, type_def)| **key == type_def.name.to_lowercase())
            .collect();
        types.sort_by_key(|(key, _)| *key);
        types.into_iter().map(|(_, type_def)| type_def).collect()
    }

    /// The type named `type_name` with its chain, or `Ok(None)` when no such
    /// type exists. Fails when the chain has a cycle.
    pub fn resolve(&self, type_name: &str) -> Result<Option<Arc<ResolvedType>>> {
        let key = type_registry_key(type_name);
        if let Some(resolved) = self.resolved.get(&key) {
            return Ok(Some(Arc::clone(resolved)));
        }
        if !self.types.contains_key(&key) {
            return Ok(None);
        }
        let (primitive, chain) = self.chain_from_key(key)?;
        Ok(Some(Arc::new(ResolvedType { primitive, chain })))
    }

    /// Adds `type_def` under `name`. A type can have several names. The kinds
    /// of `type_def` are not worked out here: a built-in type and a type taken
    /// from another schema have them already.
    pub fn insert(&mut self, name: &str, type_def: Arc<TypeDef>) {
        self.types.insert(type_registry_key(name), type_def);
        self.resolved.clear();
    }

    /// Parses a CREATE TYPE or CREATE DOMAIN statement and adds its type,
    /// without working out the kinds of value the type reads as and stores.
    pub(crate) fn add_from_sql(
        &mut self,
        dialect: &dyn crate::dialect::Dialect,
        sql: &str,
    ) -> Result<()> {
        use crate::ast::{Cmd, Stmt};

        let type_def = match crate::dialect::parse_statement(dialect, sql) {
            Ok(Some(Cmd::Stmt(Stmt::CreateType {
                type_name, body, ..
            }))) => {
                TypeDef::from_create_type(&type_name, &body, TypeOrigin::User, sql.to_string())?
            }
            Ok(Some(Cmd::Stmt(Stmt::CreateDomain {
                domain_name,
                base_type,
                default,
                not_null,
                constraints,
                ..
            }))) => TypeDef::from_domain(
                &domain_name,
                &base_type,
                not_null,
                &constraints,
                default,
                sql.to_string(),
            ),
            _ => {
                return Err(LimboError::ParseError(format!("invalid type sql: {sql}")));
            }
        };
        self.types
            .insert(type_registry_key(&type_def.name), Arc::new(type_def));
        self.resolved.clear();
        Ok(())
    }

    pub(crate) fn remove(&mut self, type_name: &str) {
        self.types.remove(&type_registry_key(type_name));
        self.rebuild_chains();
    }

    /// Works out the chain of every type. Call it after the last change of a
    /// batch of changes.
    pub(crate) fn rebuild_chains(&mut self) {
        let mut resolved = HashMap::default();
        for key in self.types.keys() {
            // `resolve` reports the cycle of a type that is left out here.
            if let Ok((primitive, chain)) = self.chain_from_key(key.clone()) {
                resolved.insert(key.clone(), Arc::new(ResolvedType { primitive, chain }));
            }
        }
        self.resolved = resolved;
    }

    /// The keys of the types whose kinds are still to be worked out, sorted.
    pub(super) fn keys_with_pending_kinds(&self) -> std::vec::Vec<String> {
        let mut keys: std::vec::Vec<String> = self
            .types
            .iter()
            .filter(|(_, type_def)| type_def.kinds_pending())
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort();
        keys
    }

    /// The type under `key`, which must exist.
    pub(super) fn under_key(&self, key: &str) -> &TypeDef {
        &self.types[key]
    }

    /// Sets the kinds of the type under `key`, which must exist.
    pub(super) fn set_kinds(&mut self, key: &str, kinds: ValueKinds) {
        let mut updated = (*self.types[key]).clone();
        updated.set_kinds(kinds);
        self.types.insert(key.to_string(), Arc::new(updated));
        self.resolved.clear();
    }

    /// Follows the base types of `type_name`: domain_a → domain_b → integer.
    /// Returns the plain SQL type at the end, and the types on the way from
    /// `type_name` to it. Fails when a type is based on itself through others.
    pub fn base_type_chain(&self, type_name: &str) -> Result<(String, Vec<Arc<TypeDef>>)> {
        self.chain_from_key(type_registry_key(type_name))
    }

    fn chain_from_key(&self, key: String) -> Result<(String, Vec<Arc<TypeDef>>)> {
        let mut chain = vec![];
        let mut visited = HashSet::default();
        let mut current = key;

        loop {
            if !visited.insert(current.clone()) {
                return Err(LimboError::ParseError(format!(
                    "circular type dependency detected: {current}"
                )));
            }
            match self.types.get(&current) {
                Some(type_def) => {
                    chain.try_push(Arc::clone(type_def))?;
                    current = type_registry_key(type_def.base());
                }
                // A name that is not a type is a plain SQL type.
                None => return Ok((current, chain)),
            }
        }
    }
}

impl TryClone for TypeRegistry {
    type Error = TryReserveError;

    fn try_clone(&self) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            types: self.types.try_clone()?,
            resolved: self.resolved.try_clone()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn domain(name: &str, base: &str) -> Arc<TypeDef> {
        Arc::new(TypeDef::from_domain(
            name,
            base,
            false,
            &[],
            None,
            String::new(),
        ))
    }

    fn chain_names(registry: &TypeRegistry, name: &str) -> (String, std::vec::Vec<String>) {
        let resolved = registry.resolve(name).unwrap().unwrap();
        let names = resolved
            .chain
            .iter()
            .map(|type_def| type_def.name.clone())
            .collect();
        (resolved.primitive.clone(), names)
    }

    #[test]
    fn resolve_follows_the_base_types_to_the_plain_type() {
        let mut registry = TypeRegistry::default();
        registry.insert("a", domain("a", "b"));
        registry.insert("b", domain("b", "integer"));
        registry.rebuild_chains();

        assert_eq!(
            chain_names(&registry, "A"),
            (
                "integer".to_string(),
                ["a".to_string(), "b".to_string()].to_vec()
            )
        );
        assert!(registry.resolve("integer").unwrap().is_none());
    }

    /// A chain that `rebuild_chains` built must not outlive a change of the
    /// types it goes through.
    #[test]
    fn resolve_gives_the_chain_of_the_current_types_after_each_change() {
        let mut registry = TypeRegistry::default();
        registry.insert("a", domain("a", "b"));
        registry.insert("b", domain("b", "integer"));
        registry.rebuild_chains();

        registry.insert("b", domain("b", "text"));
        assert_eq!(chain_names(&registry, "a").0, "text");

        registry.rebuild_chains();
        registry.remove("b");
        assert_eq!(
            chain_names(&registry, "a"),
            ("b".to_string(), ["a".to_string()].to_vec())
        );
    }

    #[test]
    fn resolve_reports_a_cycle_before_and_after_the_chains_are_built() {
        let mut registry = TypeRegistry::default();
        registry.insert("a", domain("a", "b"));
        registry.insert("b", domain("b", "a"));
        assert!(registry.resolve("a").is_err());

        registry.rebuild_chains();
        assert!(registry.resolve("a").is_err());
    }

    #[test]
    fn types_by_name_lists_a_type_with_an_alias_once() {
        let mut registry = TypeRegistry::default();
        let b = domain("b", "integer");
        registry.insert("b", Arc::clone(&b));
        registry.insert("alias_of_b", b);
        registry.insert("a", domain("a", "text"));

        let names: std::vec::Vec<&str> = registry
            .types_by_name()
            .into_iter()
            .map(|type_def| type_def.name.as_str())
            .collect();
        assert_eq!(names, ["a", "b"]);
        assert!(registry.get("alias_of_b").is_some());
    }
}
