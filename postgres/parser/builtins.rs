//! The functions, types, casts and command tags that PostgreSQL 17 has built
//! in. The files in `catalog/` list them, and `catalog/generate.sh` makes the
//! files from a PostgreSQL server.
//!
//! tursopg finds functions and types in these lists for the statements that
//! name a function or a type of PostgreSQL, such as DROP FUNCTION, so that it
//! gives the error that PostgreSQL gives.

use std::collections::HashMap;
use std::sync::OnceLock;

/// A function of the pg_catalog schema.
#[derive(Debug)]
pub struct Function {
    pub name: &'static str,
    pub kind: FunctionKind,
    pub returns_set: bool,
    pub volatility: Volatility,
    pub parameters: Vec<Parameter>,
    /// The name of the result type in pg_type, such as `int4`.
    pub result_type: &'static str,
    /// The C function that the function calls, when the function is in the
    /// internal language.
    c_function: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    Function,
    Aggregate,
    Window,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

/// An input parameter of a function.
#[derive(Debug)]
pub struct Parameter {
    pub name: Option<&'static str>,
    /// The name of the type in pg_type. The type of a VARIADIC parameter is
    /// an array type, or `any`.
    pub type_name: &'static str,
    pub variadic: bool,
    pub has_default: bool,
}

impl Function {
    /// The names of the parameter types in pg_type.
    pub fn parameter_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.parameters.iter().map(|parameter| parameter.type_name)
    }

    /// The name and the parameter types, as PostgreSQL shows them in an
    /// error, such as `lower(text)`.
    pub fn signature(&self) -> String {
        let types: Vec<String> = self.parameter_types().map(type_display_name).collect();
        format!("{}({})", self.name, types.join(", "))
    }

    /// The number of arguments that a call can give without VARIADIC
    /// expansion.
    fn required_arguments(&self) -> usize {
        self.parameters
            .iter()
            .filter(|parameter| !parameter.has_default)
            .count()
    }
}

/// The functions of pg_catalog that have the name `name`.
pub fn functions(name: &str) -> &'static [Function] {
    catalog()
        .functions
        .get(name)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// Whether `name` is the name of a C function that a LANGUAGE internal
/// function can call.
pub fn is_internal_c_function(name: &str) -> bool {
    catalog().c_functions.contains_key(name)
}

/// A type of the pg_catalog schema.
#[derive(Debug)]
pub struct Type {
    pub name: &'static str,
    /// typtype of pg_type: `b` for a base type, `c` for a composite type,
    /// `d` for a domain, `e` for an enum, `p` for a pseudo-type, `r` for a
    /// range type and `m` for a multirange type.
    pub kind: char,
    /// typcategory of pg_type, such as `N` for numeric types, `S` for string
    /// types and `A` for array types.
    pub category: char,
    /// The name of the type in an error, such as `integer` for `int4`.
    pub display_name: &'static str,
}

impl Type {
    pub fn is_pseudo(&self) -> bool {
        self.kind == 'p'
    }

    pub fn is_polymorphic(&self) -> bool {
        is_polymorphic(self.name)
    }

    pub fn is_array(&self) -> bool {
        self.category == 'A'
    }
}

/// The type of pg_catalog with the name `name` in pg_type, such as `int4`
/// or `_int4` for its array type.
pub fn find_type(name: &str) -> Option<&'static Type> {
    catalog().types.get(name)
}

/// The name of a type in an error, as PostgreSQL's format_type_be() gives
/// it: `integer` for `int4`, `text[]` for `_text`. A type that is not in
/// pg_catalog keeps its name.
pub fn type_display_name(name: &str) -> String {
    match find_type(name) {
        Some(found) => found.display_name.to_string(),
        None => name.to_string(),
    }
}

/// The types that take the type of the value that they get.
pub fn is_polymorphic(name: &str) -> bool {
    matches!(
        name,
        "anyarray"
            | "anycompatible"
            | "anycompatiblearray"
            | "anycompatiblemultirange"
            | "anycompatiblenonarray"
            | "anycompatiblerange"
            | "anyelement"
            | "anyenum"
            | "anymultirange"
            | "anynonarray"
            | "anyrange"
    )
}

/// The kinds of type conversion that PostgreSQL does without a CAST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Coercion {
    /// For an argument of a function call.
    Implicit,
    /// For a value stored in a column, or the result of a SQL function.
    Assignment,
}

/// Whether PostgreSQL converts a value of the type `source` to the type
/// `target` without a CAST, as its can_coerce_type() decides. `source` is
/// `unknown` for a string literal. `target` can be polymorphic.
pub fn can_coerce(source: &str, target: &str, coercion: Coercion) -> bool {
    if source == target || source == "unknown" || target == "any" {
        return true;
    }
    if is_polymorphic(target) {
        return accepts_polymorphic(source, target);
    }
    let Some(target_type) = find_type(target) else {
        return false;
    };
    // An I/O conversion to a string type is an assignment cast.
    if coercion == Coercion::Assignment && target_type.category == 'S' {
        return true;
    }
    if let (Some(source_element), Some(target_element)) =
        (source.strip_prefix('_'), target.strip_prefix('_'))
    {
        if find_type(source).is_some_and(Type::is_array) && target_type.is_array() {
            return can_coerce(source_element, target_element, coercion);
        }
    }
    match catalog().casts.get(&(source, target)) {
        Some(Coercion::Implicit) => true,
        Some(Coercion::Assignment) => coercion == Coercion::Assignment,
        None => false,
    }
}

/// Whether a value of the type `source` is a valid value of the type
/// `target` without a conversion, as PostgreSQL's IsBinaryCoercible()
/// decides.
pub fn is_binary_coercible(source: &str, target: &str) -> bool {
    source == target
        || target == "any"
        || (is_polymorphic(target) && accepts_polymorphic(source, target))
        || catalog().binary_casts.contains_key(&(source, target))
}

/// Whether a value of the type `source` can be the value of a parameter of
/// the polymorphic type `target`.
fn accepts_polymorphic(source: &str, target: &str) -> bool {
    let Some(source_type) = find_type(source) else {
        // A type that is not in pg_catalog is a type that the user made,
        // which can be the value of anyelement or anyenum.
        return !matches!(
            target,
            "anyarray" | "anycompatiblearray" | "anyrange" | "anycompatiblerange"
        ) && !target.ends_with("multirange");
    };
    if source_type.is_pseudo() {
        return false;
    }
    match target {
        "anyarray" | "anycompatiblearray" => source_type.is_array(),
        "anynonarray" | "anycompatiblenonarray" => !source_type.is_array(),
        "anyrange" | "anycompatiblerange" => source_type.kind == 'r',
        "anymultirange" | "anycompatiblemultirange" => source_type.kind == 'm',
        "anyenum" => source_type.kind == 'e',
        _ => true,
    }
}

/// The value of a command tag for an event trigger filter.
#[derive(Debug, Clone, Copy)]
pub struct CommandTag {
    /// Whether an event trigger for ddl_command_start, ddl_command_end or
    /// sql_drop can name the tag.
    pub event_trigger_ok: bool,
    /// Whether an event trigger for table_rewrite can name the tag.
    pub table_rewrite_ok: bool,
}

/// The command tag with the name `name`, such as `CREATE TABLE`. Like
/// PostgreSQL's GetCommandTagEnum(), the case of the letters is not
/// important.
pub fn command_tag(name: &str) -> Option<CommandTag> {
    catalog()
        .command_tags
        .iter()
        .find(|(tag, _)| tag.eq_ignore_ascii_case(name))
        .map(|(_, tag)| *tag)
}

/// The functions that can accept the arguments of a call, as PostgreSQL's
/// func_get_detail() finds them: the arguments have the types
/// `argument_types`, then the names `argument_names`. A type is `unknown`
/// for a string literal or when it is not known.
pub fn matching_functions<'a>(
    name: &str,
    argument_types: &'a [String],
    argument_names: &'a [String],
) -> impl Iterator<Item = &'static Function> + 'a {
    functions(name)
        .iter()
        .filter(move |function| accepts_arguments(function, argument_types, argument_names))
}

fn accepts_arguments(function: &Function, types: &[String], names: &[String]) -> bool {
    let positional = types.len() - names.len();
    let parameters = &function.parameters;
    if !names.is_empty() {
        // Named notation: each name must name a parameter after the
        // positional arguments, and each other parameter must have a
        // default.
        let mut given = vec![false; parameters.len()];
        for (index, given_type) in types.iter().enumerate().take(positional) {
            let Some(parameter) = parameters.get(index) else {
                return false;
            };
            if !can_coerce(given_type, parameter.type_name, Coercion::Implicit) {
                return false;
            }
            given[index] = true;
        }
        for (name, given_type) in names.iter().zip(&types[positional..]) {
            let Some(index) = parameters
                .iter()
                .position(|parameter| parameter.name == Some(name.as_str()))
            else {
                return false;
            };
            if given[index]
                || !can_coerce(given_type, parameters[index].type_name, Coercion::Implicit)
            {
                return false;
            }
            given[index] = true;
        }
        return parameters
            .iter()
            .zip(&given)
            .all(|(parameter, given)| *given || parameter.has_default);
    }
    let fits = |count: usize| count >= function.required_arguments() && count <= parameters.len();
    let accepts = |index: usize, given: &str| {
        parameters
            .get(index)
            .is_some_and(|parameter| can_coerce(given, parameter.type_name, Coercion::Implicit))
    };
    if fits(types.len()) && types.iter().enumerate().all(|(i, t)| accepts(i, t)) {
        return true;
    }
    // A VARIADIC parameter takes the extra arguments, each of the element
    // type of its array type.
    let Some(last) = parameters.last().filter(|parameter| parameter.variadic) else {
        return false;
    };
    let element = match last.type_name {
        "any" => "any",
        array => array.strip_prefix('_').unwrap_or("anyelement"),
    };
    let fixed = parameters.len() - 1;
    types.len() >= fixed
        && types[..fixed]
            .iter()
            .enumerate()
            .all(|(i, t)| accepts(i, t))
        && types[fixed..]
            .iter()
            .all(|given| can_coerce(given, element, Coercion::Implicit))
}

struct Catalog {
    functions: HashMap<&'static str, Vec<Function>>,
    c_functions: HashMap<&'static str, ()>,
    types: HashMap<&'static str, Type>,
    casts: HashMap<(&'static str, &'static str), Coercion>,
    /// The casts that do not change the value, such as varchar to text.
    binary_casts: HashMap<(&'static str, &'static str), ()>,
    command_tags: Vec<(&'static str, CommandTag)>,
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(load_catalog)
}

fn load_catalog() -> Catalog {
    let mut functions: HashMap<&'static str, Vec<Function>> = HashMap::new();
    let mut c_functions = HashMap::new();
    for line in include_str!("catalog/functions.dat").lines() {
        let function = parse_function(line);
        if let Some(c_function) = function.c_function {
            c_functions.insert(c_function, ());
        }
        functions.entry(function.name).or_default().push(function);
    }
    let types = include_str!("catalog/types.dat")
        .lines()
        .map(|line| {
            let mut fields = line.splitn(3, ' ');
            let name = fields.next().expect("type name");
            let mut flags = fields.next().expect("type kind").chars();
            let display_name = fields.next().expect("type display name");
            let ty = Type {
                name,
                kind: flags.next().expect("typtype"),
                category: flags.next().expect("typcategory"),
                display_name,
            };
            (name, ty)
        })
        .collect();
    let mut casts = HashMap::new();
    let mut binary_casts = HashMap::new();
    for line in include_str!("catalog/casts.dat").lines() {
        let mut fields = line.split(' ');
        let source = fields.next().expect("cast source");
        let target = fields.next().expect("cast target");
        let coercion = match fields.next() {
            Some("i") => Coercion::Implicit,
            _ => Coercion::Assignment,
        };
        casts.insert((source, target), coercion);
        if fields.next() == Some("b") {
            binary_casts.insert((source, target), ());
        }
    }
    let command_tags = include_str!("catalog/command_tags.dat")
        .lines()
        .map(|line| {
            let (flags, name) = line.split_at(2);
            let tag = CommandTag {
                event_trigger_ok: flags.starts_with('e'),
                table_rewrite_ok: flags.ends_with('r'),
            };
            (name.trim_start(), tag)
        })
        .collect();
    Catalog {
        functions,
        c_functions,
        types,
        casts,
        binary_casts,
        command_tags,
    }
}

/// Reads a line of functions.dat, such as
/// `f-ii make_date(year int4, month int4, day int4) date`.
fn parse_function(line: &'static str) -> Function {
    let (flags, rest) = line.split_once(' ').expect("function flags");
    let mut flags = flags.chars();
    let kind = match flags.next() {
        Some('a') => FunctionKind::Aggregate,
        Some('w') => FunctionKind::Window,
        _ => FunctionKind::Function,
    };
    let returns_set = flags.next() == Some('s');
    let volatility = match flags.next() {
        Some('i') => Volatility::Immutable,
        Some('s') => Volatility::Stable,
        _ => Volatility::Volatile,
    };
    let is_internal = flags.next() == Some('i');
    let (name, rest) = rest.split_once('(').expect("function name");
    let (parameters, rest) = rest.rsplit_once(") ").expect("function parameters");
    let (result_type, c_function) = match rest.split_once(" =") {
        Some((result_type, c_function)) => (result_type, Some(c_function)),
        None => (rest, is_internal.then_some(name)),
    };
    let parameters = if parameters.is_empty() {
        Vec::new()
    } else {
        parameters.split(", ").map(parse_parameter).collect()
    };
    Function {
        name,
        kind,
        returns_set,
        volatility,
        parameters,
        result_type,
        c_function,
    }
}

/// Reads a parameter of a line of functions.dat, such as `year int4` or
/// `variadic any`.
fn parse_parameter(text: &'static str) -> Parameter {
    let (variadic, text) = match text.strip_prefix("variadic ") {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (has_default, text) = match text.strip_suffix(" default") {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (name, type_name) = match text.split_once(' ') {
        Some((name, type_name)) => (Some(name), type_name),
        None => (None, text),
    };
    Parameter {
        name,
        type_name,
        variadic,
        has_default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_parameters_of_a_function() {
        let [make_date] = functions("make_date") else {
            panic!("make_date has one signature");
        };
        assert_eq!(
            make_date.signature(),
            "make_date(integer, integer, integer)"
        );
        assert_eq!(make_date.parameters[1].name, Some("month"));
        assert_eq!(make_date.result_type, "date");
    }

    #[test]
    fn a_string_converts_to_a_number_only_from_a_literal() {
        assert!(can_coerce("unknown", "int4", Coercion::Implicit));
        assert!(!can_coerce("text", "int4", Coercion::Assignment));
        assert!(can_coerce("int4", "text", Coercion::Assignment));
        assert!(!can_coerce("int4", "text", Coercion::Implicit));
        assert!(can_coerce("int8", "int4", Coercion::Assignment));
        assert!(!can_coerce("int8", "int4", Coercion::Implicit));
    }

    #[test]
    fn a_call_finds_the_functions_that_accept_its_arguments() {
        let unknown = ["unknown".to_string()];
        assert!(matching_functions("lower", &unknown, &[])
            .any(|function| function.signature() == "lower(text)"));
        let integer = ["int4".to_string()];
        assert_eq!(matching_functions("lower", &integer, &[]).count(), 0);
        let names = ["year", "month", "day"].map(String::from);
        let types = ["int4", "int4", "int4"].map(String::from);
        assert_eq!(matching_functions("make_date", &types, &names).count(), 1);
        let names = ["year", "month", "days"].map(String::from);
        assert_eq!(matching_functions("make_date", &types, &names).count(), 0);
        let texts = ["unknown", "unknown", "unknown"].map(String::from);
        assert_eq!(matching_functions("concat", &texts, &[]).count(), 1);
    }

    #[test]
    fn finds_command_tags_in_any_case() {
        assert!(command_tag("create table").is_some_and(|tag| tag.event_trigger_ok));
        assert!(command_tag("SELECT").is_some_and(|tag| !tag.event_trigger_ok));
        assert!(command_tag("ALTER TABLE").is_some_and(|tag| tag.table_rewrite_ok));
        assert!(command_tag("nosuch").is_none());
    }
}
