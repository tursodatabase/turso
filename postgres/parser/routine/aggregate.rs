//! CREATE AGGREGATE, checked in the order of PostgreSQL's DefineAggregate()
//! and AggregateCreate().

use super::body::check_literal_text;
use super::function::{check_parameters, invalid_function, ParameterOwner};
use super::types::{find_type, type_name_text, TypeRef};
use super::{creation_schema, error, qualified_name, strings, RoutineCatalog};
use crate::builtins::{self, FunctionKind};
use crate::sqlstate::{
    SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_SYNTAX_ERROR, SQLSTATE_UNDEFINED_FUNCTION,
    SQLSTATE_UNDEFINED_OBJECT,
};
use crate::translator::not_supported;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, DefElem};

/// The options of CREATE AGGREGATE that the checks use.
#[derive(Default)]
struct Options<'a> {
    sfunc: Option<Vec<&'a str>>,
    finalfunc: Option<Vec<&'a str>>,
    finalfunc_extra: bool,
    combinefunc: Option<Vec<&'a str>>,
    serialfunc: Option<Vec<&'a str>>,
    deserialfunc: Option<Vec<&'a str>>,
    msfunc: Option<Vec<&'a str>>,
    minvfunc: Option<Vec<&'a str>>,
    mfinalfunc: Option<Vec<&'a str>>,
    basetype: Option<TypeArg<'a>>,
    stype: Option<TypeArg<'a>>,
    mstype: Option<TypeArg<'a>>,
    msspace: bool,
    initcond: Option<&'a str>,
    minitcond: Option<&'a str>,
    parallel: Option<&'a str>,
}

/// A type that an option names: a type name, or a string such as 'ANY'.
#[derive(Clone, Copy)]
enum TypeArg<'a> {
    Name(&'a pb::TypeName),
    Text(&'a str),
}

pub(super) fn check_create_aggregate(
    stmt: &pb::DefineStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let names = strings(&stmt.defnames);
    let (schema, _) = qualified_name(&names)?;
    creation_schema(schema, catalog)?;
    let (parameters, is_ordered_set): (&[pb::Node], bool) = match stmt.args.as_slice() {
        [list, direct] if !stmt.oldstyle => {
            let parameters = match &list.node {
                Some(Node::List(list)) => list.items.as_slice(),
                _ => &[],
            };
            let is_ordered_set =
                matches!(&direct.node, Some(Node::Integer(direct)) if direct.ival >= 0);
            (parameters, is_ordered_set)
        }
        _ => (&[], false),
    };
    let options = collect_options(&stmt.definition, is_ordered_set)?;
    let Some(stype) = options.stype else {
        return Err(invalid_function("aggregate stype must be specified"));
    };
    let Some(sfunc) = &options.sfunc else {
        return Err(invalid_function("aggregate sfunc must be specified"));
    };
    check_moving_options(&options)?;
    let inputs = input_types(stmt, parameters, &options, catalog)?;
    let transition = transition_type(stype, catalog)?;
    match (&options.serialfunc, &options.deserialfunc) {
        (Some(_), Some(_)) if !transition.is("internal") => {
            return Err(invalid_function(
                "serialization functions may be specified only when the aggregate transition data type is internal",
            ))
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(invalid_function(
                "must specify both or neither of serialization and deserialization functions",
            ))
        }
        _ => {}
    }
    let moving = match options.mstype {
        Some(mstype) => Some(transition_type(mstype, catalog)?),
        None => None,
    };
    if let (Some(initcond), Some(name)) = (options.initcond, transition.builtin_name()) {
        if !transition.is_pseudo() {
            check_literal_text(initcond, name)?;
        }
    }
    if let (Some(minitcond), Some(name)) = (
        options.minitcond,
        moving.as_ref().and_then(TypeRef::builtin_name),
    ) {
        check_literal_text(minitcond, name)?;
    }
    if let Some(parallel) = options.parallel {
        if !matches!(parallel, "safe" | "restricted" | "unsafe") {
            return Err(error(
                SQLSTATE_SYNTAX_ERROR,
                "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE",
            ));
        }
    }
    if transition.is_polymorphic() && !inputs.iter().any(TypeRef::is_polymorphic) {
        return Err(invalid_function("cannot determine transition data type"));
    }
    let mut transition_arguments = vec![transition.clone()];
    transition_arguments.extend(inputs.iter().cloned());
    let function = find_aggregate_function(sfunc, &transition_arguments)?;
    check_result(
        function,
        &transition,
        "transition function",
        sfunc,
        &transition_arguments,
    )?;
    if let Some(finalfunc) = &options.finalfunc {
        let arguments = if options.finalfunc_extra {
            transition_arguments.clone()
        } else {
            vec![transition.clone()]
        };
        find_aggregate_function(finalfunc, &arguments)?;
    }
    if let Some(combinefunc) = &options.combinefunc {
        let arguments = [transition.clone(), transition.clone()];
        let function = find_aggregate_function(combinefunc, &arguments)?;
        check_result(
            function,
            &transition,
            "combine function",
            combinefunc,
            &arguments,
        )?;
    }
    if let (Some(moving), Some(msfunc)) = (&moving, &options.msfunc) {
        let mut arguments = vec![moving.clone()];
        arguments.extend(inputs.iter().cloned());
        find_aggregate_function(msfunc, &arguments)?;
        if let Some(minvfunc) = &options.minvfunc {
            find_aggregate_function(minvfunc, &arguments)?;
        }
        if let Some(mfinalfunc) = &options.mfinalfunc {
            find_aggregate_function(mfinalfunc, &[moving.clone()])?;
        }
    }
    Err(not_supported("CREATE AGGREGATE"))
}

fn collect_options(
    definition: &[pb::Node],
    is_ordered_set: bool,
) -> Result<Options<'_>, ParseError> {
    let mut options = Options::default();
    for option in definition {
        let Some(Node::DefElem(option)) = &option.node else {
            continue;
        };
        match option.defname.as_str() {
            "sfunc" | "sfunc1" => options.sfunc = Some(qualified_name_arg(option)),
            "finalfunc" => options.finalfunc = Some(qualified_name_arg(option)),
            "combinefunc" => options.combinefunc = Some(qualified_name_arg(option)),
            "serialfunc" => options.serialfunc = Some(qualified_name_arg(option)),
            "deserialfunc" => options.deserialfunc = Some(qualified_name_arg(option)),
            "msfunc" => options.msfunc = Some(qualified_name_arg(option)),
            "minvfunc" => options.minvfunc = Some(qualified_name_arg(option)),
            "mfinalfunc" => options.mfinalfunc = Some(qualified_name_arg(option)),
            "finalfunc_extra" => options.finalfunc_extra = boolean_arg(option),
            "finalfunc_modify" | "mfinalfunc_modify" => {
                let value = text_arg(option).unwrap_or_default().to_ascii_lowercase();
                if !matches!(value.as_str(), "read_only" | "shareable" | "read_write") {
                    return Err(error(
                        SQLSTATE_SYNTAX_ERROR,
                        format!(
                            "parameter \"{}\" must be READ_ONLY, SHAREABLE, or READ_WRITE",
                            option.defname
                        ),
                    ));
                }
            }
            "basetype" => options.basetype = type_arg(option),
            "hypothetical" if boolean_arg(option) && !is_ordered_set => {
                return Err(invalid_function(
                    "only ordered-set aggregates can be hypothetical",
                ));
            }
            "stype" | "stype1" => options.stype = type_arg(option),
            "mstype" => options.mstype = type_arg(option),
            "msspace" => options.msspace = true,
            "initcond" | "initcond1" => options.initcond = text_arg(option),
            "minitcond" => options.minitcond = text_arg(option),
            "parallel" => options.parallel = text_arg(option),
            // PostgreSQL warns about an option that it does not know.
            _ => {}
        }
    }
    Ok(options)
}

/// The checks of the options of a moving-aggregate mode: MSTYPE needs
/// MSFUNC and MINVFUNC, and the other options need MSTYPE.
fn check_moving_options(options: &Options) -> Result<(), ParseError> {
    if options.mstype.is_some() {
        if options.msfunc.is_none() {
            return Err(invalid_function(
                "aggregate msfunc must be specified when mstype is specified",
            ));
        }
        if options.minvfunc.is_none() {
            return Err(invalid_function(
                "aggregate minvfunc must be specified when mstype is specified",
            ));
        }
        return Ok(());
    }
    let without_mstype = [
        (options.msfunc.is_some(), "msfunc"),
        (options.minvfunc.is_some(), "minvfunc"),
        (options.mfinalfunc.is_some(), "mfinalfunc"),
        (options.msspace, "msspace"),
        (options.minitcond.is_some(), "minitcond"),
    ];
    match without_mstype.iter().find(|(given, _)| *given) {
        Some((_, name)) => Err(invalid_function(format!(
            "aggregate {name} must not be specified without mstype"
        ))),
        None => Ok(()),
    }
}

/// The types of the aggregated arguments.
fn input_types(
    stmt: &pb::DefineStmt,
    parameters: &[pb::Node],
    options: &Options,
    catalog: &dyn RoutineCatalog,
) -> Result<Vec<TypeRef>, ParseError> {
    if !stmt.oldstyle {
        if options.basetype.is_some() {
            return Err(invalid_function(
                "basetype is redundant with aggregate input type specification",
            ));
        }
        let (parameters, _) = check_parameters(parameters, ParameterOwner::Aggregate, catalog)?;
        return Ok(parameters
            .into_iter()
            .map(|parameter| parameter.ty)
            .collect());
    }
    let Some(basetype) = options.basetype else {
        return Err(invalid_function("aggregate input type must be specified"));
    };
    match basetype {
        TypeArg::Text(text) if text.eq_ignore_ascii_case("any") => Ok(Vec::new()),
        TypeArg::Name(name) if type_name_text(name).eq_ignore_ascii_case("any") => Ok(Vec::new()),
        basetype => Ok(vec![lookup_type(basetype, catalog)?]),
    }
}

/// The transition type, which cannot be a pseudo-type other than a
/// polymorphic type and internal.
fn transition_type(stype: TypeArg, catalog: &dyn RoutineCatalog) -> Result<TypeRef, ParseError> {
    let ty = lookup_type(stype, catalog)?;
    if ty.is_pseudo() && !ty.is_polymorphic() && !(ty.is("internal") && catalog.is_superuser()) {
        return Err(invalid_function(format!(
            "aggregate transition data type cannot be {}",
            ty.display_name()
        )));
    }
    Ok(ty)
}

/// The type that an option names, as PostgreSQL's typenameTypeId() finds
/// it.
fn lookup_type(arg: TypeArg, catalog: &dyn RoutineCatalog) -> Result<TypeRef, ParseError> {
    let parsed;
    let type_name = match arg {
        TypeArg::Name(name) => name,
        TypeArg::Text(text) => {
            parsed = pb::TypeName {
                names: vec![pb::Node {
                    node: Some(Node::String(pb::String {
                        sval: text.to_string(),
                    })),
                }],
                ..Default::default()
            };
            &parsed
        }
    };
    find_type(type_name, catalog)?.ok_or_else(|| {
        error(
            SQLSTATE_UNDEFINED_OBJECT,
            format!("type \"{}\" does not exist", type_name_text(type_name)),
        )
    })
}

/// The support function of an aggregate, as PostgreSQL's
/// lookup_agg_function() finds it: a normal function that accepts the
/// arguments, does not return a set, and needs no conversion of them.
fn find_aggregate_function(
    names: &[&str],
    arguments: &[TypeRef],
) -> Result<&'static builtins::Function, ParseError> {
    let types: Vec<String> = arguments
        .iter()
        .map(|ty| ty.builtin_name().unwrap_or("unknown").to_string())
        .collect();
    let signature = || {
        let types: Vec<String> = arguments.iter().map(TypeRef::display_name).collect();
        format!("{}({})", names.join("."), types.join(", "))
    };
    let function = match names {
        [name] | ["pg_catalog", name] => builtins::matching_functions(name, &types, &[])
            .find(|function| function.kind == FunctionKind::Function),
        _ => None,
    };
    let Some(function) = function else {
        return Err(error(
            SQLSTATE_UNDEFINED_FUNCTION,
            format!("function {} does not exist", signature()),
        ));
    };
    if function.returns_set {
        return Err(error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("function {} returns a set", signature()),
        ));
    }
    let needs_conversion = types
        .iter()
        .zip(function.parameter_types())
        .any(|(given, parameter)| !builtins::is_binary_coercible(given, parameter));
    if needs_conversion {
        return Err(error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!(
                "function {} requires run-time type coercion",
                function.signature()
            ),
        ));
    }
    Ok(function)
}

/// The result of a transition or combine function must be the transition
/// type.
fn check_result(
    function: &builtins::Function,
    transition: &TypeRef,
    what: &str,
    names: &[&str],
    arguments: &[TypeRef],
) -> Result<(), ParseError> {
    if builtins::is_polymorphic(function.result_type)
        || Some(function.result_type) == transition.builtin_name()
    {
        return Ok(());
    }
    let types: Vec<String> = arguments.iter().map(TypeRef::display_name).collect();
    Err(error(
        SQLSTATE_DATATYPE_MISMATCH,
        format!(
            "return type of {what} {}({}) is not {}",
            names.join("."),
            types.join(", "),
            transition.display_name()
        ),
    ))
}

fn qualified_name_arg(option: &DefElem) -> Vec<&str> {
    match option.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::TypeName(type_name)) => strings(&type_name.names),
        Some(Node::List(list)) => strings(&list.items),
        Some(Node::String(name)) => vec![name.sval.as_str()],
        _ => Vec::new(),
    }
}

fn type_arg(option: &DefElem) -> Option<TypeArg<'_>> {
    match option.arg.as_deref()?.node.as_ref()? {
        Node::TypeName(type_name) => Some(TypeArg::Name(type_name)),
        Node::String(text) => Some(TypeArg::Text(&text.sval)),
        _ => None,
    }
}

fn text_arg(option: &DefElem) -> Option<&str> {
    match option.arg.as_deref()?.node.as_ref()? {
        Node::String(text) => Some(&text.sval),
        Node::TypeName(type_name) => strings(&type_name.names).last().copied(),
        _ => None,
    }
}

/// The value of a boolean option, as PostgreSQL's defGetBoolean() reads it.
/// An option without a value is true.
fn boolean_arg(option: &DefElem) -> bool {
    match option.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
        None => true,
        Some(Node::Boolean(value)) => value.boolval,
        Some(Node::Integer(value)) => value.ival != 0,
        Some(Node::String(text)) => matches!(
            text.sval.to_ascii_lowercase().as_str(),
            "true" | "on" | "yes" | "1"
        ),
        Some(Node::TypeName(type_name)) => matches!(
            strings(&type_name.names)
                .last()
                .map(|name| name.to_ascii_lowercase())
                .as_deref(),
            Some("true" | "on" | "yes")
        ),
        _ => false,
    }
}
