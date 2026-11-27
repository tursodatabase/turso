// Translated from PostgreSQL 18.6, src/backend/parser/parse_coerce.c and src/backend/catalog/pg_inherits.c. See licenses/postgres-license.md.

use super::catalog::{
    get_array_type, get_base_type, get_element_type, get_multirange_range, get_range_multirange,
    get_range_subtype, get_type_category_preferred, is_complex, is_polymorphic_type,
    is_polymorphic_type_family1, type_is_array, type_is_array_domain, type_is_enum,
    type_is_multirange, type_is_range, typeid_type_is_composite, CastMethod, Catalog, ANYARRAYOID,
    ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID,
    ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID, ANYNONARRAYOID, ANYOID,
    ANYRANGEOID, INT2VECTOROID, INTERNALOID, INVALID_OID, OIDVECTOROID, RECORDARRAYOID, RECORDOID,
    TEXTARRAYOID, TEXTOID, TYPCATEGORY_INVALID, TYPCATEGORY_STRING, UNKNOWNOID,
};
use super::error::{datatype_mismatch, datatype_mismatch_with_detail, undefined_object};
use super::{ArgCoercion, Reason, ResolveError};
use turso_core::dialect::CastContext as CoercionContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoercionPathType {
    None,
    Func(i64),
    RelabelType,
    CoerceViaIo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommonType {
    Type(i64),
    /// The two types are of different categories.
    CannotBeMatched(i64, i64),
}

pub(crate) fn select_common_type(
    catalog: &dyn Catalog,
    typeids: &[i64],
) -> Result<CommonType, Reason> {
    assert!(!typeids.is_empty(), "a common type needs one type or more");
    let nargs = typeids.len();
    let mut i = 1;
    let mut ptype = typeids[0];

    if ptype != UNKNOWNOID {
        while i < nargs {
            if typeids[i] != ptype {
                break;
            }
            i += 1;
        }
        if i == nargs {
            return Ok(CommonType::Type(ptype));
        }
    }

    ptype = get_base_type(catalog, ptype)?;
    let (mut pcategory, mut pispreferred) = get_type_category_preferred(catalog, ptype)?;

    for &typeid in &typeids[i..] {
        let ntype = get_base_type(catalog, typeid)?;

        if ntype != UNKNOWNOID && ntype != ptype {
            let (ncategory, nispreferred) = get_type_category_preferred(catalog, ntype)?;
            if ptype == UNKNOWNOID {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                return Ok(CommonType::CannotBeMatched(ptype, ntype));
            } else if !pispreferred
                && can_coerce_type(catalog, &[ptype], &[ntype], CoercionContext::Implicit)?
                && !can_coerce_type(catalog, &[ntype], &[ptype], CoercionContext::Implicit)?
            {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
    }

    if ptype == UNKNOWNOID {
        ptype = TEXTOID;
    }

    Ok(CommonType::Type(ptype))
}

pub(crate) fn coerce_type(
    catalog: &dyn Catalog,
    input_type_id: i64,
    target_type_id: i64,
    ccontext: CoercionContext,
) -> Result<ArgCoercion, Reason> {
    if target_type_id == input_type_id {
        return Ok(ArgCoercion::None);
    }

    if target_type_id == ANYOID
        || target_type_id == ANYELEMENTOID
        || target_type_id == ANYNONARRAYOID
        || target_type_id == ANYCOMPATIBLEOID
        || target_type_id == ANYCOMPATIBLENONARRAYOID
    {
        return Ok(ArgCoercion::None);
    }

    if (target_type_id == ANYARRAYOID
        || target_type_id == ANYENUMOID
        || target_type_id == ANYRANGEOID
        || target_type_id == ANYMULTIRANGEOID
        || target_type_id == ANYCOMPATIBLEARRAYOID
        || target_type_id == ANYCOMPATIBLERANGEOID
        || target_type_id == ANYCOMPATIBLEMULTIRANGEOID)
        && input_type_id != UNKNOWNOID
    {
        get_base_type(catalog, input_type_id)?;
        return Ok(ArgCoercion::None);
    }

    // An argument of type unknown is always an untyped literal or a parameter here.
    if input_type_id == UNKNOWNOID {
        get_base_type(catalog, target_type_id)?;
        return Ok(ArgCoercion::Literal);
    }

    let coercion = match find_coercion_pathway(catalog, target_type_id, input_type_id, ccontext)? {
        CoercionPathType::None => None,
        CoercionPathType::Func(func_id) => Some(ArgCoercion::Function(func_id)),
        CoercionPathType::CoerceViaIo => Some(ArgCoercion::ViaInputOutput),
        CoercionPathType::RelabelType => Some(ArgCoercion::Relabel),
    };
    if let Some(coercion) = coercion {
        get_base_type(catalog, target_type_id)?;
        return Ok(coercion);
    }

    if input_type_id == RECORDOID && is_complex(catalog, target_type_id)? {
        return Err(Reason::Composite);
    }

    if target_type_id == RECORDOID && is_complex(catalog, input_type_id)? {
        return Err(Reason::Composite);
    }

    if target_type_id == RECORDARRAYOID && is_complex_array(catalog, input_type_id)? {
        return Err(Reason::Composite);
    }

    if type_inherits_from(catalog, input_type_id, target_type_id)?
        || type_is_of_typed_table(catalog, input_type_id)?
    {
        return Err(Reason::Composite);
    }

    panic!(
        "failed to find conversion function from {} to {}",
        catalog.format_type_name(input_type_id),
        catalog.format_type_name(target_type_id)
    );
}

pub(crate) fn can_coerce_type(
    catalog: &dyn Catalog,
    input_typeids: &[i64],
    target_typeids: &[i64],
    ccontext: CoercionContext,
) -> Result<bool, Reason> {
    assert_eq!(
        input_typeids.len(),
        target_typeids.len(),
        "every input type has a target type"
    );
    let mut have_generics = false;

    for (&input_type_id, &target_type_id) in input_typeids.iter().zip(target_typeids) {
        if input_type_id == target_type_id {
            continue;
        }

        if input_type_id == INTERNALOID || target_type_id == INTERNALOID {
            return Ok(false);
        }

        if target_type_id == ANYOID {
            continue;
        }

        if is_polymorphic_type(target_type_id) {
            have_generics = true;
            continue;
        }

        if input_type_id == UNKNOWNOID {
            continue;
        }

        let pathtype = find_coercion_pathway(catalog, target_type_id, input_type_id, ccontext)?;
        if pathtype != CoercionPathType::None {
            continue;
        }

        if input_type_id == RECORDOID && is_complex(catalog, target_type_id)? {
            return Err(Reason::Composite);
        }

        if target_type_id == RECORDOID && is_complex(catalog, input_type_id)? {
            return Err(Reason::Composite);
        }

        if target_type_id == RECORDARRAYOID && is_complex_array(catalog, input_type_id)? {
            return Err(Reason::Composite);
        }

        if type_inherits_from(catalog, input_type_id, target_type_id)?
            || type_is_of_typed_table(catalog, input_type_id)?
        {
            continue;
        }

        return Ok(false);
    }

    if have_generics && !check_generic_type_consistency(catalog, input_typeids, target_typeids)? {
        return Ok(false);
    }

    Ok(true)
}

pub(crate) fn check_generic_type_consistency(
    catalog: &dyn Catalog,
    actual_arg_types: &[i64],
    declared_arg_types: &[i64],
) -> Result<bool, Reason> {
    let mut elem_typeid = INVALID_OID;
    let mut array_typeid = INVALID_OID;
    let mut range_typeid = INVALID_OID;
    let mut multirange_typeid = INVALID_OID;
    let mut anycompatible_range_typeid = INVALID_OID;
    let mut anycompatible_range_typelem = INVALID_OID;
    let mut anycompatible_multirange_typeid = INVALID_OID;
    let mut anycompatible_multirange_typelem = INVALID_OID;
    let mut have_anynonarray = false;
    let mut have_anyenum = false;
    let mut have_anycompatible_nonarray = false;
    let mut anycompatible_actual_types = Vec::new();

    for (&decl_type, &actual_type) in declared_arg_types.iter().zip(actual_arg_types) {
        if decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID {
            if decl_type == ANYNONARRAYOID {
                have_anynonarray = true;
            } else if decl_type == ANYENUMOID {
                have_anyenum = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            if elem_typeid != INVALID_OID && actual_type != elem_typeid {
                return Ok(false);
            }
            elem_typeid = actual_type;
        } else if decl_type == ANYARRAYOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if array_typeid != INVALID_OID && actual_type != array_typeid {
                return Ok(false);
            }
            array_typeid = actual_type;
        } else if decl_type == ANYRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if range_typeid != INVALID_OID && actual_type != range_typeid {
                return Ok(false);
            }
            range_typeid = actual_type;
        } else if decl_type == ANYMULTIRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if multirange_typeid != INVALID_OID && actual_type != multirange_typeid {
                return Ok(false);
            }
            multirange_typeid = actual_type;
        } else if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
            if decl_type == ANYCOMPATIBLENONARRAYOID {
                have_anycompatible_nonarray = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            anycompatible_actual_types.push(actual_type);
        } else if decl_type == ANYCOMPATIBLEARRAYOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            let elem_type = get_element_type(catalog, actual_type);
            if elem_type == INVALID_OID {
                return Ok(false);
            }
            anycompatible_actual_types.push(elem_type);
        } else if decl_type == ANYCOMPATIBLERANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if anycompatible_range_typeid != INVALID_OID {
                if anycompatible_range_typeid != actual_type {
                    return Ok(false);
                }
            } else {
                anycompatible_range_typeid = actual_type;
                anycompatible_range_typelem = get_range_subtype(catalog, actual_type)?;
                if anycompatible_range_typelem == INVALID_OID {
                    return Ok(false);
                }
                anycompatible_actual_types.push(anycompatible_range_typelem);
            }
        } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if anycompatible_multirange_typeid != INVALID_OID {
                if anycompatible_multirange_typeid != actual_type {
                    return Ok(false);
                }
            } else {
                anycompatible_multirange_typeid = actual_type;
                anycompatible_multirange_typelem = get_multirange_range(catalog, actual_type)?;
                if anycompatible_multirange_typelem == INVALID_OID {
                    return Ok(false);
                }
            }
        }
    }

    if array_typeid != INVALID_OID && array_typeid != ANYARRAYOID {
        let array_typelem = get_element_type(catalog, array_typeid);
        if array_typelem == INVALID_OID {
            return Ok(false);
        }

        if elem_typeid == INVALID_OID {
            elem_typeid = array_typelem;
        } else if array_typelem != elem_typeid {
            return Ok(false);
        }
    }

    if multirange_typeid != INVALID_OID {
        let multirange_typelem = get_multirange_range(catalog, multirange_typeid)?;
        if multirange_typelem == INVALID_OID {
            return Ok(false);
        }

        if range_typeid == INVALID_OID {
            range_typeid = multirange_typelem;
            let range_typelem = get_range_subtype(catalog, multirange_typelem)?;
            if range_typelem == INVALID_OID {
                return Ok(false);
            }
        } else if multirange_typelem != range_typeid {
            return Ok(false);
        }
    }

    if range_typeid != INVALID_OID {
        let range_typelem = get_range_subtype(catalog, range_typeid)?;
        if range_typelem == INVALID_OID {
            return Ok(false);
        }

        if elem_typeid == INVALID_OID {
            elem_typeid = range_typelem;
        } else if range_typelem != elem_typeid {
            return Ok(false);
        }
    }

    if have_anynonarray && type_is_array_domain(catalog, elem_typeid)? {
        return Ok(false);
    }

    if have_anyenum && !type_is_enum(catalog, elem_typeid) {
        return Ok(false);
    }

    if anycompatible_multirange_typeid != INVALID_OID {
        if anycompatible_range_typeid != INVALID_OID {
            if anycompatible_multirange_typelem != anycompatible_range_typeid {
                return Ok(false);
            }
        } else {
            anycompatible_range_typeid = anycompatible_multirange_typelem;
            anycompatible_range_typelem = get_range_subtype(catalog, anycompatible_range_typeid)?;
            if anycompatible_range_typelem == INVALID_OID {
                return Ok(false);
            }
            anycompatible_actual_types.push(anycompatible_range_typelem);
        }
    }

    if !anycompatible_actual_types.is_empty() {
        let anycompatible_typeid = match select_common_type(catalog, &anycompatible_actual_types)? {
            CommonType::Type(anycompatible_typeid) => anycompatible_typeid,
            CommonType::CannotBeMatched(..) => return Ok(false),
        };

        if !verify_common_type_from_oids(
            catalog,
            anycompatible_typeid,
            &anycompatible_actual_types,
        )? {
            return Ok(false);
        }

        if have_anycompatible_nonarray && type_is_array_domain(catalog, anycompatible_typeid)? {
            return Ok(false);
        }

        if anycompatible_range_typelem != INVALID_OID
            && anycompatible_range_typelem != anycompatible_typeid
        {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(crate) fn enforce_generic_type_consistency(
    catalog: &dyn Catalog,
    actual_arg_types: &[i64],
    declared_arg_types: &mut [i64],
    rettype: i64,
    allow_poly: bool,
) -> Result<Result<i64, ResolveError>, Reason> {
    let mut have_poly_anycompatible = false;
    let mut have_poly_unknowns = false;
    let mut elem_typeid = INVALID_OID;
    let mut array_typeid = INVALID_OID;
    let mut range_typeid = INVALID_OID;
    let mut multirange_typeid = INVALID_OID;
    let mut anycompatible_typeid = INVALID_OID;
    let mut anycompatible_array_typeid = INVALID_OID;
    let mut anycompatible_range_typeid = INVALID_OID;
    let mut anycompatible_range_typelem = INVALID_OID;
    let mut anycompatible_multirange_typeid = INVALID_OID;
    let mut anycompatible_multirange_typelem = INVALID_OID;
    let mut have_anynonarray = rettype == ANYNONARRAYOID;
    let mut have_anyenum = rettype == ANYENUMOID;
    let mut have_anymultirange = rettype == ANYMULTIRANGEOID;
    let mut have_anycompatible_nonarray = rettype == ANYCOMPATIBLENONARRAYOID;
    let mut have_anycompatible_array = rettype == ANYCOMPATIBLEARRAYOID;
    let mut have_anycompatible_range = rettype == ANYCOMPATIBLERANGEOID;
    let mut have_anycompatible_multirange = rettype == ANYCOMPATIBLEMULTIRANGEOID;
    let mut n_poly_args = 0;
    let mut anycompatible_actual_types = Vec::new();

    for (&decl_type, &actual_type) in declared_arg_types.iter().zip(actual_arg_types) {
        if decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID {
            n_poly_args += 1;
            if decl_type == ANYNONARRAYOID {
                have_anynonarray = true;
            } else if decl_type == ANYENUMOID {
                have_anyenum = true;
            }
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            if elem_typeid != INVALID_OID && actual_type != elem_typeid {
                return Ok(Err(not_all_alike(
                    catalog,
                    "anyelement",
                    elem_typeid,
                    actual_type,
                )));
            }
            elem_typeid = actual_type;
        } else if decl_type == ANYARRAYOID {
            n_poly_args += 1;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if array_typeid != INVALID_OID && actual_type != array_typeid {
                return Ok(Err(not_all_alike(
                    catalog,
                    "anyarray",
                    array_typeid,
                    actual_type,
                )));
            }
            array_typeid = actual_type;
        } else if decl_type == ANYRANGEOID {
            n_poly_args += 1;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if range_typeid != INVALID_OID && actual_type != range_typeid {
                return Ok(Err(not_all_alike(
                    catalog,
                    "anyrange",
                    range_typeid,
                    actual_type,
                )));
            }
            range_typeid = actual_type;
        } else if decl_type == ANYMULTIRANGEOID {
            n_poly_args += 1;
            have_anymultirange = true;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if multirange_typeid != INVALID_OID && actual_type != multirange_typeid {
                return Ok(Err(not_all_alike(
                    catalog,
                    "anymultirange",
                    multirange_typeid,
                    actual_type,
                )));
            }
            multirange_typeid = actual_type;
        } else if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
            have_poly_anycompatible = true;
            if decl_type == ANYCOMPATIBLENONARRAYOID {
                have_anycompatible_nonarray = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            anycompatible_actual_types.push(actual_type);
        } else if decl_type == ANYCOMPATIBLEARRAYOID {
            have_poly_anycompatible = true;
            have_anycompatible_array = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            let anycompatible_elem_type = get_element_type(catalog, actual_type);
            if anycompatible_elem_type == INVALID_OID {
                return Ok(Err(is_not_of_the_declared_kind(
                    catalog,
                    "anycompatiblearray",
                    "an array",
                    actual_type,
                )));
            }
            anycompatible_actual_types.push(anycompatible_elem_type);
        } else if decl_type == ANYCOMPATIBLERANGEOID {
            have_poly_anycompatible = true;
            have_anycompatible_range = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if anycompatible_range_typeid != INVALID_OID {
                if anycompatible_range_typeid != actual_type {
                    return Ok(Err(not_all_alike(
                        catalog,
                        "anycompatiblerange",
                        anycompatible_range_typeid,
                        actual_type,
                    )));
                }
            } else {
                anycompatible_range_typeid = actual_type;
                anycompatible_range_typelem = get_range_subtype(catalog, actual_type)?;
                if anycompatible_range_typelem == INVALID_OID {
                    return Ok(Err(is_not_of_the_declared_kind(
                        catalog,
                        "anycompatiblerange",
                        "a range type",
                        actual_type,
                    )));
                }
                anycompatible_actual_types.push(anycompatible_range_typelem);
            }
        } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
            have_poly_anycompatible = true;
            have_anycompatible_multirange = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            let actual_type = get_base_type(catalog, actual_type)?;
            if anycompatible_multirange_typeid != INVALID_OID {
                if anycompatible_multirange_typeid != actual_type {
                    return Ok(Err(not_all_alike(
                        catalog,
                        "anycompatiblemultirange",
                        anycompatible_multirange_typeid,
                        actual_type,
                    )));
                }
            } else {
                anycompatible_multirange_typeid = actual_type;
                anycompatible_multirange_typelem = get_multirange_range(catalog, actual_type)?;
                if anycompatible_multirange_typelem == INVALID_OID {
                    return Ok(Err(is_not_of_the_declared_kind(
                        catalog,
                        "anycompatiblemultirange",
                        "a multirange type",
                        actual_type,
                    )));
                }
            }
        }
    }

    if n_poly_args == 0 && !have_poly_anycompatible {
        return Ok(Ok(rettype));
    }

    if n_poly_args > 0 {
        if array_typeid != INVALID_OID {
            let array_typelem;

            if array_typeid == ANYARRAYOID {
                if n_poly_args != 1
                    || (rettype != ANYARRAYOID && is_polymorphic_type_family1(rettype))
                {
                    return Ok(Err(datatype_mismatch(
                        "cannot determine element type of \"anyarray\" argument".to_string(),
                    )));
                }
                array_typelem = ANYELEMENTOID;
            } else {
                array_typelem = get_element_type(catalog, array_typeid);
                if array_typelem == INVALID_OID {
                    return Ok(Err(is_not_of_the_declared_kind(
                        catalog,
                        "anyarray",
                        "an array",
                        array_typeid,
                    )));
                }
            }

            if elem_typeid == INVALID_OID {
                elem_typeid = array_typelem;
            } else if array_typelem != elem_typeid {
                return Ok(Err(not_consistent(
                    catalog,
                    ("anyarray", array_typeid),
                    ("anyelement", elem_typeid),
                )));
            }
        }

        if multirange_typeid != INVALID_OID {
            let multirange_typelem = get_multirange_range(catalog, multirange_typeid)?;
            if multirange_typelem == INVALID_OID {
                return Ok(Err(is_not_of_the_declared_kind(
                    catalog,
                    "anymultirange",
                    "a multirange type",
                    multirange_typeid,
                )));
            }

            if range_typeid == INVALID_OID {
                range_typeid = multirange_typelem;
            } else if multirange_typelem != range_typeid {
                return Ok(Err(not_consistent(
                    catalog,
                    ("anymultirange", multirange_typeid),
                    ("anyrange", range_typeid),
                )));
            }
        } else if have_anymultirange && range_typeid != INVALID_OID {
            multirange_typeid = get_range_multirange(catalog, range_typeid)?;
        }

        if range_typeid != INVALID_OID {
            let range_typelem = get_range_subtype(catalog, range_typeid)?;
            if range_typelem == INVALID_OID {
                return Ok(Err(is_not_of_the_declared_kind(
                    catalog,
                    "anyrange",
                    "a range type",
                    range_typeid,
                )));
            }

            if elem_typeid == INVALID_OID {
                elem_typeid = range_typelem;
            } else if range_typelem != elem_typeid {
                return Ok(Err(not_consistent(
                    catalog,
                    ("anyrange", range_typeid),
                    ("anyelement", elem_typeid),
                )));
            }
        }

        if elem_typeid == INVALID_OID {
            if allow_poly {
                elem_typeid = ANYELEMENTOID;
                array_typeid = ANYARRAYOID;
                range_typeid = ANYRANGEOID;
                multirange_typeid = ANYMULTIRANGEOID;
            } else {
                return Ok(Err(datatype_mismatch(
                    "could not determine polymorphic type because input has type unknown"
                        .to_string(),
                )));
            }
        }

        if have_anynonarray
            && elem_typeid != ANYELEMENTOID
            && type_is_array_domain(catalog, elem_typeid)?
        {
            return Ok(Err(datatype_mismatch(format!(
                "type matched to anynonarray is an array type: {}",
                catalog.format_type_name(elem_typeid)
            ))));
        }

        if have_anyenum && elem_typeid != ANYELEMENTOID && !type_is_enum(catalog, elem_typeid) {
            return Ok(Err(datatype_mismatch(format!(
                "type matched to anyenum is not an enum type: {}",
                catalog.format_type_name(elem_typeid)
            ))));
        }
    }

    if have_poly_anycompatible {
        if anycompatible_multirange_typeid != INVALID_OID {
            if anycompatible_range_typeid != INVALID_OID {
                if anycompatible_multirange_typelem != anycompatible_range_typeid {
                    return Ok(Err(not_consistent(
                        catalog,
                        ("anycompatiblemultirange", anycompatible_multirange_typeid),
                        ("anycompatiblerange", anycompatible_range_typeid),
                    )));
                }
            } else {
                anycompatible_range_typeid = anycompatible_multirange_typelem;
                anycompatible_range_typelem =
                    get_range_subtype(catalog, anycompatible_range_typeid)?;
                if anycompatible_range_typelem == INVALID_OID {
                    return Ok(Err(is_not_of_the_declared_kind(
                        catalog,
                        "anycompatiblemultirange",
                        "a multirange type",
                        anycompatible_multirange_typeid,
                    )));
                }
                have_anycompatible_range = true;
                anycompatible_actual_types.push(anycompatible_range_typelem);
            }
        } else if have_anycompatible_multirange && anycompatible_range_typeid != INVALID_OID {
            anycompatible_multirange_typeid =
                get_range_multirange(catalog, anycompatible_range_typeid)?;
        }

        if !anycompatible_actual_types.is_empty() {
            anycompatible_typeid = match select_common_type(catalog, &anycompatible_actual_types)? {
                CommonType::Type(anycompatible_typeid) => anycompatible_typeid,
                CommonType::CannotBeMatched(ptype, ntype) => {
                    return Ok(Err(datatype_mismatch(format!(
                        "argument types {} and {} cannot be matched",
                        catalog.format_type_name(ptype),
                        catalog.format_type_name(ntype)
                    ))));
                }
            };

            if !verify_common_type_from_oids(
                catalog,
                anycompatible_typeid,
                &anycompatible_actual_types,
            )? {
                return Ok(Err(datatype_mismatch(
                    "arguments of anycompatible family cannot be cast to a common type".to_string(),
                )));
            }

            if have_anycompatible_array {
                anycompatible_array_typeid = get_array_type(catalog, anycompatible_typeid);
                if anycompatible_array_typeid == INVALID_OID {
                    return Ok(Err(could_not_find_array_type(
                        catalog,
                        anycompatible_typeid,
                    )));
                }
            }

            if have_anycompatible_range {
                if anycompatible_range_typeid == INVALID_OID {
                    return Ok(Err(could_not_determine_polymorphic_type(
                        "anycompatiblerange",
                    )));
                }

                if anycompatible_range_typelem != anycompatible_typeid {
                    return Ok(Err(datatype_mismatch(format!(
                        "anycompatiblerange type {} does not match anycompatible type {}",
                        catalog.format_type_name(anycompatible_range_typeid),
                        catalog.format_type_name(anycompatible_typeid)
                    ))));
                }
            }

            if have_anycompatible_multirange {
                if anycompatible_multirange_typeid == INVALID_OID {
                    return Ok(Err(could_not_determine_polymorphic_type(
                        "anycompatiblemultirange",
                    )));
                }

                if anycompatible_range_typelem != anycompatible_typeid {
                    return Ok(Err(datatype_mismatch(format!(
                        "anycompatiblemultirange type {} does not match anycompatible type {}",
                        catalog.format_type_name(anycompatible_multirange_typeid),
                        catalog.format_type_name(anycompatible_typeid)
                    ))));
                }
            }

            if have_anycompatible_nonarray && type_is_array_domain(catalog, anycompatible_typeid)? {
                return Ok(Err(datatype_mismatch(format!(
                    "type matched to anycompatiblenonarray is an array type: {}",
                    catalog.format_type_name(anycompatible_typeid)
                ))));
            }
        } else if allow_poly {
            anycompatible_typeid = ANYCOMPATIBLEOID;
            anycompatible_array_typeid = ANYCOMPATIBLEARRAYOID;
            anycompatible_range_typeid = ANYCOMPATIBLERANGEOID;
            anycompatible_multirange_typeid = ANYCOMPATIBLEMULTIRANGEOID;
        } else {
            anycompatible_typeid = TEXTOID;
            anycompatible_array_typeid = TEXTARRAYOID;
            if have_anycompatible_range {
                return Ok(Err(could_not_determine_polymorphic_type(
                    "anycompatiblerange",
                )));
            }
            if have_anycompatible_multirange {
                return Ok(Err(could_not_determine_polymorphic_type(
                    "anycompatiblemultirange",
                )));
            }
        }

        for declared_arg_type in declared_arg_types.iter_mut() {
            let decl_type = *declared_arg_type;

            if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
                *declared_arg_type = anycompatible_typeid;
            } else if decl_type == ANYCOMPATIBLEARRAYOID {
                *declared_arg_type = anycompatible_array_typeid;
            } else if decl_type == ANYCOMPATIBLERANGEOID {
                *declared_arg_type = anycompatible_range_typeid;
            } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
                *declared_arg_type = anycompatible_multirange_typeid;
            }
        }
    }

    if have_poly_unknowns {
        for (declared_arg_type, &actual_type) in declared_arg_types.iter_mut().zip(actual_arg_types)
        {
            let decl_type = *declared_arg_type;

            if actual_type != UNKNOWNOID {
                continue;
            }

            if decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID
            {
                *declared_arg_type = elem_typeid;
            } else if decl_type == ANYARRAYOID {
                if array_typeid == INVALID_OID {
                    array_typeid = get_array_type(catalog, elem_typeid);
                    if array_typeid == INVALID_OID {
                        return Ok(Err(could_not_find_array_type(catalog, elem_typeid)));
                    }
                }
                *declared_arg_type = array_typeid;
            } else if decl_type == ANYRANGEOID {
                if range_typeid == INVALID_OID {
                    return Ok(Err(could_not_determine_polymorphic_type("anyrange")));
                }
                *declared_arg_type = range_typeid;
            } else if decl_type == ANYMULTIRANGEOID {
                if multirange_typeid == INVALID_OID {
                    return Ok(Err(could_not_determine_polymorphic_type("anymultirange")));
                }
                *declared_arg_type = multirange_typeid;
            }
        }
    }

    if rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID {
        return Ok(Ok(elem_typeid));
    }

    if rettype == ANYARRAYOID {
        if array_typeid == INVALID_OID {
            array_typeid = get_array_type(catalog, elem_typeid);
            if array_typeid == INVALID_OID {
                return Ok(Err(could_not_find_array_type(catalog, elem_typeid)));
            }
        }
        return Ok(Ok(array_typeid));
    }

    if rettype == ANYRANGEOID {
        if range_typeid == INVALID_OID {
            return Ok(Err(could_not_determine_polymorphic_type("anyrange")));
        }
        return Ok(Ok(range_typeid));
    }

    if rettype == ANYMULTIRANGEOID {
        if multirange_typeid == INVALID_OID {
            return Ok(Err(could_not_determine_polymorphic_type("anymultirange")));
        }
        return Ok(Ok(multirange_typeid));
    }

    if rettype == ANYCOMPATIBLEOID || rettype == ANYCOMPATIBLENONARRAYOID {
        if anycompatible_typeid == INVALID_OID {
            return Ok(Err(datatype_mismatch(
                "could not identify anycompatible type".to_string(),
            )));
        }
        return Ok(Ok(anycompatible_typeid));
    }

    if rettype == ANYCOMPATIBLEARRAYOID {
        if anycompatible_array_typeid == INVALID_OID {
            return Ok(Err(datatype_mismatch(
                "could not identify anycompatiblearray type".to_string(),
            )));
        }
        return Ok(Ok(anycompatible_array_typeid));
    }

    if rettype == ANYCOMPATIBLERANGEOID {
        if anycompatible_range_typeid == INVALID_OID {
            return Ok(Err(datatype_mismatch(
                "could not identify anycompatiblerange type".to_string(),
            )));
        }
        return Ok(Ok(anycompatible_range_typeid));
    }

    if rettype == ANYCOMPATIBLEMULTIRANGEOID {
        if anycompatible_multirange_typeid == INVALID_OID {
            return Ok(Err(datatype_mismatch(
                "could not identify anycompatiblemultirange type".to_string(),
            )));
        }
        return Ok(Ok(anycompatible_multirange_typeid));
    }

    Ok(Ok(rettype))
}

fn verify_common_type_from_oids(
    catalog: &dyn Catalog,
    common_type: i64,
    typeids: &[i64],
) -> Result<bool, Reason> {
    for &typeid in typeids {
        if !can_coerce_type(
            catalog,
            &[typeid],
            &[common_type],
            CoercionContext::Implicit,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn not_all_alike(catalog: &dyn Catalog, declared: &str, first: i64, second: i64) -> ResolveError {
    datatype_mismatch_with_detail(
        format!("arguments declared \"{declared}\" are not all alike"),
        format!(
            "{} versus {}",
            catalog.format_type_name(first),
            catalog.format_type_name(second)
        ),
    )
}

fn not_consistent(
    catalog: &dyn Catalog,
    (declared, actual): (&str, i64),
    (other_declared, other_actual): (&str, i64),
) -> ResolveError {
    datatype_mismatch_with_detail(
        format!(
            "argument declared {declared} is not consistent with argument declared {other_declared}"
        ),
        format!(
            "{} versus {}",
            catalog.format_type_name(actual),
            catalog.format_type_name(other_actual)
        ),
    )
}

fn is_not_of_the_declared_kind(
    catalog: &dyn Catalog,
    declared: &str,
    kind: &str,
    actual: i64,
) -> ResolveError {
    datatype_mismatch(format!(
        "argument declared {declared} is not {kind} but type {}",
        catalog.format_type_name(actual)
    ))
}

fn could_not_determine_polymorphic_type(declared: &str) -> ResolveError {
    datatype_mismatch(format!(
        "could not determine polymorphic type {declared} because input has type unknown"
    ))
}

fn could_not_find_array_type(catalog: &dyn Catalog, typid: i64) -> ResolveError {
    undefined_object(format!(
        "could not find array type for data type {}",
        catalog.format_type_name(typid)
    ))
}

pub(crate) fn type_category(catalog: &dyn Catalog, type_id: i64) -> Result<char, Reason> {
    let (typcategory, _) = get_type_category_preferred(catalog, type_id)?;
    assert_ne!(typcategory, TYPCATEGORY_INVALID);
    Ok(typcategory)
}

pub(crate) fn is_preferred_type(
    catalog: &dyn Catalog,
    category: char,
    type_id: i64,
) -> Result<bool, Reason> {
    let (typcategory, typispreferred) = get_type_category_preferred(catalog, type_id)?;
    if category == typcategory || category == TYPCATEGORY_INVALID {
        Ok(typispreferred)
    } else {
        Ok(false)
    }
}

// FIXME(pg-catalog): Only a test calls this, until a call that needs binary coercion is resolved.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn is_binary_coercible(
    catalog: &dyn Catalog,
    srctype: i64,
    targettype: i64,
) -> Result<bool, Reason> {
    if srctype == targettype {
        return Ok(true);
    }

    if targettype == ANYOID || targettype == ANYELEMENTOID || targettype == ANYCOMPATIBLEOID {
        return Ok(true);
    }

    let srctype = if srctype != INVALID_OID {
        get_base_type(catalog, srctype)?
    } else {
        srctype
    };

    if srctype == targettype {
        return Ok(true);
    }

    if (targettype == ANYARRAYOID || targettype == ANYCOMPATIBLEARRAYOID)
        && type_is_array(catalog, srctype)
    {
        return Ok(true);
    }

    if (targettype == ANYNONARRAYOID || targettype == ANYCOMPATIBLENONARRAYOID)
        && !type_is_array(catalog, srctype)
    {
        return Ok(true);
    }

    if targettype == ANYENUMOID && type_is_enum(catalog, srctype) {
        return Ok(true);
    }

    if (targettype == ANYRANGEOID || targettype == ANYCOMPATIBLERANGEOID)
        && type_is_range(catalog, srctype)
    {
        return Ok(true);
    }

    if (targettype == ANYMULTIRANGEOID || targettype == ANYCOMPATIBLEMULTIRANGEOID)
        && type_is_multirange(catalog, srctype)
    {
        return Ok(true);
    }

    if targettype == RECORDOID && is_complex(catalog, srctype)? {
        return Ok(true);
    }

    if targettype == RECORDARRAYOID && is_complex_array(catalog, srctype)? {
        return Ok(true);
    }

    let Some(cast_form) = catalog.pg_cast(srctype, targettype) else {
        return Ok(false);
    };

    Ok(cast_form.method == CastMethod::Binary && cast_form.context == CoercionContext::Implicit)
}

pub(crate) fn find_coercion_pathway(
    catalog: &dyn Catalog,
    target_type_id: i64,
    source_type_id: i64,
    ccontext: CoercionContext,
) -> Result<CoercionPathType, Reason> {
    let mut result = CoercionPathType::None;

    let source_type_id = if source_type_id != INVALID_OID {
        get_base_type(catalog, source_type_id)?
    } else {
        source_type_id
    };
    let target_type_id = if target_type_id != INVALID_OID {
        get_base_type(catalog, target_type_id)?
    } else {
        target_type_id
    };

    if source_type_id == target_type_id {
        return Ok(CoercionPathType::RelabelType);
    }

    if source_type_id == INTERNALOID || target_type_id == INTERNALOID {
        return Ok(CoercionPathType::None);
    }

    if let Some(cast_form) = catalog.pg_cast(source_type_id, target_type_id) {
        if ccontext >= cast_form.context {
            result = match cast_form.method {
                CastMethod::Function(castfunc) => CoercionPathType::Func(castfunc),
                CastMethod::InputOutput => CoercionPathType::CoerceViaIo,
                CastMethod::Binary => CoercionPathType::RelabelType,
            };
        }
    } else {
        if target_type_id != OIDVECTOROID && target_type_id != INT2VECTOROID {
            let target_elem = get_element_type(catalog, target_type_id);
            let source_elem = get_element_type(catalog, source_type_id);
            if target_elem != INVALID_OID && source_elem != INVALID_OID {
                let elempathtype =
                    find_coercion_pathway(catalog, target_elem, source_elem, ccontext)?;
                if elempathtype != CoercionPathType::None {
                    return Err(Reason::Array);
                }
            }
        }

        let to_string = ccontext >= CoercionContext::Assignment
            && type_category(catalog, target_type_id)? == TYPCATEGORY_STRING;
        if to_string
            || (ccontext >= CoercionContext::Explicit
                && type_category(catalog, source_type_id)? == TYPCATEGORY_STRING)
        {
            result = CoercionPathType::CoerceViaIo;
        }
    }

    Ok(result)
}

fn is_complex_array(catalog: &dyn Catalog, typid: i64) -> Result<bool, Reason> {
    let elemtype = get_element_type(catalog, typid);

    Ok(elemtype != INVALID_OID && is_complex(catalog, elemtype)?)
}

fn type_is_of_typed_table(catalog: &dyn Catalog, reltype_id: i64) -> Result<bool, Reason> {
    if is_complex(catalog, reltype_id)? {
        return Err(Reason::Composite);
    }

    Ok(false)
}

fn type_inherits_from(
    catalog: &dyn Catalog,
    subclass_type_id: i64,
    superclass_type_id: i64,
) -> Result<bool, Reason> {
    if !is_complex(catalog, subclass_type_id)? {
        return Ok(false);
    }
    if !typeid_type_is_composite(catalog, superclass_type_id)? {
        return Ok(false);
    }

    Err(Reason::Composite)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::catalog::fixtures::FixtureCatalog;
    use crate::resolve::oracle::{oracle_cases, OracleCase};
    use std::collections::BTreeSet;
    use CoercionContext::{Assignment, Explicit, Implicit};

    const BOOL: i64 = 16;
    const NAME: i64 = 19;
    const INT8: i64 = 20;
    const INT2: i64 = 21;
    const INT4: i64 = 23;
    const OID: i64 = 26;
    const PG_TYPE_ROW: i64 = 71;
    const JSON: i64 = 114;
    const FLOAT4: i64 = 700;
    const FLOAT8: i64 = 701;
    const INT4_ARRAY: i64 = 1007;
    const INT8_ARRAY: i64 = 1016;
    const BPCHAR: i64 = 1042;
    const VARCHAR: i64 = 1043;
    const DATE: i64 = 1082;
    const TIMESTAMP: i64 = 1114;
    const TIMESTAMPTZ: i64 = 1184;
    const NUMERIC: i64 = 1700;
    const REGCLASS: i64 = 2205;
    const JSONB: i64 = 3802;
    const INT4RANGE: i64 = 3904;
    const INT4MULTIRANGE: i64 = 4451;

    fn oracle_types() -> BTreeSet<i64> {
        oracle_cases()
            .iter()
            .flat_map(|case| case.args.clone())
            .filter(|&oid| oid != UNKNOWNOID)
            .collect()
    }

    fn parameter_types(catalog: &FixtureCatalog, case: &OracleCase) -> Option<Vec<i64>> {
        let chosen = case.chosen?;
        if case.kind == "operator" {
            let operator = catalog
                .operator_by_oid(chosen)
                .expect("the chosen operator is in the fixtures");
            return Some(match operator.kind {
                'l' => vec![operator.right],
                _ => vec![operator.left, operator.right],
            });
        }
        let proc = catalog
            .proc_by_oid(chosen)
            .expect("the chosen function is in the fixtures");
        (proc.variadic == INVALID_OID && proc.arg_types.len() == case.args.len())
            .then(|| proc.arg_types.clone())
    }

    fn takes_any_type(parameter: i64) -> bool {
        parameter == ANYOID || is_polymorphic_type(parameter)
    }

    #[test]
    fn a_cast_in_pg_cast_is_allowed_in_its_context_and_in_wider_ones_only() {
        let catalog = FixtureCatalog::get();
        let types = oracle_types();
        let mut casts = 0;
        for &source in &types {
            for &target in &types {
                let Some(cast) = catalog.pg_cast(source, target) else {
                    continue;
                };
                if source == target {
                    continue;
                }
                casts += 1;
                let expected = match cast.method {
                    CastMethod::Function(oid) => CoercionPathType::Func(oid),
                    CastMethod::InputOutput => CoercionPathType::CoerceViaIo,
                    CastMethod::Binary => CoercionPathType::RelabelType,
                };
                let narrowest = [Implicit, Assignment, Explicit]
                    .into_iter()
                    .find(|&context| {
                        find_coercion_pathway(catalog, target, source, context) == Ok(expected)
                    });
                assert_eq!(narrowest, Some(cast.context), "{source} to {target}");
                for context in [Implicit, Assignment, Explicit] {
                    if context < cast.context {
                        assert_eq!(
                            find_coercion_pathway(catalog, target, source, context),
                            Ok(CoercionPathType::None),
                            "{source} to {target} in {context:?}"
                        );
                    }
                }
            }
        }
        assert!(casts > 150, "only {casts} casts checked");
    }

    #[test]
    fn without_a_pg_cast_row_only_a_string_type_converts_and_not_implicitly() {
        let catalog = FixtureCatalog::get();
        let types = oracle_types();
        let mut pairs = 0;
        for &source in &types {
            for &target in &types {
                let both_arrays = type_is_array(catalog, source) && type_is_array(catalog, target);
                if source == target || both_arrays || catalog.pg_cast(source, target).is_some() {
                    continue;
                }
                pairs += 1;
                let to_string = type_category(catalog, target) == Ok(TYPCATEGORY_STRING);
                let from_string = type_category(catalog, source) == Ok(TYPCATEGORY_STRING);
                let through_io = |allowed| {
                    Ok(if allowed {
                        CoercionPathType::CoerceViaIo
                    } else {
                        CoercionPathType::None
                    })
                };
                assert_eq!(
                    find_coercion_pathway(catalog, target, source, Implicit),
                    Ok(CoercionPathType::None),
                    "{source} to {target}"
                );
                assert_eq!(
                    find_coercion_pathway(catalog, target, source, Assignment),
                    through_io(to_string),
                    "{source} to {target}"
                );
                assert_eq!(
                    find_coercion_pathway(catalog, target, source, Explicit),
                    through_io(to_string || from_string),
                    "{source} to {target}"
                );
            }
        }
        assert!(pairs > 7000, "only {pairs} pairs checked");
    }

    #[test]
    fn each_argument_gets_the_coercion_postgresql_inserted() {
        let catalog = FixtureCatalog::get();
        let mut checked = 0;
        for case in oracle_cases() {
            let Some(parameters) = parameter_types(catalog, case) else {
                continue;
            };
            for ((&argument, &parameter), coercion) in
                case.args.iter().zip(&parameters).zip(&case.coercions)
            {
                if argument == UNKNOWNOID || takes_any_type(parameter) {
                    continue;
                }
                let expected = if coercion == "none" {
                    assert_eq!(argument, parameter, "{} {:?}", case.name, case.args);
                    continue;
                } else if let Some(function) = coercion.strip_prefix("fn:") {
                    let (function, result) = function.split_once('>').expect("fn:<oid>><type oid>");
                    assert_eq!(result.parse(), Ok(parameter));
                    CoercionPathType::Func(function.parse().expect("a function oid"))
                } else if let Some(result) = coercion.strip_prefix("relabel>") {
                    assert_eq!(result.parse(), Ok(parameter));
                    CoercionPathType::RelabelType
                } else {
                    continue;
                };
                assert_eq!(
                    find_coercion_pathway(catalog, parameter, argument, Implicit),
                    Ok(expected),
                    "{} {:?}",
                    case.name,
                    case.args
                );
                checked += 1;
            }
        }
        assert!(checked > 500, "only {checked} coercions checked");
    }

    #[test]
    fn the_arguments_can_be_coerced_to_the_parameters_of_the_chosen_row() {
        let catalog = FixtureCatalog::get();
        let mut coercible = 0;
        for case in oracle_cases() {
            let Some(parameters) = parameter_types(catalog, case) else {
                continue;
            };
            let coerces_an_array = case
                .coercions
                .iter()
                .any(|coercion| coercion.starts_with("array>"));
            match can_coerce_type(catalog, &case.args, &parameters, Implicit) {
                Ok(true) => coercible += 1,
                Err(Reason::Array) if coerces_an_array => {}
                other => panic!("{} {:?}: {other:?}", case.name, case.args),
            }
        }
        assert!(coercible > 8000, "only {coercible} calls checked");
    }

    #[test]
    fn a_call_postgresql_refuses_has_no_row_its_arguments_can_be_coerced_to() {
        let catalog = FixtureCatalog::get();
        let mut refused = 0;
        for case in oracle_cases() {
            if case.kind != "function" || case.sqlstate.as_deref() != Some("42883") {
                continue;
            }
            let candidates = catalog.pg_proc(&case.name);
            if candidates
                .iter()
                .any(|proc| proc.variadic != INVALID_OID || proc.default_count > 0)
            {
                continue;
            }
            for proc in candidates {
                if proc.arg_types.len() != case.args.len() {
                    continue;
                }
                assert_ne!(
                    can_coerce_type(catalog, &case.args, proc.arg_types, Implicit),
                    Ok(true),
                    "{} {:?} can be coerced to row {}",
                    case.name,
                    case.args,
                    proc.oid
                );
            }
            refused += 1;
        }
        assert!(refused > 8000, "only {refused} calls checked");
    }

    #[test]
    fn a_cast_written_as_a_function_call_takes_the_explicit_pathway() {
        let catalog = FixtureCatalog::get();
        let mut checked = 0;
        for case in oracle_cases() {
            let (Some(node), [source]) = (&case.node, case.args.as_slice()) else {
                continue;
            };
            if *source == UNKNOWNOID {
                continue;
            }
            let target = catalog
                .type_oids()
                .into_iter()
                .find(|&oid| catalog.type_name(oid) == Some(case.name.as_str()))
                .expect("the cast names a type");
            let expected = match node.as_str() {
                "CONST" => {
                    assert_eq!(*source, target);
                    continue;
                }
                "COERCEVIAIO" => CoercionPathType::CoerceViaIo,
                "RELABELTYPE" => CoercionPathType::RelabelType,
                other => panic!("unexpected node {other}"),
            };
            assert_eq!(
                find_coercion_pathway(catalog, target, *source, Explicit),
                Ok(expected),
                "{}({source})",
                case.name
            );
            checked += 1;
        }
        assert!(checked > 20, "only {checked} casts checked");
    }

    #[test]
    fn a_type_converts_to_text_on_assignment_and_from_text_only_when_asked() {
        let catalog = FixtureCatalog::get();
        let none = Ok(CoercionPathType::None);
        let io = Ok(CoercionPathType::CoerceViaIo);
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INT4, Implicit),
            none
        );
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INT4, Assignment),
            io
        );
        assert_eq!(find_coercion_pathway(catalog, TEXTOID, INT4, Explicit), io);
        assert_eq!(
            find_coercion_pathway(catalog, INT4, TEXTOID, Implicit),
            none
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4, TEXTOID, Assignment),
            none
        );
        assert_eq!(find_coercion_pathway(catalog, INT4, TEXTOID, Explicit), io);
        assert_eq!(find_coercion_pathway(catalog, DATE, INT4, Explicit), none);
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INT4_ARRAY, Explicit),
            io
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4_ARRAY, TEXTOID, Explicit),
            io
        );
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, PG_TYPE_ROW, Explicit),
            io
        );
    }

    #[test]
    fn a_pg_cast_row_gives_the_method_from_its_context_on() {
        let catalog = FixtureCatalog::get();
        let none = Ok(CoercionPathType::None);
        assert_eq!(
            find_coercion_pathway(catalog, INT8, INT4, Implicit),
            Ok(CoercionPathType::Func(481))
        );
        assert_eq!(find_coercion_pathway(catalog, INT4, INT8, Implicit), none);
        assert_eq!(
            find_coercion_pathway(catalog, INT4, INT8, Assignment),
            Ok(CoercionPathType::Func(480))
        );
        assert_eq!(find_coercion_pathway(catalog, INT4, BOOL, Assignment), none);
        assert_eq!(
            find_coercion_pathway(catalog, INT4, BOOL, Explicit),
            Ok(CoercionPathType::Func(2558))
        );
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, VARCHAR, Implicit),
            Ok(CoercionPathType::RelabelType)
        );
        assert_eq!(find_coercion_pathway(catalog, JSONB, JSON, Implicit), none);
        assert_eq!(
            find_coercion_pathway(catalog, JSONB, JSON, Assignment),
            Ok(CoercionPathType::CoerceViaIo)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4, INT4, Implicit),
            Ok(CoercionPathType::RelabelType)
        );
    }

    #[test]
    fn nothing_converts_to_or_from_internal() {
        let catalog = FixtureCatalog::get();
        let none = Ok(CoercionPathType::None);
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INTERNALOID, Explicit),
            none
        );
        assert_eq!(
            find_coercion_pathway(catalog, INTERNALOID, TEXTOID, Explicit),
            none
        );
        assert_eq!(
            can_coerce_type(catalog, &[UNKNOWNOID], &[INTERNALOID], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INTERNALOID], &[INTERNALOID], Implicit),
            Ok(true)
        );
    }

    #[test]
    fn a_cast_between_arrays_whose_elements_convert_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            find_coercion_pathway(catalog, INT8_ARRAY, INT4_ARRAY, Implicit),
            Err(Reason::Array)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4_ARRAY, INT2VECTOROID, Explicit),
            Err(Reason::Array)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4_ARRAY, INT8_ARRAY, Implicit),
            Ok(CoercionPathType::None)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT2VECTOROID, INT4_ARRAY, Explicit),
            Ok(CoercionPathType::None)
        );
    }

    #[test]
    fn an_untyped_literal_can_be_coerced_to_any_type_but_internal() {
        let catalog = FixtureCatalog::get();
        for target in catalog.type_oids() {
            assert_eq!(
                can_coerce_type(catalog, &[UNKNOWNOID], &[target], Implicit),
                Ok(target != INTERNALOID && target != ANYENUMOID),
                "unknown to {target}"
            );
        }
    }

    #[test]
    fn every_argument_must_be_coercible() {
        let catalog = FixtureCatalog::get();
        assert_eq!(can_coerce_type(catalog, &[], &[], Implicit), Ok(true));
        assert_eq!(
            can_coerce_type(catalog, &[INT4, INT2], &[INT8, FLOAT8], Implicit),
            Ok(true)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4, TEXTOID], &[INT8, FLOAT8], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT8, INT4], &[INT4, INT4], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT8, INT4], &[INT4, INT4], Assignment),
            Ok(true)
        );
    }

    fn fits(arguments: &[i64], parameters: &[i64]) -> Result<bool, Reason> {
        can_coerce_type(FixtureCatalog::get(), arguments, parameters, Implicit)
    }

    #[test]
    fn a_parameter_of_type_any_takes_every_argument_but_internal() {
        assert_eq!(fits(&[INT4], &[ANYOID]), Ok(true));
        assert_eq!(fits(&[UNKNOWNOID], &[ANYOID]), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY, INT4RANGE], &[ANYOID, ANYOID]), Ok(true));
        assert_eq!(fits(&[INTERNALOID], &[ANYOID]), Ok(false));
    }

    #[test]
    fn a_polymorphic_parameter_does_not_help_when_another_argument_does_not_fit() {
        assert_eq!(fits(&[INT4, INT4], &[ANYELEMENTOID, INT8]), Ok(true));
        assert_eq!(fits(&[INT4, TEXTOID], &[ANYELEMENTOID, INT8]), Ok(false));
        assert_eq!(fits(&[ANYELEMENTOID], &[ANYELEMENTOID]), Ok(true));
    }

    #[test]
    fn the_arguments_of_anyelement_parameters_must_all_have_one_type() {
        let both = [ANYELEMENTOID, ANYELEMENTOID];
        assert_eq!(fits(&[INT4, INT4], &both), Ok(true));
        assert_eq!(fits(&[INT4, UNKNOWNOID], &both), Ok(true));
        assert_eq!(fits(&[UNKNOWNOID, UNKNOWNOID], &both), Ok(true));
        assert_eq!(fits(&[INT4, INT8], &both), Ok(false));
        assert_eq!(
            fits(&[INT4, INT2], &[ANYELEMENTOID, ANYNONARRAYOID]),
            Ok(false)
        );
    }

    #[test]
    fn an_anyarray_parameter_takes_an_array_of_the_anyelement_type() {
        let array_and_element = [ANYARRAYOID, ANYELEMENTOID];
        assert_eq!(fits(&[INT4_ARRAY], &[ANYARRAYOID]), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY, INT4], &array_and_element), Ok(true));
        assert_eq!(fits(&[UNKNOWNOID, INT4], &array_and_element), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY, INT8], &array_and_element), Ok(false));
        assert_eq!(fits(&[INT4], &[ANYARRAYOID]), Ok(false));
        assert_eq!(fits(&[INT4, INT4], &array_and_element), Ok(false));
        assert_eq!(
            fits(&[INT4_ARRAY, INT8_ARRAY], &[ANYARRAYOID, ANYARRAYOID]),
            Ok(false)
        );
        assert_eq!(
            fits(&[INT4_ARRAY, UNKNOWNOID], &[ANYARRAYOID, ANYARRAYOID]),
            Ok(true)
        );
    }

    #[test]
    fn anynonarray_refuses_an_array_and_anyenum_refuses_what_is_not_an_enum() {
        assert_eq!(fits(&[INT4], &[ANYNONARRAYOID]), Ok(true));
        assert_eq!(fits(&[UNKNOWNOID], &[ANYNONARRAYOID]), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY], &[ANYNONARRAYOID]), Ok(false));
        assert_eq!(
            fits(&[INT4_ARRAY, INT4_ARRAY], &[ANYARRAYOID, ANYNONARRAYOID]),
            Ok(false)
        );
        assert_eq!(fits(&[INT4], &[ANYENUMOID]), Ok(false));
        assert_eq!(fits(&[UNKNOWNOID], &[ANYENUMOID]), Ok(false));
    }

    #[test]
    fn a_range_parameter_refuses_a_type_that_is_not_a_range() {
        for parameter in [
            ANYRANGEOID,
            ANYMULTIRANGEOID,
            ANYCOMPATIBLERANGEOID,
            ANYCOMPATIBLEMULTIRANGEOID,
        ] {
            assert_eq!(fits(&[INT4], &[parameter]), Ok(false), "{parameter}");
            assert_eq!(fits(&[INT4_ARRAY], &[parameter]), Ok(false), "{parameter}");
            assert_eq!(fits(&[UNKNOWNOID], &[parameter]), Ok(true), "{parameter}");
        }
        assert_eq!(fits(&[INT4RANGE], &[ANYMULTIRANGEOID]), Ok(false));
        assert_eq!(fits(&[INT4MULTIRANGE], &[ANYRANGEOID]), Ok(false));
        assert_eq!(fits(&[INT4, INT8], &[ANYRANGEOID, ANYRANGEOID]), Ok(false));
    }

    #[test]
    fn a_range_argument_fits_a_range_parameter() {
        for (argument, parameter) in [
            (INT4RANGE, ANYRANGEOID),
            (INT4RANGE, ANYCOMPATIBLERANGEOID),
            (INT4MULTIRANGE, ANYMULTIRANGEOID),
            (INT4MULTIRANGE, ANYCOMPATIBLEMULTIRANGEOID),
        ] {
            assert_eq!(
                fits(&[argument], &[parameter]),
                Ok(true),
                "{argument} to {parameter}"
            );
        }
        assert_eq!(
            fits(&[INT4RANGE, INT4], &[ANYRANGEOID, ANYELEMENTOID]),
            Ok(true)
        );
        assert_eq!(
            fits(&[INT4RANGE, INT8], &[ANYRANGEOID, ANYELEMENTOID]),
            Ok(false)
        );
        assert_eq!(
            fits(
                &[INT4MULTIRANGE, INT4RANGE],
                &[ANYMULTIRANGEOID, ANYRANGEOID]
            ),
            Ok(true)
        );
        assert_eq!(
            fits(
                &[INT4RANGE, INT8],
                &[ANYCOMPATIBLERANGEOID, ANYCOMPATIBLEOID]
            ),
            Ok(false)
        );
        assert_eq!(
            fits(
                &[INT4RANGE, INT2],
                &[ANYCOMPATIBLERANGEOID, ANYCOMPATIBLEOID]
            ),
            Ok(true)
        );
    }

    #[test]
    fn the_arguments_of_anycompatible_parameters_must_have_a_common_type() {
        let both = [ANYCOMPATIBLEOID, ANYCOMPATIBLEOID];
        let array_and_element = [ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEOID];
        assert_eq!(fits(&[INT4, INT8], &both), Ok(true));
        assert_eq!(fits(&[INT4, UNKNOWNOID], &both), Ok(true));
        assert_eq!(fits(&[INT4, TEXTOID], &both), Ok(false));
        assert_eq!(fits(&[INT4_ARRAY, INT4], &array_and_element), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY, INT8], &array_and_element), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY, NUMERIC], &array_and_element), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY, TEXTOID], &array_and_element), Ok(false));
        assert_eq!(fits(&[INT4, INT4], &array_and_element), Ok(false));
        assert_eq!(fits(&[INT4], &[ANYCOMPATIBLENONARRAYOID]), Ok(true));
        assert_eq!(fits(&[INT4_ARRAY], &[ANYCOMPATIBLENONARRAYOID]), Ok(false));
    }

    fn enforce(
        arguments: &[i64],
        parameters: &[i64],
        result: i64,
    ) -> Result<(Vec<i64>, i64), ResolveError> {
        let mut parameters = parameters.to_vec();
        enforce_generic_type_consistency(
            FixtureCatalog::get(),
            arguments,
            &mut parameters,
            result,
            false,
        )
        .expect("the types are supported")
        .map(|result| (parameters, result))
    }

    #[test]
    fn a_polymorphic_result_takes_the_type_the_arguments_give() {
        assert_eq!(
            enforce(&[INT4], &[ANYELEMENTOID], ANYELEMENTOID),
            Ok((vec![ANYELEMENTOID], INT4))
        );
        assert_eq!(
            enforce(&[INT4], &[ANYELEMENTOID], ANYARRAYOID),
            Ok((vec![ANYELEMENTOID], INT4_ARRAY))
        );
        assert_eq!(
            enforce(&[INT4_ARRAY], &[ANYARRAYOID], ANYELEMENTOID),
            Ok((vec![ANYARRAYOID], INT4))
        );
        assert_eq!(
            enforce(&[INT4_ARRAY, INT4], &[ANYARRAYOID, INT4], ANYARRAYOID),
            Ok((vec![ANYARRAYOID, INT4], INT4_ARRAY))
        );
        assert_eq!(
            enforce(&[INT4], &[ANYNONARRAYOID], TEXTOID),
            Ok((vec![ANYNONARRAYOID], TEXTOID))
        );
    }

    #[test]
    fn a_result_stays_as_declared_when_no_parameter_is_polymorphic() {
        assert_eq!(enforce(&[INT4], &[INT8], INT8), Ok((vec![INT8], INT8)));
        assert_eq!(
            enforce(&[UNKNOWNOID], &[INT4], ANYARRAYOID),
            Ok((vec![INT4], ANYARRAYOID))
        );
        assert_eq!(
            enforce(&[UNKNOWNOID], &[ANYOID], INT4),
            Ok((vec![ANYOID], INT4))
        );
    }

    #[test]
    fn an_untyped_literal_of_a_polymorphic_parameter_takes_the_type_the_others_give() {
        assert_eq!(
            enforce(
                &[INT4_ARRAY, UNKNOWNOID],
                &[ANYARRAYOID, ANYELEMENTOID],
                INT4
            ),
            Ok((vec![ANYARRAYOID, INT4], INT4))
        );
        assert_eq!(
            enforce(
                &[UNKNOWNOID, INT4],
                &[ANYARRAYOID, ANYELEMENTOID],
                ANYARRAYOID
            ),
            Ok((vec![INT4_ARRAY, ANYELEMENTOID], INT4_ARRAY))
        );
        assert_eq!(
            enforce(
                &[INT4_ARRAY, UNKNOWNOID],
                &[ANYARRAYOID, ANYARRAYOID],
                ANYARRAYOID
            ),
            Ok((vec![ANYARRAYOID, INT4_ARRAY], INT4_ARRAY))
        );
    }

    #[test]
    fn anycompatible_parameters_become_the_common_type_of_their_arguments() {
        let array_and_element = [ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEOID];
        assert_eq!(
            enforce(
                &[INT4_ARRAY, INT4],
                &array_and_element,
                ANYCOMPATIBLEARRAYOID
            ),
            Ok((vec![INT4_ARRAY, INT4], INT4_ARRAY))
        );
        assert_eq!(
            enforce(
                &[INT4_ARRAY, INT8],
                &array_and_element,
                ANYCOMPATIBLEARRAYOID
            ),
            Ok((vec![INT8_ARRAY, INT8], INT8_ARRAY))
        );
        assert_eq!(
            enforce(&[INT8_ARRAY, UNKNOWNOID], &array_and_element, INT4),
            Ok((vec![INT8_ARRAY, INT8], INT4))
        );
        assert_eq!(
            enforce(
                &[UNKNOWNOID, UNKNOWNOID],
                &array_and_element,
                ANYCOMPATIBLEARRAYOID
            ),
            Ok((vec![TEXTARRAYOID, TEXTOID], TEXTARRAYOID))
        );
        assert_eq!(
            enforce(
                &[INT4, NUMERIC],
                &[ANYCOMPATIBLEOID, ANYCOMPATIBLENONARRAYOID],
                ANYCOMPATIBLEOID
            ),
            Ok((vec![NUMERIC, NUMERIC], NUMERIC))
        );
    }

    fn enforce_error(
        arguments: &[i64],
        parameters: &[i64],
        result: i64,
    ) -> (&'static str, String, Option<String>) {
        let error = enforce(arguments, parameters, result).expect_err("the call is refused");
        (
            error.sqlstate(),
            error.message().to_string(),
            error.detail().map(str::to_string),
        )
    }

    #[test]
    fn untyped_literals_alone_do_not_give_a_polymorphic_type() {
        assert_eq!(
            enforce_error(&[UNKNOWNOID], &[ANYELEMENTOID], TEXTOID),
            (
                "42804",
                "could not determine polymorphic type because input has type unknown".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[UNKNOWNOID, INT4], &[ANYARRAYOID, INT4], INT4),
            (
                "42804",
                "could not determine polymorphic type because input has type unknown".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[UNKNOWNOID], &[ANYCOMPATIBLERANGEOID], TEXTOID),
            (
                "42804",
                "could not determine polymorphic type anycompatiblerange because input has type unknown"
                    .to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[UNKNOWNOID], &[ANYCOMPATIBLEMULTIRANGEOID], TEXTOID),
            (
                "42804",
                "could not determine polymorphic type anycompatiblemultirange because input has type unknown"
                    .to_string(),
                None
            )
        );
    }

    #[test]
    fn an_anyarray_argument_cannot_be_matched_with_another_polymorphic_parameter() {
        let message = "cannot determine element type of \"anyarray\" argument".to_string();
        assert_eq!(
            enforce_error(
                &[ANYARRAYOID, ANYARRAYOID],
                &[ANYARRAYOID, ANYARRAYOID],
                BOOL
            ),
            ("42804", message.clone(), None)
        );
        assert_eq!(
            enforce_error(&[ANYARRAYOID], &[ANYARRAYOID], ANYELEMENTOID),
            ("42804", message, None)
        );
        assert_eq!(
            enforce(&[ANYARRAYOID, TEXTOID], &[ANYARRAYOID, TEXTOID], TEXTOID),
            Ok((vec![ANYARRAYOID, TEXTOID], TEXTOID))
        );
        assert_eq!(
            enforce(&[ANYARRAYOID], &[ANYARRAYOID], ANYARRAYOID),
            Ok((vec![ANYARRAYOID], ANYARRAYOID))
        );
    }

    #[test]
    fn arguments_that_break_a_polymorphic_rule_are_refused_with_the_two_types() {
        assert_eq!(
            enforce_error(&[INT4, INT8], &[ANYELEMENTOID, ANYELEMENTOID], BOOL),
            (
                "42804",
                "arguments declared \"anyelement\" are not all alike".to_string(),
                Some("integer versus bigint".to_string())
            )
        );
        assert_eq!(
            enforce_error(&[INT4_ARRAY, INT8], &[ANYARRAYOID, ANYELEMENTOID], BOOL),
            (
                "42804",
                "argument declared anyarray is not consistent with argument declared anyelement"
                    .to_string(),
                Some("integer[] versus bigint".to_string())
            )
        );
        assert_eq!(
            enforce_error(&[INT4], &[ANYARRAYOID], BOOL),
            (
                "42804",
                "argument declared anyarray is not an array but type integer".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[INT4], &[ANYRANGEOID], BOOL),
            (
                "42804",
                "argument declared anyrange is not a range type but type integer".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[INT4_ARRAY], &[ANYNONARRAYOID], BOOL),
            (
                "42804",
                "type matched to anynonarray is an array type: integer[]".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[INT4], &[ANYENUMOID], BOOL),
            (
                "42804",
                "type matched to anyenum is not an enum type: integer".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(
                &[INT4, TEXTOID],
                &[ANYCOMPATIBLEOID, ANYCOMPATIBLEOID],
                BOOL
            ),
            (
                "42804",
                "argument types integer and text cannot be matched".to_string(),
                None
            )
        );
        assert_eq!(
            enforce_error(&[ANYELEMENTOID], &[ANYELEMENTOID], ANYARRAYOID),
            (
                "42704",
                "could not find array type for data type anyelement".to_string(),
                None
            )
        );
    }

    #[test]
    fn a_range_argument_decides_the_element_range_and_multirange_types() {
        assert_eq!(
            enforce(&[INT4RANGE], &[ANYRANGEOID], ANYELEMENTOID),
            Ok((vec![ANYRANGEOID], INT4))
        );
        assert_eq!(
            enforce(&[INT4RANGE], &[ANYRANGEOID], ANYMULTIRANGEOID),
            Ok((vec![ANYRANGEOID], INT4MULTIRANGE))
        );
        assert_eq!(
            enforce(&[INT4MULTIRANGE], &[ANYMULTIRANGEOID], ANYRANGEOID),
            Ok((vec![ANYMULTIRANGEOID], INT4RANGE))
        );
        assert_eq!(
            enforce(&[INT4MULTIRANGE], &[ANYMULTIRANGEOID], ANYELEMENTOID),
            Ok((vec![ANYMULTIRANGEOID], INT4))
        );
        assert_eq!(
            enforce(
                &[INT4RANGE, INT2],
                &[ANYCOMPATIBLERANGEOID, ANYCOMPATIBLEOID],
                ANYCOMPATIBLEOID
            ),
            Ok((vec![INT4RANGE, INT4], INT4))
        );
    }

    #[test]
    fn a_composite_argument_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            can_coerce_type(catalog, &[PG_TYPE_ROW], &[RECORDOID], Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            can_coerce_type(catalog, &[RECORDOID], &[PG_TYPE_ROW], Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            can_coerce_type(catalog, &[PG_TYPE_ROW], &[INT4], Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4], &[PG_TYPE_ROW], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[RECORDOID], &[INT4], Implicit),
            Ok(false)
        );
    }

    #[test]
    fn a_type_the_catalog_does_not_have_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            find_coercion_pathway(catalog, INT4, 999_999, Implicit),
            Err(Reason::MissingType)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4], &[999_999], Implicit),
            Err(Reason::MissingType)
        );
        assert_eq!(type_category(catalog, 999_999), Err(Reason::MissingType));
    }

    #[test]
    fn binary_coercible_means_an_implicit_cast_that_calls_no_function() {
        let catalog = FixtureCatalog::get();
        assert_eq!(is_binary_coercible(catalog, INT4, INT4), Ok(true));
        assert_eq!(is_binary_coercible(catalog, VARCHAR, TEXTOID), Ok(true));
        assert_eq!(is_binary_coercible(catalog, TEXTOID, VARCHAR), Ok(true));
        assert_eq!(is_binary_coercible(catalog, INT4, OID), Ok(true));
        assert_eq!(is_binary_coercible(catalog, INT4, REGCLASS), Ok(true));
        assert_eq!(is_binary_coercible(catalog, OID, INT4), Ok(false));
        assert_eq!(is_binary_coercible(catalog, INT4, INT8), Ok(false));
        assert_eq!(is_binary_coercible(catalog, BPCHAR, TEXTOID), Ok(false));
        assert_eq!(is_binary_coercible(catalog, INT4, TEXTOID), Ok(false));
        assert_eq!(
            is_binary_coercible(catalog, 999_999, INT4),
            Err(Reason::MissingType)
        );
    }

    #[test]
    fn a_type_is_binary_coercible_to_the_polymorphic_type_of_its_kind() {
        let catalog = FixtureCatalog::get();
        for source in [INT4, INT4_ARRAY, INT4RANGE, PG_TYPE_ROW] {
            assert_eq!(is_binary_coercible(catalog, source, ANYOID), Ok(true));
            assert_eq!(
                is_binary_coercible(catalog, source, ANYELEMENTOID),
                Ok(true)
            );
            assert_eq!(
                is_binary_coercible(catalog, source, ANYCOMPATIBLEOID),
                Ok(true)
            );
        }
        let accepted = [
            (INT4_ARRAY, ANYARRAYOID),
            (INT4_ARRAY, ANYCOMPATIBLEARRAYOID),
            (INT4, ANYNONARRAYOID),
            (INT4, ANYCOMPATIBLENONARRAYOID),
            (INT4RANGE, ANYRANGEOID),
            (INT4RANGE, ANYCOMPATIBLERANGEOID),
            (INT4MULTIRANGE, ANYMULTIRANGEOID),
            (INT4MULTIRANGE, ANYCOMPATIBLEMULTIRANGEOID),
            (PG_TYPE_ROW, RECORDOID),
        ];
        for (source, target) in accepted {
            assert_eq!(
                is_binary_coercible(catalog, source, target),
                Ok(true),
                "{source} to {target}"
            );
        }
        let refused = [
            (INT4, ANYARRAYOID),
            (INT4_ARRAY, ANYNONARRAYOID),
            (INT4, ANYENUMOID),
            (INT4, ANYRANGEOID),
            (INT4RANGE, ANYMULTIRANGEOID),
            (INT4, RECORDOID),
            (INT4_ARRAY, RECORDARRAYOID),
        ];
        for (source, target) in refused {
            assert_eq!(
                is_binary_coercible(catalog, source, target),
                Ok(false),
                "{source} to {target}"
            );
        }
    }

    #[test]
    fn each_category_has_its_preferred_types() {
        let catalog = FixtureCatalog::get();
        assert_eq!(type_category(catalog, INT4), Ok('N'));
        assert_eq!(type_category(catalog, VARCHAR), Ok(TYPCATEGORY_STRING));
        assert_eq!(type_category(catalog, UNKNOWNOID), Ok('X'));
        assert_eq!(type_category(catalog, INT4_ARRAY), Ok('A'));
        assert_eq!(is_preferred_type(catalog, 'N', FLOAT8), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'N', OID), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'N', NUMERIC), Ok(false));
        assert_eq!(is_preferred_type(catalog, 'S', TEXTOID), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'S', VARCHAR), Ok(false));
        assert_eq!(is_preferred_type(catalog, 'D', TIMESTAMPTZ), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'D', TIMESTAMP), Ok(false));
        assert_eq!(is_preferred_type(catalog, 'S', FLOAT8), Ok(false));
        assert_eq!(
            is_preferred_type(catalog, TYPCATEGORY_INVALID, FLOAT8),
            Ok(true)
        );
        assert_eq!(
            is_preferred_type(catalog, TYPCATEGORY_INVALID, INT4),
            Ok(false)
        );
    }

    #[test]
    fn the_common_type_is_the_one_the_others_convert_to_implicitly() {
        let catalog = FixtureCatalog::get();
        let common = |types: &[i64]| select_common_type(catalog, types);
        assert_eq!(common(&[INT4]), Ok(CommonType::Type(INT4)));
        assert_eq!(common(&[INT4, INT4, INT4]), Ok(CommonType::Type(INT4)));
        assert_eq!(common(&[INT8, INT4]), Ok(CommonType::Type(INT8)));
        assert_eq!(common(&[INT4, INT8]), Ok(CommonType::Type(INT8)));
        assert_eq!(common(&[INT4, INT2, INT8]), Ok(CommonType::Type(INT8)));
        assert_eq!(common(&[NUMERIC, INT4]), Ok(CommonType::Type(NUMERIC)));
        assert_eq!(common(&[FLOAT4, NUMERIC]), Ok(CommonType::Type(FLOAT4)));
        assert_eq!(common(&[NUMERIC, FLOAT8]), Ok(CommonType::Type(FLOAT8)));
        assert_eq!(common(&[INT8, FLOAT4]), Ok(CommonType::Type(FLOAT4)));
        assert_eq!(common(&[TIMESTAMP, DATE]), Ok(CommonType::Type(TIMESTAMP)));
        assert_eq!(
            common(&[DATE, TIMESTAMPTZ]),
            Ok(CommonType::Type(TIMESTAMPTZ))
        );
        assert_eq!(common(&[NAME, VARCHAR]), Ok(CommonType::Type(NAME)));
        assert_eq!(common(&[VARCHAR, NAME]), Ok(CommonType::Type(NAME)));
    }

    #[test]
    fn the_common_type_stays_on_the_first_type_when_both_convert_or_it_is_preferred() {
        let catalog = FixtureCatalog::get();
        let common = |types: &[i64]| select_common_type(catalog, types);
        assert_eq!(common(&[TEXTOID, VARCHAR]), Ok(CommonType::Type(TEXTOID)));
        assert_eq!(common(&[VARCHAR, TEXTOID]), Ok(CommonType::Type(VARCHAR)));
        assert_eq!(common(&[VARCHAR, BPCHAR]), Ok(CommonType::Type(VARCHAR)));
        assert_eq!(common(&[OID, INT4]), Ok(CommonType::Type(OID)));
        assert_eq!(common(&[INT4, OID]), Ok(CommonType::Type(OID)));
    }

    #[test]
    fn untyped_literals_take_the_type_of_the_others_or_text() {
        let catalog = FixtureCatalog::get();
        let common = |types: &[i64]| select_common_type(catalog, types);
        assert_eq!(common(&[UNKNOWNOID]), Ok(CommonType::Type(TEXTOID)));
        assert_eq!(
            common(&[UNKNOWNOID, UNKNOWNOID]),
            Ok(CommonType::Type(TEXTOID))
        );
        assert_eq!(common(&[DATE, UNKNOWNOID]), Ok(CommonType::Type(DATE)));
        assert_eq!(common(&[UNKNOWNOID, INT4]), Ok(CommonType::Type(INT4)));
        assert_eq!(
            common(&[UNKNOWNOID, INT4, UNKNOWNOID, INT8]),
            Ok(CommonType::Type(INT8))
        );
    }

    #[test]
    fn types_of_different_categories_have_no_common_type() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            select_common_type(catalog, &[TEXTOID, INT4]),
            Ok(CommonType::CannotBeMatched(TEXTOID, INT4))
        );
        assert_eq!(
            select_common_type(catalog, &[INT4, BOOL]),
            Ok(CommonType::CannotBeMatched(INT4, BOOL))
        );
        assert_eq!(
            select_common_type(catalog, &[UNKNOWNOID, INT4, INT8, BOOL]),
            Ok(CommonType::CannotBeMatched(INT8, BOOL))
        );
        assert_eq!(
            select_common_type(catalog, &[INT4, 999_999]),
            Err(Reason::MissingType)
        );
    }

    #[test]
    fn an_argument_is_coerced_the_way_its_implicit_cast_says() {
        let catalog = FixtureCatalog::get();
        let coerce = |input, target| coerce_type(catalog, input, target, Implicit);
        assert_eq!(coerce(INT4, INT4), Ok(ArgCoercion::None));
        assert_eq!(coerce(INT4, INT8), Ok(ArgCoercion::Function(481)));
        assert_eq!(coerce(VARCHAR, TEXTOID), Ok(ArgCoercion::Relabel));
        assert_eq!(coerce(INT4, OID), Ok(ArgCoercion::Relabel));
        assert_eq!(coerce(UNKNOWNOID, DATE), Ok(ArgCoercion::Literal));
        assert_eq!(coerce(UNKNOWNOID, UNKNOWNOID), Ok(ArgCoercion::None));
        assert_eq!(
            coerce_type(catalog, JSON, JSONB, Assignment),
            Ok(ArgCoercion::ViaInputOutput)
        );
        assert_eq!(
            coerce_type(catalog, INT8, INT4, Assignment),
            Ok(ArgCoercion::Function(480))
        );
    }

    #[test]
    fn an_argument_of_a_polymorphic_parameter_is_left_as_it_is() {
        let catalog = FixtureCatalog::get();
        for (input, target) in [
            (INT4, ANYOID),
            (INT4, ANYELEMENTOID),
            (UNKNOWNOID, ANYELEMENTOID),
            (INT4, ANYCOMPATIBLEOID),
            (INT4_ARRAY, ANYARRAYOID),
            (INT4RANGE, ANYRANGEOID),
        ] {
            assert_eq!(
                coerce_type(catalog, input, target, Implicit),
                Ok(ArgCoercion::None),
                "{input} to {target}"
            );
        }
        assert_eq!(
            coerce_type(catalog, UNKNOWNOID, ANYARRAYOID, Implicit),
            Ok(ArgCoercion::Literal)
        );
    }

    #[test]
    fn coercing_an_argument_between_arrays_or_composites_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            coerce_type(catalog, INT4_ARRAY, INT8_ARRAY, Implicit),
            Err(Reason::Array)
        );
        assert_eq!(
            coerce_type(catalog, PG_TYPE_ROW, RECORDOID, Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            coerce_type(catalog, RECORDOID, PG_TYPE_ROW, Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            coerce_type(catalog, INT4, 999_999, Implicit),
            Err(Reason::MissingType)
        );
    }
}
