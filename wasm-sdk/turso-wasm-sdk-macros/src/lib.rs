use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, PatType, ReturnType, Type};

/// Classify a parameter type for argument reading.
enum ParamKind {
    I64,
    F64,
    Str,
    Bytes,
    Option(Box<ParamKind>),
}

/// Classify a return type for result writing.
enum RetKind {
    I64,
    F64,
    Str,
    String,
    Bytes,
    VecU8,
    Unit,
    Option(Box<RetKind>),
}

fn last_segment_ident(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()),
        _ => None,
    }
}

fn first_generic_type(ty: &Type) -> Option<&Type> {
    match ty {
        Type::Path(p) => {
            let seg = p.path.segments.last()?;
            match &seg.arguments {
                syn::PathArguments::AngleBracketed(args) => {
                    args.args.first().and_then(|a| match a {
                        syn::GenericArgument::Type(t) => Some(t),
                        _ => None,
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn classify_param(ty: &Type) -> ParamKind {
    match ty {
        Type::Path(_) => {
            let ident = last_segment_ident(ty).expect("unsupported parameter type");
            match ident.as_str() {
                "i64" => ParamKind::I64,
                "f64" => ParamKind::F64,
                "Option" => {
                    let inner = first_generic_type(ty).expect("Option must have a type argument");
                    ParamKind::Option(Box::new(classify_param(inner)))
                }
                other => panic!("unsupported parameter type: {other}"),
            }
        }
        Type::Reference(r) => match &*r.elem {
            Type::Path(p) => {
                let ident = p.path.segments.last().unwrap().ident.to_string();
                if ident == "str" {
                    ParamKind::Str
                } else {
                    panic!("unsupported reference parameter type: &{ident}")
                }
            }
            Type::Slice(s) => {
                let ident = last_segment_ident(&s.elem).expect("unsupported slice element type");
                if ident == "u8" {
                    ParamKind::Bytes
                } else {
                    panic!("unsupported slice parameter type: &[{ident}]")
                }
            }
            _ => panic!("unsupported reference parameter type"),
        },
        _ => panic!("unsupported parameter type"),
    }
}

fn classify_ret(output: &ReturnType) -> RetKind {
    match output {
        ReturnType::Default => RetKind::Unit,
        ReturnType::Type(_, ty) => classify_ret_type(ty),
    }
}

fn classify_ret_type(ty: &Type) -> RetKind {
    match ty {
        Type::Path(_) => {
            let ident = last_segment_ident(ty).expect("unsupported return type");
            match ident.as_str() {
                "i64" => RetKind::I64,
                "f64" => RetKind::F64,
                "String" => RetKind::String,
                "Vec" => {
                    let inner = first_generic_type(ty).expect("Vec must have a type argument");
                    let inner_ident = last_segment_ident(inner).expect("unsupported Vec element");
                    if inner_ident == "u8" {
                        RetKind::VecU8
                    } else {
                        panic!("unsupported return type: Vec<{inner_ident}>")
                    }
                }
                "Option" => {
                    let inner = first_generic_type(ty).expect("Option must have a type argument");
                    RetKind::Option(Box::new(classify_ret_type(inner)))
                }
                other => panic!("unsupported return type: {other}"),
            }
        }
        Type::Reference(r) => match &*r.elem {
            Type::Path(p) => {
                let ident = p.path.segments.last().unwrap().ident.to_string();
                if ident == "str" {
                    RetKind::Str
                } else {
                    panic!("unsupported reference return type: &{ident}")
                }
            }
            Type::Slice(s) => {
                let ident = last_segment_ident(&s.elem).expect("unsupported slice element type");
                if ident == "u8" {
                    RetKind::Bytes
                } else {
                    panic!("unsupported slice return type: &[{ident}]")
                }
            }
            _ => panic!("unsupported reference return type"),
        },
        Type::Tuple(t) if t.elems.is_empty() => RetKind::Unit,
        _ => panic!("unsupported return type"),
    }
}

// ── Type encoding for turso_sig section ────────────────────────────────────

/// Encode a ParamKind to the turso_sig byte.
fn param_sig_byte(kind: &ParamKind) -> u8 {
    match kind {
        ParamKind::I64 => 0x01,
        ParamKind::F64 => 0x02,
        ParamKind::Str => 0x03,
        ParamKind::Bytes => 0x04,
        ParamKind::Option(inner) => match inner.as_ref() {
            ParamKind::I64 => 0x05,
            ParamKind::F64 => 0x06,
            ParamKind::Str => 0x07,
            ParamKind::Bytes => 0x08,
            _ => panic!("unsupported Option<Option<...>> nesting"),
        },
    }
}

/// Encode a RetKind to the turso_sig byte.
fn ret_sig_byte(kind: &RetKind) -> u8 {
    match kind {
        RetKind::I64 => 0x01,
        RetKind::F64 => 0x02,
        RetKind::Str | RetKind::String => 0x03,
        RetKind::Bytes | RetKind::VecU8 => 0x04,
        RetKind::Unit => 0x09,
        RetKind::Option(inner) => match inner.as_ref() {
            RetKind::I64 => 0x05,
            RetKind::F64 => 0x06,
            RetKind::Str | RetKind::String => 0x07,
            RetKind::Bytes | RetKind::VecU8 => 0x08,
            _ => panic!("unsupported Option<Option<...>> nesting"),
        },
    }
}

// ── Argv slot layout generation ────────────────────────────────────────────

/// Generate return expression for the (argc, argv) -> i64 convention.
/// All returns are i64; the host uses turso_sig to interpret.
fn gen_return_expr(kind: &RetKind, val: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    match kind {
        RetKind::I64 => quote! { #val },
        RetKind::F64 => quote! { (#val).to_bits() as i64 },
        RetKind::Str => quote! { ::turso_wasm_sdk::ret_packed_text(#val) },
        RetKind::String => quote! { ::turso_wasm_sdk::ret_packed_text(&#val) },
        RetKind::Bytes => quote! { ::turso_wasm_sdk::ret_packed_blob(#val) },
        RetKind::VecU8 => quote! { ::turso_wasm_sdk::ret_packed_blob(&#val) },
        RetKind::Unit => quote! { { let _ = #val; 0i64 } },
        RetKind::Option(inner) => {
            let inner_ret = gen_return_expr(inner, quote! { __v });
            match inner.as_ref() {
                RetKind::I64 => {
                    quote! {
                        match #val {
                            Some(__v) => {
                                unsafe { *(::turso_wasm_sdk::NULL_FLAG_OFFSET as *mut u8) = 0; }
                                #inner_ret
                            }
                            None => {
                                unsafe { *(::turso_wasm_sdk::NULL_FLAG_OFFSET as *mut u8) = 1; }
                                0i64
                            }
                        }
                    }
                }
                RetKind::F64 => {
                    quote! {
                        match #val {
                            Some(__v) => {
                                unsafe { *(::turso_wasm_sdk::NULL_FLAG_OFFSET as *mut u8) = 0; }
                                #inner_ret
                            }
                            None => {
                                unsafe { *(::turso_wasm_sdk::NULL_FLAG_OFFSET as *mut u8) = 1; }
                                0i64
                            }
                        }
                    }
                }
                RetKind::Str | RetKind::String | RetKind::Bytes | RetKind::VecU8 => {
                    // ptr=0 means null
                    quote! {
                        match #val {
                            Some(__v) => { #inner_ret }
                            None => 0i64,
                        }
                    }
                }
                _ => panic!("unsupported Option<Option<...>> nesting"),
            }
        }
    }
}

/// Transforms a natural Rust function into a WASM UDF export using the
/// `(argc: i32, argv: i32) -> i64` convention with turso_sig type knowledge.
///
/// The argv array contains raw typed values (no tag bytes). The host writes
/// values according to turso_sig; the guest reads them knowing the exact types.
///
/// # Supported parameter types
/// `i64`, `f64`, `&str`, `&[u8]`, `Option<T>` (where T is any of the above)
///
/// # Supported return types
/// `i64`, `f64`, `&str`, `String`, `&[u8]`, `Vec<u8>`, `()`, `Option<T>`
#[proc_macro_attribute]
pub fn turso_wasm(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let name = &func.sig.ident;
    let vis = &func.vis;

    // Build the inner function (renamed to avoid conflict)
    let mut inner_fn = func.clone();
    inner_fn.sig.ident = syn::Ident::new("__inner", name.span());
    inner_fn.vis = syn::Visibility::Inherited;
    inner_fn.attrs.retain(|a| !a.path().is_ident("turso_wasm"));

    // Classify params and build argv reading code
    let mut param_kinds = Vec::new();
    let mut argv_reads = Vec::new();
    let mut call_args = Vec::new();
    let mut slot: usize = 0;

    for (i, arg) in func.sig.inputs.iter().enumerate() {
        let FnArg::Typed(PatType { pat, ty, .. }) = arg else {
            panic!("#[turso_wasm] functions cannot have `self` parameters");
        };
        let kind = classify_param(ty);
        let slot_idx = syn::Index::from(slot);

        match &kind {
            ParamKind::I64 => {
                argv_reads.push(quote! {
                    let #pat: i64 = *__argv.add(#slot_idx);
                });
                slot += 1;
            }
            ParamKind::F64 => {
                argv_reads.push(quote! {
                    let #pat: f64 = f64::from_bits(*__argv.add(#slot_idx) as u64);
                });
                slot += 1;
            }
            ParamKind::Str => {
                argv_reads.push(quote! {
                    let __packed = *__argv.add(#slot_idx) as u64;
                    let __ptr = (__packed & 0xFFFF_FFFF) as usize;
                    let __len = (__packed >> 32) as usize;
                    let #pat = core::str::from_utf8_unchecked(
                        core::slice::from_raw_parts(__ptr as *const u8, __len)
                    );
                });
                slot += 1;
            }
            ParamKind::Bytes => {
                argv_reads.push(quote! {
                    let __packed = *__argv.add(#slot_idx) as u64;
                    let __ptr = (__packed & 0xFFFF_FFFF) as usize;
                    let __len = (__packed >> 32) as usize;
                    let #pat = core::slice::from_raw_parts(__ptr as *const u8, __len);
                });
                slot += 1;
            }
            ParamKind::Option(inner) => match inner.as_ref() {
                ParamKind::I64 => {
                    let slot_val = syn::Index::from(slot + 1);
                    argv_reads.push(quote! {
                        let #pat: Option<i64> = if *__argv.add(#slot_idx) != 0 {
                            None
                        } else {
                            Some(*__argv.add(#slot_val))
                        };
                    });
                    slot += 2;
                }
                ParamKind::F64 => {
                    let slot_val = syn::Index::from(slot + 1);
                    argv_reads.push(quote! {
                        let #pat: Option<f64> = if *__argv.add(#slot_idx) != 0 {
                            None
                        } else {
                            Some(f64::from_bits(*__argv.add(#slot_val) as u64))
                        };
                    });
                    slot += 2;
                }
                ParamKind::Str => {
                    argv_reads.push(quote! {
                        let __packed = *__argv.add(#slot_idx) as u64;
                        let __ptr = (__packed & 0xFFFF_FFFF) as usize;
                        let __len = (__packed >> 32) as usize;
                        let #pat: Option<&str> = if __ptr == 0 {
                            None
                        } else {
                            Some(core::str::from_utf8_unchecked(
                                core::slice::from_raw_parts(__ptr as *const u8, __len)
                            ))
                        };
                    });
                    slot += 1;
                }
                ParamKind::Bytes => {
                    argv_reads.push(quote! {
                        let __packed = *__argv.add(#slot_idx) as u64;
                        let __ptr = (__packed & 0xFFFF_FFFF) as usize;
                        let __len = (__packed >> 32) as usize;
                        let #pat: Option<&[u8]> = if __ptr == 0 {
                            None
                        } else {
                            Some(core::slice::from_raw_parts(__ptr as *const u8, __len))
                        };
                    });
                    slot += 1;
                }
                _ => panic!("unsupported Option<Option<...>> nesting"),
            },
        }

        let _ = i; // suppress unused warning
        call_args.push(quote! { #pat });
        param_kinds.push(kind);
    }

    let ret_kind = classify_ret(&func.sig.output);
    let ret_expr = gen_return_expr(&ret_kind, quote! { __ret });

    // Build the turso_sig custom section payload:
    // [name_len: u8][name: bytes][narg: u8][param_types: narg bytes][ret_type: u8]
    let name_str = name.to_string();
    let name_bytes = name_str.as_bytes();
    let name_len = name_bytes.len() as u8;
    let narg = param_kinds.len() as u8;
    let param_sig_bytes: Vec<u8> = param_kinds.iter().map(param_sig_byte).collect();
    let ret_byte = ret_sig_byte(&ret_kind);

    let payload: Vec<u8> = std::iter::once(name_len)
        .chain(name_bytes.iter().copied())
        .chain(std::iter::once(narg))
        .chain(param_sig_bytes.iter().copied())
        .chain(std::iter::once(ret_byte))
        .collect();
    let payload_len = payload.len();

    let static_name = syn::Ident::new(
        &format!("_TURSO_SIG_{}", name_str.to_uppercase()),
        name.span(),
    );

    let expanded = quote! {
        #[no_mangle]
        #vis unsafe extern "C" fn #name(__argc: i32, __argv_ptr: i32) -> i64 {
            #inner_fn

            let __argv = __argv_ptr as *const i64;
            #(#argv_reads)*
            let __ret = __inner(#(#call_args),*);
            #ret_expr
        }

        const _: () = {
            #[link_section = "turso_sig"]
            #[used]
            static #static_name: [u8; #payload_len] = [#(#payload),*];
        };
    };

    expanded.into()
}
