use quote::quote;
use std::collections::HashMap;

use proc_macro::TokenStream;
use syn::{parse_macro_input, spanned::Spanned, Arm, ExprMatch, Lit, Pat};

pub fn match_ignore_ascci_case(input: TokenStream) -> TokenStream {
    let match_block = parse_macro_input!(input as ExprMatch);
    if match_block.arms.is_empty() {
        return syn::Error::new(
            match_block.span(),
            "expected at least one arm with literal string/byte/bytes/char",
        )
        .to_compile_error()
        .into();
    }
    let mut arms: Vec<(Vec<u8>, Arm)> = Vec::with_capacity(match_block.arms.len());
    let mut fallback_arm: Option<Arm> = None;
    for arm in &match_block.arms {
        match &arm.pat {
            Pat::Lit(lit) => match &lit.lit {
                Lit::ByteStr(bs) => {
                    arms.push((bs.value().to_ascii_uppercase(), arm.clone()));
                }
                _ => {
                    return syn::Error::new(
                        arm.pat.span().span(),
                        "expected literal string/byte/bytes/char",
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Pat::Wild(_) => {
                fallback_arm = Some(arm.clone());
            }
            Pat::Or(or) => {
                for case in &or.cases {
                    match case {
                        Pat::Lit(lit) => match &lit.lit {
                            Lit::ByteStr(bs) => {
                                arms.push((bs.value().to_ascii_uppercase(), arm.clone()));
                            }
                            _ => {
                                return syn::Error::new(
                                    arm.pat.span().span(),
                                    "expected literal string/byte/bytes/char",
                                )
                                .to_compile_error()
                                .into();
                            }
                        },
                        _ => {
                            return syn::Error::new(
                                arm.pat.span().span(),
                                "expected literal string/byte/bytes/char",
                            )
                            .to_compile_error()
                            .into();
                        }
                    }
                }
            }
            _b => {
                return syn::Error::new(
                    arm.pat.span().span(),
                    "expected literal string/byte/bytes/char",
                )
                .to_compile_error()
                .into();
            }
        }
    }

    struct PathEntry {
        result: Option<Arm>,
        sub_entries: HashMap<u8, Box<PathEntry>>,
    }

    let mut paths = Box::new(PathEntry {
        result: None,
        sub_entries: HashMap::new(),
    });

    for (keyword_b, arm) in arms.drain(..) {
        let mut current = &mut paths;

        for b in keyword_b {
            match current.sub_entries.get(&b) {
                Some(_) => {
                    current = current.sub_entries.get_mut(&b).unwrap();
                }
                None => {
                    let new_entry = Box::new(PathEntry {
                        result: None,
                        sub_entries: HashMap::new(),
                    });
                    current.sub_entries.insert(b, new_entry);
                    current = current.sub_entries.get_mut(&b).unwrap();
                }
            }
        }

        assert!(current.result.is_none());
        current.result = Some(arm);
    }

    fn write_entry(
        idx: usize,
        var_name: proc_macro2::TokenStream,
        fallback_arm: Option<Arm>,
        entry: &PathEntry,
    ) -> proc_macro2::TokenStream {
        let eof_handle = if let Some(ref result) = entry.result {
            let guard = if let Some(ref b) = result.guard {
                let expr = &b.1;
                quote! { if #expr }
            } else {
                quote! {}
            };
            let body = &result.body;
            quote! { None #guard => { #body } }
        } else {
            quote! {}
        };

        let fallback_handle = if let Some(ref result) = fallback_arm {
            let body = &result.body;
            quote! { _ => { #body } }
        } else {
            quote! {}
        };

        let mut arms = Vec::with_capacity(entry.sub_entries.len());
        for (&b, sub_entry) in &entry.sub_entries {
            let sub_match = write_entry(idx + 1, var_name.clone(), fallback_arm.clone(), sub_entry);
            if b.is_ascii_alphabetic() {
                let b_lower = b.to_ascii_lowercase();
                arms.push(quote! { Some(#b) | Some(#b_lower) => #sub_match });
            } else {
                arms.push(quote! { Some(#b) => #sub_match });
            }
        }

        quote! { match #var_name.get(#idx) {
            #eof_handle
            #(#arms)*
            #fallback_handle
        } }
    }

    let expr = match_block.expr;
    TokenStream::from(write_entry(0, quote! { #expr }, fallback_arm, &paths))
}
