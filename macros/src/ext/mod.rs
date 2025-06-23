use proc_macro::TokenStream;
use quote::quote;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::token::Eq;
use syn::{parse_macro_input, Ident, LitStr, Token};
mod agg_derive;
mod scalars;
mod vfs_derive;
mod vtab_derive;
pub use agg_derive::derive_agg_func;
pub use scalars::scalar;
pub use vfs_derive::derive_vfs_module;
pub use vtab_derive::derive_vtab_module;

pub fn register_extension(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as RegisterExtensionInput);
    let RegisterExtensionInput {
        aggregates,
        scalars,
        vtabs,
        vfs,
    } = input_ast;

    let scalar_calls = scalars.iter().map(|scalar_ident| {
        let register_fn = syn::Ident::new(&format!("register_{scalar_ident}"), scalar_ident.span());
        quote! {
            {
                let result = unsafe { #register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });

    let aggregate_calls = aggregates.iter().map(|agg_ident| {
        let register_fn = syn::Ident::new(&format!("register_{agg_ident}"), agg_ident.span());
        quote! {
            {
                let result = unsafe{ #agg_ident::#register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let vtab_calls = vtabs.iter().map(|vtab_ident| {
        let register_fn = syn::Ident::new(&format!("register_{vtab_ident}"), vtab_ident.span());
        quote! {
            {
                let result = unsafe{ #vtab_ident::#register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let vfs_calls = vfs.iter().map(|vfs_ident| {
        let register_fn = syn::Ident::new(&format!("register_{vfs_ident}"), vfs_ident.span());
        quote! {
            {
                let result = unsafe { #register_fn(api) };
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let static_vfs = vfs.iter().map(|vfs_ident| {
        let static_register =
            syn::Ident::new(&format!("register_static_{vfs_ident}"), vfs_ident.span());
        quote! {
            {
                    let result = api.add_builtin_vfs(unsafe { #static_register()});
                    if !result.is_ok() {
                        return result;
                }
            }
        }
    });
    let static_aggregates = aggregate_calls.clone();
    let static_scalars = scalar_calls.clone();
    let static_vtabs = vtab_calls.clone();

    let expanded = quote! {
    #[cfg(not(target_family = "wasm"))]
    #[cfg(not(feature = "static"))]
    #[global_allocator]
    static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

            #[cfg(feature = "static")]
            pub unsafe extern "C" fn register_extension_static(api: &mut ::turso_ext::ExtensionApi) -> ::turso_ext::ResultCode {
                #(#static_scalars)*

                #(#static_aggregates)*

                #(#static_vtabs)*

                #[cfg(not(target_family = "wasm"))]
                #(#static_vfs)*

                ::turso_ext::ResultCode::OK
              }

            #[cfg(not(feature = "static"))]
            #[no_mangle]
            pub unsafe extern "C" fn register_extension(api: &::turso_ext::ExtensionApi) -> ::turso_ext::ResultCode {
                #(#scalar_calls)*

                #(#aggregate_calls)*

                #(#vtab_calls)*

                #(#vfs_calls)*

                ::turso_ext::ResultCode::OK
            }
        };

    TokenStream::from(expanded)
}

pub(crate) struct RegisterExtensionInput {
    pub aggregates: Vec<Ident>,
    pub scalars: Vec<Ident>,
    pub vtabs: Vec<Ident>,
    pub vfs: Vec<Ident>,
}

impl syn::parse::Parse for RegisterExtensionInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut aggregates = Vec::new();
        let mut scalars = Vec::new();
        let mut vtabs = Vec::new();
        let mut vfs = Vec::new();
        while !input.is_empty() {
            if input.peek(syn::Ident) && input.peek2(Token![:]) {
                let section_name: Ident = input.parse()?;
                input.parse::<Token![:]>()?;
                let names = ["aggregates", "scalars", "vtabs", "vfs"];
                if names.contains(&section_name.to_string().as_str()) {
                    let content;
                    syn::braced!(content in input);
                    let parsed_items = Punctuated::<Ident, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();

                    match section_name.to_string().as_str() {
                        "aggregates" => aggregates = parsed_items,
                        "scalars" => scalars = parsed_items,
                        "vtabs" => vtabs = parsed_items,
                        "vfs" => vfs = parsed_items,
                        _ => unreachable!(),
                    };

                    if input.peek(Token![,]) {
                        input.parse::<Token![,]>()?;
                    }
                } else {
                    return Err(syn::Error::new(section_name.span(), "Unknown section"));
                }
            } else {
                return Err(input.error("Expected aggregates:, scalars:, or vtabs: section"));
            }
        }

        Ok(Self {
            aggregates,
            scalars,
            vtabs,
            vfs,
        })
    }
}

pub(crate) struct ScalarInfo {
    pub name: String,
    pub alias: Option<String>,
}

impl ScalarInfo {
    pub fn new(name: String, alias: Option<String>) -> Self {
        Self { name, alias }
    }
}

impl syn::parse::Parse for ScalarInfo {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        let mut name = None;
        let mut alias = None;
        while !input.is_empty() {
            if let Ok(ident) = input.parse::<Ident>() {
                if ident.to_string().as_str() == "name" {
                    let _ = input.parse::<Eq>();
                    name = Some(input.parse::<LitStr>()?);
                } else if ident.to_string().as_str() == "alias" {
                    let _ = input.parse::<Eq>();
                    alias = Some(input.parse::<LitStr>()?);
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        let Some(name) = name else {
            return Err(input.error("Expected name"));
        };
        Ok(Self::new(name.value(), alias.map(|i| i.value())))
    }
}
