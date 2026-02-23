//! Procedural macros for sql_gen.
//!
//! Provides the `#[trace_gen(Origin::X)]` attribute macro for automatic scope tracing.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use syn::{
    FnArg, ItemFn, Pat, Path, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// Attribute to automatically wrap a generator function body in a tracing scope.
///
/// # Usage
///
/// ```ignore
/// #[trace_gen(Origin::Insert)]
/// pub fn generate_insert<C: Capabilities>(
///     generator: &SqlGen<C>,
///     ctx: &mut Context,
/// ) -> Result<Stmt, GenError> {
///     // function body - will be wrapped in ctx.scope(Origin::Insert, |ctx| { ... })
/// }
/// ```
///
/// # Requirements
///
/// - The function must have a parameter of type `&mut Context`
/// - The Origin path must be valid in the scope where the function is defined
#[proc_macro_attribute]
pub fn trace_gen(attr: TokenStream, item: TokenStream) -> TokenStream {
    let origin = parse_macro_input!(attr as OriginArg);
    let input_fn = parse_macro_input!(item as ItemFn);

    match transform_function(origin.path, input_fn) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct OriginArg {
    path: Path,
}

impl Parse for OriginArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let path: Path = input.parse()?;
        Ok(OriginArg { path })
    }
}

fn transform_function(origin: Path, mut func: ItemFn) -> syn::Result<TokenStream2> {
    // Find the Context parameter
    let ctx_param_name = find_context_param(&func)?;

    // Get the original function body
    let original_body = &func.block;

    // Create the new body wrapped in scope
    let new_body: syn::Block = syn::parse_quote! {
        {
            #ctx_param_name.scope(#origin, |#ctx_param_name| #original_body)
        }
    };

    func.block = Box::new(new_body);

    Ok(func.to_token_stream())
}

fn find_context_param(func: &ItemFn) -> syn::Result<syn::Ident> {
    for arg in &func.sig.inputs {
        if let FnArg::Typed(pat_type) = arg {
            // Check if the type is &mut Context
            if is_mut_context_type(&pat_type.ty) {
                // Extract the parameter name
                if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                    return Ok(pat_ident.ident.clone());
                }
            }
        }
    }

    Err(syn::Error::new_spanned(
        &func.sig,
        "trace_gen requires a `&mut Context` parameter",
    ))
}

fn is_mut_context_type(ty: &Type) -> bool {
    if let Type::Reference(type_ref) = ty {
        if type_ref.mutability.is_some() {
            if let Type::Path(type_path) = type_ref.elem.as_ref() {
                // Check if the last segment is "Context"
                if let Some(segment) = type_path.path.segments.last() {
                    return segment.ident == "Context";
                }
            }
        }
    }
    false
}
