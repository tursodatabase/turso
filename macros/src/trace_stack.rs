use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, ItemFn, LitStr, Token,
};

enum TraceStackArgs {
    Inferred,
    Label(LitStr),
}

impl Parse for TraceStackArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self::Inferred);
        }

        let label = input.parse()?;
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
        if !input.is_empty() {
            return Err(input.error("expected at most one string literal label"));
        }

        Ok(Self::Label(label))
    }
}

pub(crate) fn trace_stack_attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as TraceStackArgs);
    let mut function = parse_macro_input!(input as ItemFn);
    let function_name = &function.sig.ident;
    let inferred_span_name = LitStr::new(&function_name.to_string(), function_name.span());

    let (span_name, guard) = match args {
        TraceStackArgs::Inferred => {
            let guard = quote! {
                let _stack = crate::stack::trace_scope(concat!(module_path!(), "::", stringify!(#function_name)));
            };
            (inferred_span_name, guard)
        }
        TraceStackArgs::Label(label) => {
            let guard = quote! {
                let _stack = crate::stack::trace_scope(#label);
            };
            (label, guard)
        }
    };

    let body = &function.block;
    function.block = syn::parse_quote!({
        #[cfg(feature = "stacker")]
        let _stack_span = tracing::debug_span!(target: "turso_stack", #span_name).entered();
        #guard
        #body
    });

    quote!(#function).into()
}
