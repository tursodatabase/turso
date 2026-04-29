use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    Expr, Ident, ImplItemFn, ItemFn, LitStr, Token,
};

enum TraceStackArgs {
    Inferred,
    InferredWithDetail(Expr),
    Label(LitStr),
    LabelWithDetail(LitStr, Expr),
}

impl Parse for TraceStackArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self::Inferred);
        }

        if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            if ident != "detail" {
                return Err(syn::Error::new_spanned(ident, "expected `detail`"));
            }
            input.parse::<Token![=]>()?;
            let detail = input.parse()?;
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
            if !input.is_empty() {
                return Err(input.error("expected a single detail expression"));
            }
            return Ok(Self::InferredWithDetail(detail));
        }

        let label = input.parse()?;
        if input.is_empty() {
            return Ok(Self::Label(label));
        }

        input.parse::<Token![,]>()?;
        if !input.is_empty() {
            let detail = input.parse()?;
            if input.is_empty() {
                return Ok(Self::LabelWithDetail(label, detail));
            }
            input.parse::<Token![,]>()?;
        }
        if !input.is_empty() {
            return Err(
                input.error("expected a string literal label and optional detail expression")
            );
        }

        Ok(Self::Label(label))
    }
}

pub(crate) fn trace_stack_attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = match syn::parse::<TraceStackArgs>(attr) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };
    let input: proc_macro2::TokenStream = input.into();

    if let Ok(mut function) = syn::parse2::<ItemFn>(input.clone()) {
        let guard = trace_guard(&args, &function.sig.ident);
        let body = &function.block;
        function.block = syn::parse_quote!({
            #guard
            #body
        });
        return quote!(#function).into();
    }

    if let Ok(mut function) = syn::parse2::<ImplItemFn>(input.clone()) {
        let guard = trace_guard(&args, &function.sig.ident);
        let body = &function.block;
        function.block = syn::parse_quote!({
            #guard
            #body
        });
        return quote!(#function).into();
    }

    syn::Error::new_spanned(input, "trace_stack can only be applied to functions")
        .to_compile_error()
        .into()
}

fn trace_guard(args: &TraceStackArgs, function_name: &syn::Ident) -> proc_macro2::TokenStream {
    let inferred_span_name = LitStr::new(&function_name.to_string(), function_name.span());

    let (span_name, guard) = match args {
        TraceStackArgs::Inferred => {
            let guard = quote! {
                let _stack = crate::stack::trace_scope(concat!(module_path!(), "::", stringify!(#function_name)));
            };
            (inferred_span_name, guard)
        }
        TraceStackArgs::InferredWithDetail(detail) => {
            let guard = quote! {
                let _stack = crate::stack::trace_scope_with_detail(concat!(module_path!(), "::", stringify!(#function_name)), #detail);
            };
            (inferred_span_name, guard)
        }
        TraceStackArgs::Label(label) => {
            let guard = quote! {
                let _stack = crate::stack::trace_scope(#label);
            };
            (label.clone(), guard)
        }
        TraceStackArgs::LabelWithDetail(label, detail) => {
            let guard = quote! {
                let _stack = crate::stack::trace_scope_with_detail(#label, #detail);
            };
            (label.clone(), guard)
        }
    };

    quote! {
        #[cfg(feature = "stacker")]
        let _stack_span = tracing::debug_span!(target: "turso_stack", #span_name).entered();
        #guard
    }
}
