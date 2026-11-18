use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat, Signature};

pub(crate) fn grow_stack_when_low_attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
    if let Some(argument) = proc_macro2::TokenStream::from(attr).into_iter().next() {
        return syn::Error::new_spanned(argument, "grow_stack_when_low takes no arguments")
            .to_compile_error()
            .into();
    }
    let mut function = parse_macro_input!(input as ItemFn);
    let call_again = match call_with_same_arguments(&function.sig) {
        Ok(call) => call,
        Err(error) => return error.to_compile_error().into(),
    };
    let body = &function.block;
    function.block = syn::parse_quote!({
        {
            const MINIMUM_STACK_SIZE: usize = if cfg!(debug_assertions) {
                1024 * 1024
            } else {
                128 * 1024
            };
            const NEW_STACK_SIZE: usize = if cfg!(debug_assertions) {
                8 * 1024 * 1024
            } else {
                2 * 1024 * 1024
            };
            #[cold]
            #[inline(never)]
            fn call_on_new_stack<R>(call: impl FnOnce() -> R) -> R {
                ::stacker::grow(NEW_STACK_SIZE, call)
            }
            if ::stacker::remaining_stack().is_some_and(|remaining| remaining < MINIMUM_STACK_SIZE)
            {
                return call_on_new_stack(move || #call_again);
            }
        }
        #body
    });
    quote!(#function).into()
}

/// Builds a call of the function with its own arguments, for example
/// `Self::parse_expr(self, precedence)`.
fn call_with_same_arguments(sig: &Signature) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(asyncness) = &sig.asyncness {
        return Err(syn::Error::new_spanned(
            asyncness,
            "grow_stack_when_low does not support async functions",
        ));
    }
    if let Some(constness) = &sig.constness {
        return Err(syn::Error::new_spanned(
            constness,
            "grow_stack_when_low does not support const functions",
        ));
    }
    if sig.ident == "drop" {
        return Err(syn::Error::new_spanned(
            &sig.ident,
            "grow_stack_when_low cannot call Drop::drop again: put it on a function that drop calls",
        ));
    }
    let name = &sig.ident;
    let mut has_receiver = false;
    let mut arguments = Vec::with_capacity(sig.inputs.len());
    for input in &sig.inputs {
        match input {
            FnArg::Receiver(_) => has_receiver = true,
            FnArg::Typed(argument) => match argument.pat.as_ref() {
                Pat::Ident(binding) if binding.by_ref.is_none() && binding.subpat.is_none() => {
                    arguments.push(&binding.ident);
                }
                pattern => {
                    return Err(syn::Error::new_spanned(
                        pattern,
                        "grow_stack_when_low needs a plain name for each argument",
                    ))
                }
            },
        }
    }
    Ok(if has_receiver {
        quote!(Self::#name(self, #(#arguments),*))
    } else {
        quote!(#name(#(#arguments),*))
    })
}
