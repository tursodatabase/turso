use proc_macro::TokenStream;
use quote::quote;
use std::iter::repeat_n;
use syn::parse_macro_input;

/// macros that checks that bindings signature matches with implementation signature
#[proc_macro_attribute]
pub fn signature(attr: TokenStream, item: TokenStream) -> TokenStream {
    let path = parse_macro_input!(attr as syn::Path);
    let input = parse_macro_input!(item as syn::ItemFn);

    let fn_name = &input.sig.ident;
    let arg_count = input.sig.inputs.len();

    let args = repeat_n(quote!(_), arg_count);

    quote! {
        const _:() = {
            let _: [unsafe extern "C" fn(#(#args),*) -> _; 2] = [
                #path::#fn_name,
                #fn_name,
            ];
        };

        #input
    }
    .into()
}
