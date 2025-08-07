use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

pub fn turso_test_attr(input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);

    let fn_attrs = &input_fn.attrs;
    let fn_vis = &input_fn.vis;
    let fn_sig = &input_fn.sig;
    let fn_block = &input_fn.block;

    let expanded = quote! {
        #[test]
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            #fn_block
        }
    };

    expanded.into()
}
