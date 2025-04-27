mod sqlite_values;

use proc_macro::TokenStream;
use proc_macro_error::proc_macro_error;
use sqlite_values::ValueList2D;
use syn::parse_macro_input;

#[proc_macro_error]
#[proc_macro]
pub fn sqlite_values(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ValueList2D);

    sqlite_values::sqlite_values_impl(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
