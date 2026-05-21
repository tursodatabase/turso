use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemFn};

use super::ScalarInfo;

pub fn scalar(attr: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as ItemFn);
    let fn_name = &ast.sig.ident;
    let args_variable = &ast.sig.inputs.first();
    let mut args_variable_name = None;
    if let Some(syn::FnArg::Typed(syn::PatType { pat, .. })) = args_variable {
        if let syn::Pat::Ident(ident) = &**pat {
            args_variable_name = Some(ident.ident.clone());
        }
    }
    let scalar_info = parse_macro_input!(attr as ScalarInfo);
    let name = &scalar_info.name;
    let register_fn_name = format_ident!("register_{}", fn_name);
    let args_variable_name =
        format_ident!("{}", args_variable_name.unwrap_or(format_ident!("args")));
    let fn_body = &ast.block;
    let alias_check = if let Some(alias) = &scalar_info.alias {
        quote! {
            let Ok(alias_c_name) = ::std::ffi::CString::new(#alias) else {
                return ::turso_ext::ResultCode::Error;
            };
            (api.register_scalar_function)(
                api.ctx,
                alias_c_name.as_ptr(),
                -1,
                false,
                0,
                #fn_name,
                None,
                None,
            );
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(
            api: *const ::turso_ext::ExtensionApi
        ) -> ::turso_ext::ResultCode {
            if api.is_null() {
                return ::turso_ext::ResultCode::Error;
            }
            let api = unsafe { &*api };
            let Ok(c_name) = ::std::ffi::CString::new(#name) else {
                return ::turso_ext::ResultCode::Error;
            };
            (api.register_scalar_function)(
                api.ctx,
                c_name.as_ptr(),
                -1,
                false,
                0,
                #fn_name,
                None,
                None,
            );
            #alias_check
            ::turso_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #fn_name(
            _context: usize,
            argc: i32,
            argv: *const ::turso_ext::Value
        ) -> ::turso_ext::Value {
            let #args_variable_name = if argv.is_null() || argc <= 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(argv, argc as usize) }
            };
            #fn_body
        }
    };

    TokenStream::from(expanded)
}
