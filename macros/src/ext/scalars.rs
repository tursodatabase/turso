use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, Ident, ItemFn};

use super::ScalarInfo;

fn argument_name(ast: &ItemFn, index: usize, fallback: &str) -> Ident {
    ast.sig
        .inputs
        .iter()
        .nth(index)
        .and_then(|arg| {
            let syn::FnArg::Typed(syn::PatType { pat, .. }) = arg else {
                return None;
            };
            let syn::Pat::Ident(ident) = &**pat else {
                return None;
            };
            Some(ident.ident.clone())
        })
        .unwrap_or_else(|| format_ident!("{fallback}"))
}

pub fn scalar(attr: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as ItemFn);
    let fn_name = &ast.sig.ident;
    let scalar_info = parse_macro_input!(attr as ScalarInfo);
    let name = &scalar_info.name;
    let register_fn_name = format_ident!("register_{}", fn_name);
    let args_variable_name = argument_name(&ast, 0, "args");
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
            argv: *const ::turso_ext::Value,
            _context_destructor: Option<::turso_ext::ContextDestructor>,
            _value_destructor: Option<::turso_ext::ValueDestructor>
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

/// Derive registration glue for a stateful scalar function (a struct that
/// implements `turso_ext::ScalarFunc`).
///
/// Generates C-ABI `call`/`drop` shims plus a free `register_<Struct>` entry
/// point so the struct can be listed directly in the `scalars: { .. }` section
/// of `register_extension!`, alongside `#[scalar]` functions.
///
/// The `ScalarFunc::State` is boxed once per registration and handed back to the
/// engine as the opaque `context` pointer; the generated drop shim is registered
/// as the context destructor so the engine frees the state exactly once when the
/// function is unregistered or the owning connection is dropped.
pub fn derive_scalar(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let call_fn_name = format_ident!("{}_call", struct_name);
    let drop_fn_name = format_ident!("{}_drop", struct_name);
    let register_fn_name = format_ident!("register_{}", struct_name);

    let expanded = quote! {
        #[no_mangle]
        pub unsafe extern "C" fn #call_fn_name(
            context: usize,
            argc: i32,
            argv: *const ::turso_ext::Value,
            _context_destructor: Option<::turso_ext::ContextDestructor>,
            _value_destructor: Option<::turso_ext::ValueDestructor>,
        ) -> ::turso_ext::Value {
            let state = unsafe {
                &*(context as *const <#struct_name as ::turso_ext::ScalarFunc>::State)
            };
            let args = if argv.is_null() || argc <= 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(argv, argc as usize) }
            };
            <#struct_name as ::turso_ext::ScalarFunc>::call(state, args)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #drop_fn_name(context: usize) {
            if context != 0 {
                drop(unsafe {
                    ::std::boxed::Box::from_raw(
                        context as *mut <#struct_name as ::turso_ext::ScalarFunc>::State,
                    )
                });
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(
            api: *const ::turso_ext::ExtensionApi
        ) -> ::turso_ext::ResultCode {
            if api.is_null() {
                return ::turso_ext::ResultCode::Error;
            }
            let api = unsafe { &*api };

            let Ok(c_name) = ::std::ffi::CString::new(
                <#struct_name as ::turso_ext::ScalarFunc>::NAME
            ) else {
                return ::turso_ext::ResultCode::Error;
            };
            let context = ::std::boxed::Box::into_raw(::std::boxed::Box::new(
                <#struct_name as ::turso_ext::ScalarFunc>::init(),
            )) as usize;
            let rc = (api.register_scalar_function)(
                api.ctx,
                c_name.as_ptr(),
                -1,
                false,
                context,
                #call_fn_name,
                Some(#drop_fn_name),
                None,
            );
            if !rc.is_ok() {
                unsafe { #drop_fn_name(context) };
                return rc;
            }

            if let Some(alias) = <#struct_name as ::turso_ext::ScalarFunc>::ALIAS {
                let Ok(alias_c_name) = ::std::ffi::CString::new(alias) else {
                    return ::turso_ext::ResultCode::Error;
                };
                // Each registration owns an independent state box so the drop
                // shim frees each exactly once.
                let alias_context = ::std::boxed::Box::into_raw(::std::boxed::Box::new(
                    <#struct_name as ::turso_ext::ScalarFunc>::init(),
                )) as usize;
                let alias_rc = (api.register_scalar_function)(
                    api.ctx,
                    alias_c_name.as_ptr(),
                    -1,
                    false,
                    alias_context,
                    #call_fn_name,
                    Some(#drop_fn_name),
                    None,
                );
                if !alias_rc.is_ok() {
                    unsafe { #drop_fn_name(alias_context) };
                    return alias_rc;
                }
            }

            ::turso_ext::ResultCode::OK
        }
    };

    TokenStream::from(expanded)
}
