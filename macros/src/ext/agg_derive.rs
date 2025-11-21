use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};

pub fn derive_agg_func(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let step_fn_name = format_ident!("{}_step", struct_name);
    let finalize_fn_name = format_ident!("{}_finalize", struct_name);
    let init_fn_name = format_ident!("{}_init", struct_name);
    let register_fn_name = format_ident!("register_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            pub extern "C" fn #init_fn_name() -> *mut ::turso_ext::AggCtx {
                let state = ::std::boxed::Box::new(<#struct_name as ::turso_ext::AggFunc>::State::default());
                let ctx = ::std::boxed::Box::new(::turso_ext::AggCtx {
                    state: ::std::boxed::Box::into_raw(state) as *mut ::std::os::raw::c_void,
                });
                ::std::boxed::Box::into_raw(ctx)
            }

            #[no_mangle]
            pub extern "C" fn #step_fn_name(
                ctx: *mut ::turso_ext::AggCtx,
                argc: i32,
                argv: *const ::turso_ext::Value,
            ) {
                unsafe {
                    let ctx = &mut *ctx;
                    let state = &mut *(ctx.state as *mut <#struct_name as ::turso_ext::AggFunc>::State);
                    let args = ::std::slice::from_raw_parts(argv, argc as usize);
                    <#struct_name as ::turso_ext::AggFunc>::step(state, args);
                }
            }

            #[no_mangle]
            pub extern "C" fn #finalize_fn_name(
                ctx: *mut ::turso_ext::AggCtx
            ) -> ::turso_ext::Value {
                unsafe {
                    let ctx = &mut *ctx;
                    let state = ::std::boxed::Box::from_raw(ctx.state as *mut <#struct_name as ::turso_ext::AggFunc>::State);
                    match <#struct_name as ::turso_ext::AggFunc>::finalize(*state) {
                        Ok(val) => val,
                        Err(e) => {
                            ::turso_ext::Value::error_with_message(e.to_string())
                        }
                    }
                }
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::turso_ext::ExtensionApi
            ) -> ::turso_ext::ResultCode {
                if api.is_null() {
                    return ::turso_ext::ResultCode::Error;
                }

                let api = &*api;
                let name_str = #struct_name::NAME;
                let c_name = match ::std::ffi::CString::new(name_str) {
                    Ok(cname) => cname,
                    Err(_) => return ::turso_ext::ResultCode::Error,
                };

                (api.register_aggregate_function)(
                    api.ctx,
                    c_name.as_ptr(),
                    #struct_name::ARGS,
                    #struct_name::#init_fn_name
                        as ::turso_ext::InitAggFunction,
                    #struct_name::#step_fn_name
                        as ::turso_ext::StepFunction,
                    #struct_name::#finalize_fn_name
                        as ::turso_ext::FinalizeFunction,
                )
            }
        }
    };

    TokenStream::from(expanded)
}
