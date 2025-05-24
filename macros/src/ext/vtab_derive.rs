use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};

pub fn derive_vtab_module(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let register_fn_name = format_ident!("register_{}", struct_name);
    let create_fn_name = format_ident!("create_{}", struct_name);
    let open_fn_name = format_ident!("open_{}", struct_name);
    let close_fn_name = format_ident!("close_{}", struct_name);
    let filter_fn_name = format_ident!("filter_{}", struct_name);
    let column_fn_name = format_ident!("column_{}", struct_name);
    let next_fn_name = format_ident!("next_{}", struct_name);
    let eof_fn_name = format_ident!("eof_{}", struct_name);
    let update_fn_name = format_ident!("update_{}", struct_name);
    let rowid_fn_name = format_ident!("rowid_{}", struct_name);
    let destroy_fn_name = format_ident!("destroy_{}", struct_name);
    let best_idx_fn_name = format_ident!("best_idx_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            unsafe extern "C" fn #create_fn_name(
                argv: *const ::limbo_ext::Value, argc: i32
            ) -> ::limbo_ext::VTabCreateResult {
                let args = if argv.is_null() {
                    &Vec::new()
                } else {
                    ::std::slice::from_raw_parts(argv, argc as usize)
                };
                match <#struct_name as ::limbo_ext::VTabModule>::create(&args) {
                    Ok((schema, table)) => {
                        ::limbo_ext::VTabCreateResult {
                            code: ::limbo_ext::ResultCode::OK,
                            schema: ::std::ffi::CString::new(schema).unwrap().into_raw(),
                            table: ::std::boxed::Box::into_raw(::std::boxed::Box::new(table)) as *const ::std::ffi::c_void,
                        }
                    },
                    Err(e) => {
                        ::limbo_ext::VTabCreateResult {
                            code: e,
                            schema: ::std::ptr::null(),
                            table: ::std::ptr::null(),
                        }
                    }
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #open_fn_name(table: *const ::std::ffi::c_void) -> *const ::std::ffi::c_void {
                if table.is_null() {
                    return ::std::ptr::null();
                }
                let table = table as *const <#struct_name as ::limbo_ext::VTabModule>::Table;
                let table: &<#struct_name as ::limbo_ext::VTabModule>::Table = &*table;
                if let Ok(cursor) = <#struct_name as ::limbo_ext::VTabModule>::Table::open(table) {
                    return ::std::boxed::Box::into_raw(::std::boxed::Box::new(cursor)) as *const ::std::ffi::c_void;
                } else {
                    return ::std::ptr::null();
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #close_fn_name(
                cursor: *const ::std::ffi::c_void
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = cursor as *mut <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor;
                let cursor = ::std::boxed::Box::from_raw(cursor);
                <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor::close(&*cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #filter_fn_name(
                cursor: *const ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
                idx_str: *const ::std::ffi::c_char,
                idx_num: i32,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = &mut *(cursor as *mut <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor);
                let args = ::std::slice::from_raw_parts(argv, argc as usize);
                let idx_str = if idx_str.is_null() {
                    None
                } else {
                    Some((unsafe { ::std::ffi::CStr::from_ptr(idx_str).to_str().unwrap() }, idx_num))
                };
                <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor::filter(cursor, args, idx_str)
            }

            #[no_mangle]
            unsafe extern "C" fn #column_fn_name(
                cursor: *const ::std::ffi::c_void,
                idx: u32,
            ) -> ::limbo_ext::Value {
                if cursor.is_null() {
                    return ::limbo_ext::Value::error(::limbo_ext::ResultCode::Error);
                }
                let cursor = &*(cursor as *const <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor);
                match <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor::column(cursor, idx) {
                    Ok(val) => val,
                    Err(e) => ::limbo_ext::Value::error_with_message(e.to_string())
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #next_fn_name(
                cursor: *const ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = &mut *(cursor as *mut <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor);
                <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor::next(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #eof_fn_name(
                cursor: *const ::std::ffi::c_void,
            ) -> bool {
                if cursor.is_null() {
                    return true;
                }
                let cursor = &*(cursor as *const <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor);
                <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor::eof(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #update_fn_name(
                table: *const ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
                p_out_rowid: *mut i64,
            ) -> ::limbo_ext::ResultCode {
                if table.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let table = &mut *(table as *mut <#struct_name as ::limbo_ext::VTabModule>::Table);
                let args = ::std::slice::from_raw_parts(argv, argc as usize);

                let old_rowid = match args.get(0).map(|v| v.value_type()) {
                    Some(::limbo_ext::ValueType::Integer) => args.get(0).unwrap().to_integer(),
                    _ => None,
                };
                let new_rowid = match args.get(1).map(|v| v.value_type()) {
                    Some(::limbo_ext::ValueType::Integer) => args.get(1).unwrap().to_integer(),
                    _ => None,
                };
                let columns = &args[2..];
                match (old_rowid, new_rowid) {
                    // DELETE: old_rowid provided, no new_rowid
                    (Some(old), None) => {
                     if <#struct_name as VTabModule>::Table::delete(table, old).is_err() {
                            return ::limbo_ext::ResultCode::Error;
                      }
                            return ::limbo_ext::ResultCode::OK;
                    }
                    // UPDATE: old_rowid provided and new_rowid may exist
                    (Some(old), Some(new)) => {
                        if <#struct_name as VTabModule>::Table::update(table, old, &columns).is_err() {
                            return ::limbo_ext::ResultCode::Error;
                        }
                        return ::limbo_ext::ResultCode::OK;
                    }
                    // INSERT: no old_rowid (old_rowid = None)
                    (None, _) => {
                        if let Ok(rowid) = <#struct_name as VTabModule>::Table::insert(table, &columns) {
                            if !p_out_rowid.is_null() {
                                *p_out_rowid = rowid;
                                 return ::limbo_ext::ResultCode::RowID;
                            }
                            return ::limbo_ext::ResultCode::OK;
                        }
                    }
                }
                return ::limbo_ext::ResultCode::Error;
            }

            #[no_mangle]
            pub unsafe extern "C" fn #rowid_fn_name(cursor: *const ::std::ffi::c_void) -> i64 {
                if cursor.is_null() {
                    return -1;
                }
                let cursor = &*(cursor as *const <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor);
                <<#struct_name as ::limbo_ext::VTabModule>::Table as ::limbo_ext::VTable>::Cursor::rowid(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #destroy_fn_name(
                table: *const ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if table.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                // Take ownership of the table so it can be properly dropped.
                let mut table: ::std::boxed::Box<<#struct_name as ::limbo_ext::VTabModule>::Table> =
                ::std::boxed::Box::from_raw(table as *mut <#struct_name as ::limbo_ext::VTabModule>::Table);
                if <#struct_name as VTabModule>::Table::destroy(&mut *table).is_err() {
                    return ::limbo_ext::ResultCode::Error;
                }

                return ::limbo_ext::ResultCode::OK;
            }

            #[no_mangle]
            pub unsafe extern "C" fn #best_idx_fn_name(
                constraints: *const ::limbo_ext::ConstraintInfo,
                n_constraints: i32,
                order_by: *const ::limbo_ext::OrderByInfo,
                n_order_by: i32,
            ) -> ::limbo_ext::ExtIndexInfo {
                let constraints = if n_constraints > 0 { std::slice::from_raw_parts(constraints, n_constraints as usize) } else { &[] };
                let order_by = if n_order_by > 0 { std::slice::from_raw_parts(order_by, n_order_by as usize) } else { &[] };
                <#struct_name as ::limbo_ext::VTabModule>::Table::best_index(constraints, order_by).to_ffi()
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let api = &*api;
                let name = <#struct_name as ::limbo_ext::VTabModule>::NAME;
                let name_c = ::std::ffi::CString::new(name).unwrap().into_raw() as *const ::std::ffi::c_char;
                let module = ::limbo_ext::VTabModuleImpl {
                    name: name_c,
                    create: Self::#create_fn_name,
                    open: Self::#open_fn_name,
                    close: Self::#close_fn_name,
                    filter: Self::#filter_fn_name,
                    column: Self::#column_fn_name,
                    next: Self::#next_fn_name,
                    eof: Self::#eof_fn_name,
                    update: Self::#update_fn_name,
                    rowid: Self::#rowid_fn_name,
                    destroy: Self::#destroy_fn_name,
                    best_idx: Self::#best_idx_fn_name,
                };
                (api.register_vtab_module)(api.ctx, name_c, module, <#struct_name as ::limbo_ext::VTabModule>::VTAB_KIND)
            }
        }
    };

    TokenStream::from(expanded)
}
