use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};

pub fn derive_vfs_module(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    let struct_name = &derive_input.ident;
    let register_fn_name = format_ident!("register_{}", struct_name);
    let register_static = format_ident!("register_static_{}", struct_name);
    let open_fn_name = format_ident!("{}_open", struct_name);
    let close_fn_name = format_ident!("{}_close", struct_name);
    let read_fn_name = format_ident!("{}_read", struct_name);
    let write_fn_name = format_ident!("{}_write", struct_name);
    let trunc_fn_name = format_ident!("{}_truncate", struct_name);
    let lock_fn_name = format_ident!("{}_lock", struct_name);
    let unlock_fn_name = format_ident!("{}_unlock", struct_name);
    let sync_fn_name = format_ident!("{}_sync", struct_name);
    let size_fn_name = format_ident!("{}_size", struct_name);
    let run_once_fn_name = format_ident!("{}_run_once", struct_name);
    let generate_random_number_fn_name = format_ident!("{}_generate_random_number", struct_name);
    let get_current_time_fn_name = format_ident!("{}_get_current_time", struct_name);

    let expanded = quote! {
        #[allow(non_snake_case)]
        pub unsafe extern "C" fn #register_static() -> *const ::turso_ext::VfsImpl {
            let ctx = #struct_name::default();
            let ctx = ::std::boxed::Box::into_raw(::std::boxed::Box::new(ctx)) as *const ::std::ffi::c_void;
            let name = ::std::ffi::CString::new(<#struct_name as ::turso_ext::VfsExtension>::NAME).unwrap().into_raw();
            let vfs_mod = ::turso_ext::VfsImpl {
                vfs: ctx,
                name,
                open: #open_fn_name,
                close: #close_fn_name,
                read: #read_fn_name,
                write: #write_fn_name,
                lock: #lock_fn_name,
                unlock: #unlock_fn_name,
                sync: #sync_fn_name,
                size: #size_fn_name,
                truncate: #trunc_fn_name,
                run_once: #run_once_fn_name,
                gen_random_number: #generate_random_number_fn_name,
                current_time: #get_current_time_fn_name,
            };
            ::std::boxed::Box::into_raw(::std::boxed::Box::new(vfs_mod)) as *const ::turso_ext::VfsImpl
        }

        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(api: &::turso_ext::ExtensionApi) -> ::turso_ext::ResultCode {
            let ctx = #struct_name::default();
            let ctx = ::std::boxed::Box::into_raw(::std::boxed::Box::new(ctx)) as *const ::std::ffi::c_void;
            let name = ::std::ffi::CString::new(<#struct_name as ::turso_ext::VfsExtension>::NAME).unwrap().into_raw();
            let vfs_mod = ::turso_ext::VfsImpl {
                vfs: ctx,
                name,
                open: #open_fn_name,
                close: #close_fn_name,
                read: #read_fn_name,
                write: #write_fn_name,
                lock: #lock_fn_name,
                unlock: #unlock_fn_name,
                sync: #sync_fn_name,
                size: #size_fn_name,
                truncate: #trunc_fn_name,
                run_once: #run_once_fn_name,
                gen_random_number: #generate_random_number_fn_name,
                current_time: #get_current_time_fn_name,
            };
            let vfsimpl = ::std::boxed::Box::into_raw(::std::boxed::Box::new(vfs_mod)) as *const ::turso_ext::VfsImpl;
            (api.vfs_interface.register_vfs)(name, vfsimpl)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #open_fn_name(
            ctx: *const ::std::ffi::c_void,
            path: *const ::std::ffi::c_char,
            flags: i32,
            direct: bool,
        ) -> *const ::std::ffi::c_void {
            let ctx = &*(ctx as *const ::turso_ext::VfsImpl);
            let Ok(path_str) = ::std::ffi::CStr::from_ptr(path).to_str() else {
                  return ::std::ptr::null_mut();
            };
            let vfs = &*(ctx.vfs as *const #struct_name);
            let Ok(file_handle) = <#struct_name as ::turso_ext::VfsExtension>::open_file(vfs, path_str, flags, direct) else {
                return ::std::ptr::null();
            };
            let boxed = ::std::boxed::Box::into_raw(::std::boxed::Box::new(file_handle)) as *const ::std::ffi::c_void;
            let Ok(vfs_file) = ::turso_ext::VfsFileImpl::new(boxed, ctx) else {
                return ::std::ptr::null();
            };
            ::std::boxed::Box::into_raw(::std::boxed::Box::new(vfs_file)) as *const ::std::ffi::c_void
        }

        #[no_mangle]
        pub unsafe extern "C" fn #close_fn_name(file_ptr: *const ::std::ffi::c_void) -> ::turso_ext::ResultCode {
            if file_ptr.is_null() {
                return ::turso_ext::ResultCode::Error;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let vfs_instance = &*(vfs_file.vfs as *const #struct_name);

            // this time we need to own it so we can drop it
            let file: ::std::boxed::Box<<#struct_name as ::turso_ext::VfsExtension>::File> =
             ::std::boxed::Box::from_raw(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            if let Err(e) = <#struct_name as ::turso_ext::VfsExtension>::close(vfs_instance, *file) {
                return e;
            }
            ::turso_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #read_fn_name(file_ptr: *const ::std::ffi::c_void, buf: *mut u8, count: usize, offset: i64) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            match <#struct_name as ::turso_ext::VfsExtension>::File::read(file, ::std::slice::from_raw_parts_mut(buf, count), count, offset) {
                Ok(n) => n,
                Err(_) => -1,
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #run_once_fn_name(ctx: *const ::std::ffi::c_void) -> ::turso_ext::ResultCode {
            if ctx.is_null() {
                return ::turso_ext::ResultCode::Error;
            }
            let ctx = &mut *(ctx as *mut #struct_name);
            if let Err(e) = <#struct_name as ::turso_ext::VfsExtension>::run_once(ctx) {
                return e;
            }
            ::turso_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #write_fn_name(file_ptr: *const ::std::ffi::c_void, buf: *const u8, count: usize, offset: i64) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            match <#struct_name as ::turso_ext::VfsExtension>::File::write(file, ::std::slice::from_raw_parts(buf, count), count, offset) {
                Ok(n) => n,
                Err(_) => -1,
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #lock_fn_name(file_ptr: *const ::std::ffi::c_void, exclusive: bool) -> ::turso_ext::ResultCode {
            if file_ptr.is_null() {
                return ::turso_ext::ResultCode::Error;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            if let Err(e) = <#struct_name as ::turso_ext::VfsExtension>::File::lock(file, exclusive) {
                return e;
            }
            ::turso_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #unlock_fn_name(file_ptr: *const ::std::ffi::c_void) -> ::turso_ext::ResultCode {
            if file_ptr.is_null() {
                return ::turso_ext::ResultCode::Error;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            if let Err(e) = <#struct_name as ::turso_ext::VfsExtension>::File::unlock(file) {
                return e;
            }
            ::turso_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #sync_fn_name(file_ptr: *const ::std::ffi::c_void) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            if <#struct_name as ::turso_ext::VfsExtension>::File::sync(file).is_err() {
                return -1;
            }
            0
        }

        #[no_mangle]
        pub unsafe extern "C" fn #trunc_fn_name(file_ptr: *const ::std::ffi::c_void, len: i64) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            if <#struct_name as ::turso_ext::VfsExtension>::File::truncate(file, len).is_err() {
                return -1;
            }
            0
        }

        #[no_mangle]
        pub unsafe extern "C" fn #size_fn_name(file_ptr: *const ::std::ffi::c_void) -> i64 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::turso_ext::VfsFileImpl = &mut *(file_ptr as *mut ::turso_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::turso_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::turso_ext::VfsExtension>::File);
            <#struct_name as ::turso_ext::VfsExtension>::File::size(file)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #generate_random_number_fn_name() -> i64 {
            let obj = #struct_name::default();
            <#struct_name as ::turso_ext::VfsExtension>::generate_random_number(&obj)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #get_current_time_fn_name() -> *const ::std::ffi::c_char {
            let obj = #struct_name::default();
            let time = <#struct_name as ::turso_ext::VfsExtension>::get_current_time(&obj);
            // release ownership of the string to core
            ::std::ffi::CString::new(time).unwrap().into_raw() as *const ::std::ffi::c_char
        }
    };

    TokenStream::from(expanded)
}
