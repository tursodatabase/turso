use std::{collections::HashSet, ops::Deref};

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
    ItemFn, Meta, Pat, ReturnType, Token, Type,
};

#[derive(Debug, Clone, Copy)]
struct SpannedType<T>(T, Span);

impl<T> SpannedType<T> {
    fn map<U>(self, func: impl FnOnce(T) -> U) -> SpannedType<U> {
        SpannedType(func(self.0), self.1)
    }
}

impl<T> Deref for SpannedType<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ToTokens> ToTokens for SpannedType<T> {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let span = self.1;
        let val = &self.0;
        let out_tokens = quote_spanned! {span=>
            #val
        };
        out_tokens.to_tokens(tokens);
    }
}

#[derive(Debug)]
struct Args {
    path: Option<SpannedType<String>>,
    mvcc: Option<SpannedType<()>>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let args = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut seen_args = HashSet::new();

        let mut path = None;
        let mut mvcc = None;

        let errors = args
            .into_iter()
            .filter_map(|meta| {
                match meta {
                    Meta::NameValue(nv) => {
                        let ident = nv.path.get_ident();
                        if nv.path.is_ident("path") {
                            if let syn::Expr::Lit(syn::ExprLit {
                                lit: syn::Lit::Str(lit_str),
                                ..
                            }) = &nv.value
                            {
                                path = Some(SpannedType(lit_str.value(), nv.value.span()));
                                seen_args.insert(ident.unwrap().clone());
                            } else {
                                return Some(syn::Error::new_spanned(
                                    nv.value,
                                    "argument is not a string literal",
                                ));
                            }
                        } else {
                            return Some(syn::Error::new_spanned(nv.path, "unexpected argument"));
                        }
                    }
                    Meta::Path(p) => {
                        let ident = p.get_ident();
                        if p.is_ident("mvcc") {
                            mvcc = Some(SpannedType((), p.span()));
                            seen_args.insert(ident.unwrap().clone());
                        } else {
                            return Some(syn::Error::new_spanned(p, "unexpected flag"));
                        }
                    }
                    _ => {
                        return Some(syn::Error::new_spanned(meta, "unexpected argument format"));
                    }
                };
                None
            })
            .reduce(|mut accum, err| {
                accum.combine(err);
                accum
            });

        dbg!(&errors);
        if let Some(errors) = errors {
            return Err(errors);
        }

        Ok(Args { path, mvcc })
    }
}

struct DatabaseFunction {
    input: ItemFn,
    tmp_db_fn_arg: Pat,
    args: Args,
}

impl DatabaseFunction {
    fn new(input: ItemFn, tmp_db_fn_arg: Pat, args: Args) -> Self {
        Self {
            input,
            tmp_db_fn_arg,
            args,
        }
    }
}

impl ToTokens for DatabaseFunction {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let ItemFn {
            attrs,
            vis,
            sig,
            block,
        } = &self.input;

        let fn_name = &sig.ident;
        let fn_generics = &sig.generics;

        // Check the return type
        let is_result = is_result(&sig.output);

        let db_path = self.args.path.clone().map_or_else(
            || {
                let name = format!("{fn_name}.db");
                quote! {#name}
            },
            |path| path.to_token_stream(),
        );

        let mut db_opts = quote! {
            turso_core::DatabaseOpts::new()
                .with_indexes(true)
                .with_index_method(true)
                .with_encryption(true)
        };

        if let Some(spanned) = self
            .args
            .mvcc
            .map(|val| val.map(|_| quote! {.with_mvcc(true)}))
        {
            db_opts = quote! {
                #db_opts
                #spanned
            }
        }

        let arg_name = &self.tmp_db_fn_arg;
        let fn_out = &sig.output;

        eprintln!("db_path: {db_path}\ndb_opts: {db_opts}");

        let call_func = if is_result {
            quote! {(|#arg_name| #fn_out #block)(#arg_name).unwrap();}
        } else {
            quote! {(|#arg_name| #block)(#arg_name);}
        };

        let out = quote! {
            #[test]
            #(#attrs)*
            #vis fn #fn_name #fn_generics() {
                let #arg_name = crate::common::TempDatabase::new_with_opts(#db_path, #db_opts);

                #call_func
            }
        };
        out.to_tokens(tokens);
    }
}

pub fn test_macro_attribute(args: TokenStream, input: TokenStream) -> TokenStream {
    eprintln!("args: {args} \ninput : {input}");
    let input = parse_macro_input!(input as ItemFn);

    let args = parse_macro_input!(args as Args);

    dbg!(&args);

    let tmp_db_arg = match check_fn_inputs(&input) {
        Ok(fn_arg) => fn_arg,
        Err(err) => return err.into_compile_error().into(),
    };

    let db_function = DatabaseFunction::new(input, tmp_db_arg, args);

    db_function.to_token_stream().into()
}

fn check_fn_inputs(input: &ItemFn) -> syn::Result<Pat> {
    let msg = "Only 1 function argument can be passed and it must be of type `TempDatabase`";
    let args = &input.sig.inputs;
    if args.len() != 1 {
        return Err(syn::Error::new_spanned(&input.sig, msg));
    }
    let first = args.first().unwrap();
    match first {
        syn::FnArg::Receiver(receiver) => return Err(syn::Error::new_spanned(receiver, msg)),
        syn::FnArg::Typed(pat_type) => {
            if let Type::Path(type_path) = pat_type.ty.as_ref() {
                // Check if qself is None (not a qualified path like <T as Trait>::Type)
                if type_path.qself.is_some() {
                    return Err(syn::Error::new_spanned(type_path, msg));
                }

                // Get the last segment of the path
                // This works for both:
                // - Simple: TempDatabase
                // - Qualified: crate::TempDatabase, my_module::TempDatabase
                if !type_path
                    .path
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "TempDatabase")
                {
                    return Err(syn::Error::new_spanned(type_path, msg));
                }
                Ok(*(pat_type.pat.clone()))
            } else {
                return Err(syn::Error::new_spanned(pat_type, msg));
            }
        }
    }
}

fn is_result(return_type: &ReturnType) -> bool {
    match return_type {
        ReturnType::Default => false, // Returns ()
        ReturnType::Type(_, ty) => {
            // Check if the type path contains "Result"
            if let syn::Type::Path(type_path) = ty.as_ref() {
                type_path
                    .path
                    .segments
                    .last()
                    .map(|seg| seg.ident == "Result")
                    .unwrap_or(false)
            } else {
                false
            }
        }
    }
}
