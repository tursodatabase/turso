use std::collections::HashSet;

use proc_macro::TokenStream;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    ItemFn, Meta, Token,
};

#[derive(Debug)]
struct Args {
    path: Option<String>,
    mvcc: bool,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let args = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut seen_args = HashSet::new();

        let mut path = None;
        let mut mvcc = false;

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
                                path = Some(lit_str.value());
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
                            mvcc = true;
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

pub fn test_macro_attribute(args: TokenStream, input: TokenStream) -> TokenStream {
    eprintln!("args: {args} \ninput : {input}");
    let input = parse_macro_input!(input as ItemFn);

    let mut args = parse_macro_input!(args as Args);

    dbg!(&args);

    quote::quote! {
        #[test]
        #input
    }
    .into()
}
