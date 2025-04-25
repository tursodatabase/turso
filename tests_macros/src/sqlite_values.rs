use proc_macro::TokenStream;
use quote::ToTokens;
use rusqlite::types::Value;
use syn::{parse_macro_input, punctuated::Punctuated, Ident, Lit, LitStr, Token};

pub fn sqlite_values_impl(input: ValueList2D) -> syn::Result<proc_macro2::TokenStream> {
    Ok("".into_token_stream())
}

struct ValueList {
    values: Vec<Value>,
}

impl syn::parse::Parse for ValueList {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut values = Vec::new();

        let content;
        syn::bracketed!(content in input);
        if content.is_empty() {
            return Ok(Self { values });
        }

        while !content.is_empty() {
            let value = if content.peek(Ident) {
                let ident: Ident = content.parse()?;
                if ident.to_string() != "x" {
                    return Err(syn::Error::new(
                        ident.span(),
                        "expected `x` identifier for blobs",
                    ));
                }
                let blob: LitStr = content.parse()?;
                let hex = hex::decode(blob.value().as_bytes())
                    .map_err(|e| syn::Error::new(blob.span(), e.to_string()))?;
                hex.into()
            } else {
                let curr: Lit = content.parse()?;
                let value = match curr {
                    Lit::Bool(bool) => bool.value().into(),
                    Lit::Str(s) => s.value().into(),
                    Lit::Int(i) => i.base10_parse::<i64>()?.into(),
                    Lit::Float(f) => f.base10_parse::<f64>()?.into(),
                    lit => {
                        return Err(syn::Error::new(lit.span(), "unexpected literal"));
                    }
                };
                value
            };
            values.push(value);
            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        Ok(Self { values })
    }
}

pub(crate) struct ValueList2D {
    inner: Punctuated<ValueList, Token![,]>,
}

impl syn::parse::Parse for ValueList2D {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let inner = Punctuated::parse_terminated(input)?;
        Ok(Self { inner })
    }
}
