use quote::quote;
use quote::ToTokens;
use quote::TokenStreamExt as _;
use rusqlite::types::Value;
use syn::token::Bracket;
use syn::{punctuated::Punctuated, Ident, Lit, Token};

pub fn sqlite_values_impl(input: ValueList2D) -> syn::Result<proc_macro2::TokenStream> {
    Ok(input.into_token_stream())
}

#[derive(Debug)]
struct NewValue(Value);

impl syn::parse::Parse for NewValue {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let value = if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            let name = ident.to_string();
            match name.as_str() {
                null if null.eq_ignore_ascii_case("null") => Value::Null,
                _ => {
                    return Err(syn::Error::new(ident.span(), "expected NULL identifier"));
                }
            }
        } else {
            let curr: Lit = input.parse()?;
            let value = match curr {
                Lit::Bool(bool) => bool.value().into(),
                Lit::Str(s) => s.value().into(),
                Lit::Int(i) => i.base10_parse::<i64>()?.into(),
                Lit::Float(f) => f.base10_parse::<f64>()?.into(),
                Lit::ByteStr(b) => {
                    let hex = hex::decode(b.value())
                        .map_err(|e| syn::Error::new(b.span(), e.to_string()))?;
                    hex.into()
                }
                lit => {
                    return Err(syn::Error::new(lit.span(), "unexpected literal type"));
                }
            };
            value
        };
        Ok(Self(value))
    }
}

#[derive(Debug)]
struct ValueList {
    values: Vec<NewValue>,
}

impl syn::parse::Parse for ValueList {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut values = Vec::new();

        if input.peek(Bracket) {
            let content;
            syn::bracketed!(content in input);
            if content.is_empty() {
                return Ok(Self { values });
            }

            while !content.is_empty() {
                let value: NewValue = content.parse()?;
                values.push(value);
                if content.peek(Token![,]) {
                    content.parse::<Token![,]>()?;
                }
            }
        } else {
            let value: NewValue = input.parse()?;
            values.push(value);
        }

        Ok(Self { values })
    }
}

impl quote::ToTokens for ValueList {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let iter = self.values.iter().map(|v| match &v.0 {
            Value::Null => quote! {
                ::rusqlite::types::Value::Null
            },
            Value::Integer(i) => quote! {
                ::rusqlite::types::Value::Integer(#i)
            },
            Value::Real(f) => quote! {
                ::rusqlite::types::Value::Real(#f)
            },
            Value::Text(t) => quote! {
                ::rusqlite::types::Value::Text(#t)
            },
            Value::Blob(b) => {
                quote! {
                    ::rusqlite::types::Value::Blob(vec![#(#b),*])
                }
            }
        });
        tokens.append_all(quote! { vec![#(#iter),*] });
    }
}

pub(crate) struct ValueList2D {
    inner: Punctuated<ValueList, Token![,]>,
}

impl syn::parse::Parse for ValueList2D {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            if ident.to_string() != "None" {
                return Err(syn::Error::new(ident.span(), "expected None identifier for empty sqlite values"));
            }
            let punctuated = Punctuated::new();
            return Ok(Self { inner: punctuated })
        }

        if input.peek(Bracket) {
            let fork = input.fork();
            let first_content;
            let _first_bracket = syn::bracketed!(first_content in fork);
            if first_content.peek(Bracket) {
                let content;
                syn::bracketed!(content in input);
                let inner = Punctuated::parse_terminated(&content)?;
                return Ok(Self { inner });
            }
        }
        let inner = Punctuated::parse_terminated(input)?;
        Ok(Self { inner })
    }
}

impl quote::ToTokens for ValueList2D {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let iter = self.inner.iter().map(|v| v.into_token_stream());
        tokens.append_all(quote! { vec![#(#iter),*] })
    }
}
