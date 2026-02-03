//! Procedural macros for turso assertions that integrate with Antithesis SDK.
//!
//! These proc macros solve the problem that Antithesis SDK requires actual string literals
//! for messages (`$message:literal`), but `stringify!()` produces macro expansions, not literals.
//! Proc macros can generate actual literal tokens via `quote!`.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::ToTokens;
use syn::{
    braced,
    parse::{Parse, ParseStream},
    Expr, LitStr, Token,
};

/// Input for condition-based assertions: turso_assert!, turso_debug_assert!, etc.
/// Supports:
/// - `(cond)`
/// - `(cond, "msg")`
/// - `(cond, "msg", { ... })` - Antithesis details
/// - `(cond, "msg", fmt_arg1, fmt_arg2, ...)` - format arguments
pub struct ConditionAssertInput {
    pub condition: Expr,
    pub message: Option<LitStr>,
    pub details: Option<TokenStream2>,
    pub format_args: Option<TokenStream2>,
}

impl Parse for ConditionAssertInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let condition: Expr = input.parse()?;

        if !input.peek(Token![,]) {
            return Ok(ConditionAssertInput {
                condition,
                message: None,
                details: None,
                format_args: None,
            });
        }
        input.parse::<Token![,]>()?;

        if !input.peek(LitStr) {
            // No message literal follows the comma - consume the rest as format args
            // (shouldn't normally happen, but handle gracefully)
            let rest: TokenStream2 = input.parse()?;
            return Ok(ConditionAssertInput {
                condition,
                message: None,
                details: None,
                format_args: if rest.is_empty() { None } else { Some(rest) },
            });
        }

        let message: LitStr = input.parse()?;

        if !input.peek(Token![,]) {
            return Ok(ConditionAssertInput {
                condition,
                message: Some(message),
                details: None,
                format_args: None,
            });
        }

        // After message + comma, check if next is { for details
        // We need to lookahead without consuming the comma yet
        let fork = input.fork();
        fork.parse::<Token![,]>()?;

        if fork.peek(syn::token::Brace) {
            // It's a details block: { ... }
            input.parse::<Token![,]>()?;
            let content;
            braced!(content in input);
            let inner: TokenStream2 = content.parse()?;
            return Ok(ConditionAssertInput {
                condition,
                message: Some(message),
                details: Some(inner),
                format_args: None,
            });
        }

        // Otherwise it's format arguments - consume everything remaining
        input.parse::<Token![,]>()?;
        let rest: TokenStream2 = input.parse()?;
        Ok(ConditionAssertInput {
            condition,
            message: Some(message),
            details: None,
            format_args: if rest.is_empty() { None } else { Some(rest) },
        })
    }
}

/// Input for message-only assertions: turso_assert_reachable!, turso_assert_unreachable!, etc.
/// Supports:
/// - `("msg")`
/// - `("msg", { ... })`
pub struct MessageAssertInput {
    pub message: LitStr,
    pub details: Option<TokenStream2>,
}

impl Parse for MessageAssertInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let message: LitStr = input.parse()?;

        let details = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.peek(syn::token::Brace) {
                let content;
                braced!(content in input);
                let inner: TokenStream2 = content.parse()?;
                Some(inner)
            } else {
                None
            }
        } else {
            None
        };

        Ok(MessageAssertInput { message, details })
    }
}

/// Input for comparison assertions: turso_assert_eq!, turso_assert_greater_than!, etc.
/// Supports:
/// - `(left, right, "msg")`
/// - `(left, right, "msg", { ... })`
pub struct ComparisonAssertInput {
    pub left: Expr,
    pub right: Expr,
    pub message: LitStr,
    pub details: Option<TokenStream2>,
}

impl Parse for ComparisonAssertInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let left: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let right: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let message: LitStr = input.parse()?;

        let details = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.peek(syn::token::Brace) {
                let content;
                braced!(content in input);
                let inner: TokenStream2 = content.parse()?;
                Some(inner)
            } else {
                None
            }
        } else {
            None
        };

        Ok(ComparisonAssertInput {
            left,
            right,
            message,
            details,
        })
    }
}

/// Convert an expression to a string literal token
pub fn expr_to_lit_str(expr: &Expr) -> LitStr {
    let expr_str = expr.to_token_stream().to_string();
    LitStr::new(&expr_str, Span::call_site())
}

/// Generate the details JSON expression
pub fn details_json(details: &Option<TokenStream2>) -> TokenStream2 {
    use quote::quote;
    match details {
        Some(inner) => quote! { &serde_json::json!({ #inner }) },
        None => quote! { &serde_json::json!({}) },
    }
}
