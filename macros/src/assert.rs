//! Procedural macros for turso assertions that integrate with Antithesis SDK.
//!
//! These proc macros solve the problem that Antithesis SDK requires actual string literals
//! for messages (`$message:literal`), but `stringify!()` produces macro expansions, not literals.
//! Proc macros can generate actual literal tokens via `quote!`.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, ToTokens};
use syn::{
    braced,
    parse::{Parse, ParseStream},
    Expr, LitStr, Token,
};

/// A single key-value pair from a details block: `"key": expr`
pub struct DetailPair {
    pub key: LitStr,
    pub value: Expr,
}

/// Parsed details block: `{ "key1": expr1, "key2": expr2 }`
pub struct DetailsList {
    pub pairs: Vec<DetailPair>,
}

impl DetailsList {
    /// Parse the contents of a braced details block from a ParseStream.
    /// Expects the braces to already be consumed (i.e., receives the inner content).
    pub fn parse_inner(content: ParseStream) -> syn::Result<Self> {
        let mut pairs = Vec::new();
        while !content.is_empty() {
            let key: LitStr = content.parse()?;
            content.parse::<Token![:]>()?;
            let value: Expr = content.parse()?;
            pairs.push(DetailPair { key, value });
            if !content.peek(Token![,]) {
                break;
            }
            content.parse::<Token![,]>()?;
        }
        Ok(DetailsList { pairs })
    }
}

/// Input for condition-based assertions: turso_assert!, turso_debug_assert!, etc.
/// Supports:
/// - `(cond)`
/// - `(cond, "msg")`
/// - `(cond, "msg", { ... })` - Antithesis details
pub struct ConditionAssertInput {
    pub condition: Expr,
    pub message: Option<LitStr>,
    pub details: Option<DetailsList>,
}

impl Parse for ConditionAssertInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let condition: Expr = input.parse()?;

        if !input.peek(Token![,]) {
            return Ok(ConditionAssertInput {
                condition,
                message: None,
                details: None,
            });
        }
        input.parse::<Token![,]>()?;

        if !input.peek(LitStr) {
            return Err(input.error("expected a string literal message after comma"));
        }

        let message: LitStr = input.parse()?;

        if !input.peek(Token![,]) {
            return Ok(ConditionAssertInput {
                condition,
                message: Some(message),
                details: None,
            });
        }

        // After message + comma, check if next is { for details
        // We need to lookahead without consuming the comma yet
        let fork = input.fork();
        fork.parse::<Token![,]>()?;

        if fork.peek(syn::token::Brace) {
            // It's a details block: { "key": value, ... }
            input.parse::<Token![,]>()?;
            let content;
            braced!(content in input);
            let details = DetailsList::parse_inner(&content)?;
            return Ok(ConditionAssertInput {
                condition,
                message: Some(message),
                details: Some(details),
            });
        }

        // Format arguments are not supported — use a details block instead
        Err(input.error(
            "expected details block `{ ... }` or end of macro input; \
             format arguments are not supported — use inline format strings \
             or a details block instead",
        ))
    }
}

/// Input for message-only assertions: turso_assert_reachable!, turso_assert_unreachable!, etc.
/// Supports:
/// - `("msg")`
/// - `("msg", { ... })`
pub struct MessageAssertInput {
    pub message: LitStr,
    pub details: Option<DetailsList>,
}

impl Parse for MessageAssertInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let message: LitStr = input.parse()?;

        let details = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.peek(syn::token::Brace) {
                let content;
                braced!(content in input);
                Some(DetailsList::parse_inner(&content)?)
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
/// - `(left, right)`
/// - `(left, right, "msg")`
/// - `(left, right, "msg", { ... })`
pub struct ComparisonAssertInput {
    pub left: Expr,
    pub right: Expr,
    pub message: Option<LitStr>,
    pub details: Option<DetailsList>,
}

impl Parse for ComparisonAssertInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let left: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let right: Expr = input.parse()?;

        if !input.peek(Token![,]) {
            return Ok(ComparisonAssertInput {
                left,
                right,
                message: None,
                details: None,
            });
        }
        input.parse::<Token![,]>()?;

        if !input.peek(LitStr) {
            return Ok(ComparisonAssertInput {
                left,
                right,
                message: None,
                details: None,
            });
        }

        let message: LitStr = input.parse()?;

        let details = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.peek(syn::token::Brace) {
                let content;
                braced!(content in input);
                Some(DetailsList::parse_inner(&content)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(ComparisonAssertInput {
            left,
            right,
            message: Some(message),
            details,
        })
    }
}

/// Generate an auto-message for comparison assertions from the left/right expressions and operator.
/// Braces are escaped so the message is safe to use as a format string in `assert!`.
pub fn comparison_auto_message(left: &Expr, right: &Expr, op: &str) -> LitStr {
    let left_str = left.to_token_stream().to_string();
    let right_str = right.to_token_stream().to_string();
    let msg = format!("{left_str} {op} {right_str}");
    let msg = msg.replace('{', "{{").replace('}', "}}");
    LitStr::new(&msg, Span::call_site())
}

/// Convert an expression to a string literal token.
/// Braces are escaped so the message is safe to use as a format string in `assert!`.
pub fn expr_to_lit_str(expr: &Expr) -> LitStr {
    let expr_str = expr.to_token_stream().to_string();
    let expr_str = expr_str.replace('{', "{{").replace('}', "}}");
    LitStr::new(&expr_str, Span::call_site())
}

/// Generate the details JSON expression from structured pairs.
pub fn details_json(details: &Option<DetailsList>) -> TokenStream2 {
    match details {
        Some(list) if !list.pairs.is_empty() => {
            let keys: Vec<_> = list.pairs.iter().map(|p| &p.key).collect();
            let vals: Vec<_> = list.pairs.iter().map(|p| &p.value).collect();
            quote! { &serde_json::json!({ #( #keys: format!("{:?}", &#vals) ),* }) }
        }
        _ => quote! { &serde_json::json!({}) },
    }
}

/// Generate a compile-time `Debug` check for detail values.
pub fn details_debug_check(details: &Option<DetailsList>) -> TokenStream2 {
    match details {
        Some(list) if !list.pairs.is_empty() => {
            let vals: Vec<_> = list.pairs.iter().map(|p| &p.value).collect();
            quote! {
                if false {
                    #( let _ = format!("{:?}", &#vals); )*
                }
            }
        }
        _ => quote! {},
    }
}

/// Generate format arguments that include details in the panic message.
///
/// With details:    `"{} | key1={:?}, key2={:?}", msg, val1, val2`
/// Without details: `"{}", msg`
///
/// Uses `{:?}` (Debug) rather than `{}` (Display) because detail values may be
/// types like `&[u8]` that implement Debug but not Display.
pub fn details_format_args(msg: &LitStr, details: &Option<DetailsList>) -> TokenStream2 {
    match details {
        Some(list) if !list.pairs.is_empty() => {
            let fmt_parts: Vec<String> = list
                .pairs
                .iter()
                .map(|p| format!("{}={{:?}}", p.key.value()))
                .collect();
            let fmt = format!("{{}} | {}", fmt_parts.join(", "));
            let fmt_lit = LitStr::new(&fmt, msg.span());
            let vals: Vec<_> = list.pairs.iter().map(|p| &p.value).collect();
            quote! { #fmt_lit, #msg, #(#vals),* }
        }
        _ => quote! { "{}", #msg },
    }
}

/// A named condition: `field_name: condition_expr`
pub struct NamedCondition {
    pub condition: Expr,
}

/// Input for boolean guidance assertions: turso_assert_some!, turso_assert_all!
/// Syntax:
/// - `({field1: cond1, field2: cond2}, "message")`
/// - `({field1: cond1, field2: cond2}, "message", { ... })`
pub struct BooleanGuidanceInput {
    pub conditions: Vec<NamedCondition>,
    pub message: LitStr,
    pub details: Option<DetailsList>,
}

impl Parse for BooleanGuidanceInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        braced!(content in input);

        let mut conditions = Vec::new();
        while !content.is_empty() {
            let _name: syn::Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            let condition: Expr = content.parse()?;
            conditions.push(NamedCondition { condition });
            if !content.peek(Token![,]) {
                break;
            }
            content.parse::<Token![,]>()?;
        }

        input.parse::<Token![,]>()?;
        let message: LitStr = input.parse()?;

        let details = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.peek(syn::token::Brace) {
                let content;
                braced!(content in input);
                Some(DetailsList::parse_inner(&content)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(BooleanGuidanceInput {
            conditions,
            message,
            details,
        })
    }
}
