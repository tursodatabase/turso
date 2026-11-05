use proc_macro::TokenStream;
use quote::quote;
use syn::visit_mut::{self, VisitMut};
use syn::{parse_macro_input, Expr, ExprCall, ExprMethodCall, ItemFn, LitStr};

pub(crate) enum DivanCalls {
    Batched,
    Single,
}

pub(crate) fn divan_bench_attribute(
    attr: TokenStream,
    input: TokenStream,
    calls: DivanCalls,
) -> TokenStream {
    let attr = proc_macro2::TokenStream::from(attr);
    let mut function = parse_macro_input!(input as ItemFn);
    match calls {
        DivanCalls::Batched => {
            if let Err(error) = pass_turso_bencher(&mut function) {
                return error.to_compile_error().into();
            }
        }
        DivanCalls::Single => {}
    }
    let nightly_name = LitStr::new(
        &format!("{} nightly", function.sig.ident),
        function.sig.ident.span(),
    );

    let stable_attr = if attr.is_empty() {
        quote! { #[cfg_attr(not(nightly), divan::bench)] }
    } else {
        quote! { #[cfg_attr(not(nightly), divan::bench(#attr))] }
    };

    let nightly_attr = if attr.is_empty() {
        quote! { #[cfg_attr(nightly, divan::bench(name = #nightly_name))] }
    } else {
        quote! { #[cfg_attr(nightly, divan::bench(name = #nightly_name, #attr))] }
    };

    quote! {
        #nightly_attr
        #stable_attr
        #function
    }
    .into()
}

/// divan calls the function with a `divan::Bencher`. This makes the function take
/// that type and hand its body a `turso_benches::Bencher` instead, which measures
/// many calls at once in CodSpeed builds. Helpers that the body passes the bencher
/// to get the same type, so they can't measure a single call by accident.
fn pass_turso_bencher(function: &mut ItemFn) -> syn::Result<()> {
    let Some(syn::FnArg::Typed(param)) = function.sig.inputs.first_mut() else {
        return Err(syn::Error::new_spanned(
            &function.sig,
            "divan_bench functions take a `turso_benches::Bencher` as their first parameter",
        ));
    };
    let syn::Pat::Ident(name) = param.pat.as_ref() else {
        return Err(syn::Error::new_spanned(
            &param.pat,
            "expected a plain name for the bencher parameter",
        ));
    };
    let name = name.ident.clone();
    let ty = std::mem::replace(
        param.ty.as_mut(),
        syn::parse_quote!(::divan::Bencher<'_, '_>),
    );
    function.block.stmts.insert(
        0,
        syn::parse_quote!(let #name: #ty = ::turso_benches::Bencher::new(#name);),
    );
    Ok(())
}

pub(crate) fn criterion_benchmark_attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "codspeed_criterion_benchmark does not accept arguments",
        )
        .to_compile_error()
        .into();
    }

    let mut function = parse_macro_input!(input as ItemFn);
    CriterionBenchmarkIds.visit_block_mut(&mut function.block);

    quote! { #function }.into()
}

struct CriterionBenchmarkIds;

impl VisitMut for CriterionBenchmarkIds {
    fn visit_expr_method_call_mut(&mut self, expr: &mut ExprMethodCall) {
        visit_mut::visit_expr_method_call_mut(self, expr);

        if expr.method != "bench_function" && expr.method != "bench_with_input" {
            return;
        }

        let Some(first_arg) = expr.args.first_mut() else {
            return;
        };
        let original = first_arg.clone();
        let nightly = nightly_criterion_id(original.clone());
        *first_arg = syn::parse_quote!({
            #[cfg(nightly)]
            {
                #nightly
            }
            #[cfg(not(nightly))]
            {
                #original
            }
        });
    }
}

fn nightly_criterion_id(expr: Expr) -> Expr {
    if let Expr::Call(call) = &expr {
        if let Some(id) = nightly_benchmark_id_call(call) {
            return id;
        }
    }

    syn::parse_quote! {
        format!("{} nightly", #expr)
    }
}

fn nightly_benchmark_id_call(call: &ExprCall) -> Option<Expr> {
    let Expr::Path(func_path) = call.func.as_ref() else {
        return None;
    };

    let mut segments = func_path.path.segments.iter().rev();
    let method = segments.next()?;
    let ty = segments.next()?;
    if ty.ident != "BenchmarkId" {
        return None;
    }

    let func = &call.func;
    match (method.ident.to_string().as_str(), call.args.len()) {
        ("new", 2) => {
            let mut args = call.args.iter();
            let function_name = args.next()?;
            let parameter = args.next()?;
            Some(syn::parse_quote! {
                #func(#function_name, format!("{} nightly", #parameter))
            })
        }
        ("from_parameter", 1) => {
            let parameter = call.args.first()?;
            Some(syn::parse_quote! {
                #func(format!("{} nightly", #parameter))
            })
        }
        _ => None,
    }
}
