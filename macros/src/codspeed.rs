use proc_macro::TokenStream;
use quote::quote;
use syn::visit_mut::{self, VisitMut};
use syn::{parse_macro_input, Expr, ExprCall, ExprMethodCall, ItemFn, LitStr};

pub(crate) fn divan_bench_attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attr = proc_macro2::TokenStream::from(attr);
    let mut function = parse_macro_input!(input as ItemFn);
    let mut batch = DivanBatch { error: None };
    batch.visit_block_mut(&mut function.block);
    if let Some(error) = batch.error {
        return error.to_compile_error().into();
    }
    function.block.stmts.splice(0..0, divan_batch_helpers());
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

/// Number of calls in one CodSpeed measurement of a divan benchmark.
const DIVAN_CODSPEED_BATCH: usize = 100;

/// codspeed-divan-compat measures a single call. For a function of a few hundred
/// instructions, one cache miss more or less (for example because unrelated code
/// moved in the binary) changes the result by 30%. In CodSpeed builds this rewrites
/// `bencher.bench_local(f)` and `bencher.with_inputs(g).bench_local_values(f)` to
/// measure DIVAN_CODSPEED_BATCH calls at once.
struct DivanBatch {
    error: Option<syn::Error>,
}

impl VisitMut for DivanBatch {
    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        visit_mut::visit_expr_mut(self, expr);

        let Expr::MethodCall(call) = &*expr else {
            return;
        };
        let method = call.method.to_string();
        match method.as_str() {
            "bench_local" | "bench_local_values" => {}
            "bench" | "bench_values" | "bench_refs" | "bench_local_refs" => {
                self.error = Some(syn::Error::new_spanned(
                    &call.method,
                    format!("divan_bench does not support `{method}`; use `bench_local` or `bench_local_values`"),
                ));
                return;
            }
            _ => return,
        }
        let Some(benched) = call.args.first() else {
            self.error = Some(syn::Error::new_spanned(
                &call.method,
                "expected a closure argument",
            ));
            return;
        };
        let mut original = call.clone();
        // Attributes such as `#[expect(..)]` on the statement belong to the whole
        // rewritten expression, not only to the copy that non-CodSpeed builds use.
        let attrs = std::mem::take(&mut original.attrs);
        let batched: Expr = if call.method == "bench_local" {
            let receiver = &call.receiver;
            syn::parse_quote!(#receiver.bench_local(__codspeed_batch(#benched)))
        } else {
            let inputs = match call.receiver.as_ref() {
                Expr::MethodCall(inputs)
                    if inputs.method == "with_inputs" && inputs.args.len() == 1 =>
                {
                    inputs
                }
                other => {
                    self.error = Some(syn::Error::new_spanned(
                        other,
                        "divan_bench expects `.with_inputs(..)` directly before `bench_local_values`",
                    ));
                    return;
                }
            };
            let bencher = &inputs.receiver;
            let gen_input = &inputs.args[0];
            syn::parse_quote!({
                let (__gen_inputs, __benched) = __codspeed_batch_values(#gen_input, #benched);
                #bencher.with_inputs(__gen_inputs).bench_local_values(__benched)
            })
        };
        let mut rewritten: syn::ExprBlock = syn::parse_quote!({
            #[cfg(feature = "codspeed")]
            {
                #batched
            }
            #[cfg(not(feature = "codspeed"))]
            {
                #original
            }
        });
        rewritten.attrs = attrs;
        *expr = Expr::Block(rewritten);
    }
}

/// Functions called by the code that `DivanBatch` generates. The closures are passed
/// straight to them so the closures still get their parameter types from a trait bound.
fn divan_batch_helpers() -> Vec<syn::Stmt> {
    let block: syn::Block = syn::parse_quote!({
        #[cfg(feature = "codspeed")]
        #[allow(dead_code)]
        fn __codspeed_batch<O, B: FnMut() -> O>(mut benched: B) -> impl FnMut() {
            move || {
                for _ in 0..#DIVAN_CODSPEED_BATCH {
                    ::std::hint::black_box(benched());
                }
            }
        }

        #[cfg(feature = "codspeed")]
        #[allow(dead_code)]
        fn __codspeed_batch_values<I, O, G: FnMut() -> I, B: FnMut(I) -> O>(
            mut gen_input: G,
            mut benched: B,
        ) -> (impl FnMut() -> Vec<I>, impl FnMut(Vec<I>)) {
            (
                move || (0..#DIVAN_CODSPEED_BATCH).map(|_| gen_input()).collect(),
                move |inputs: Vec<I>| {
                    for input in inputs {
                        ::std::hint::black_box(benched(input));
                    }
                },
            )
        }
    });
    block.stmts
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
