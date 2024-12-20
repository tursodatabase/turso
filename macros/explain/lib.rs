extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Expr, Fields, Ident, Lit, Meta, Variant};

#[proc_macro_derive(Explain, attributes(desc, p1, p2, p3))]
pub fn explain_derive(code: TokenStream) -> TokenStream {
    let input = parse_macro_input!(code as DeriveInput);
    let enum_name = &input.ident;

    let struct_name: Ident = Ident::new(
        format!("{}Description", enum_name).as_str(),
        Span::call_site(),
    );
    let const_name: Ident = Ident::new(
        format!(
            "{}_DESCRIPTIONS",
            enum_name.to_string().to_ascii_uppercase()
        )
        .as_str(),
        Span::call_site(),
    );

    let Data::Enum(data_enum) = &input.data else {
        panic!("Explain is meant for an enum");
    };

    let mut name_arms = Vec::new();
    let mut desc_arms = Vec::new();
    let mut desc_entries = Vec::new();
    let mut p1_arms = Vec::new();
    let mut p2_arms = Vec::new();
    let mut p3_arms = Vec::new();

    for variant in &data_enum.variants {
        let variant_name = &variant.ident;
        let variant_str = variant_name.to_string();
        let desc_str = gen_description(variant);
        name_arms.push(quote! { Self::#variant_name{..} => #variant_str });
        desc_arms.push(quote! { Self::#variant_name{..} => #desc_str });
        desc_entries.push(gen_desc_item(
            &struct_name,
            variant_name.to_string(),
            desc_str,
        ));
        p1_arms.push(gen_opcode("p1", variant));
        p2_arms.push(gen_opcode("p2", variant));
        p3_arms.push(gen_opcode("p3", variant));
    }

    let count = name_arms.len();

    assert_eq!(desc_entries.len(), count);
    assert_eq!(name_arms.len(), count);
    assert_eq!(desc_arms.len(), count);
    assert_eq!(p1_arms.len(), count);
    assert_eq!(p2_arms.len(), count);
    assert_eq!(p3_arms.len(), count);

    eprintln!("{}", struct_name);

    let output = quote! {
        pub struct #struct_name {
            pub name: &'static str,
            pub description: &'static str,
        }

        impl std::fmt::Display for #struct_name  {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}\n-------\n{}", self.name, self.description)
            }
        }

        pub const #const_name: [#struct_name; #count] = [
            #(#desc_entries,)*
        ];

        impl #enum_name {
            fn explain_name(&self) -> &str {
                match self {
                    #(#name_arms,)*
                }
            }

            fn explain_desc(&self) -> &str {
                match self {
                    #(#desc_arms,)*
                }
            }

            fn explain_p1(&self) -> i32 {
                match self {
                    #(#p1_arms,)*
                }
            }

            fn explain_p2(&self) -> i32 {
                match self {
                    #(#p2_arms,)*
                }
            }

            fn explain_p3(&self) -> i32 {
                match self {
                    #(#p3_arms,)*
                }
            }
        }
    };

    TokenStream::from(output)
}

fn gen_description(variant: &Variant) -> String {
    for attr in &variant.attrs {
        if attr.path().is_ident("desc") {
            if let Meta::NameValue(name) = &attr.meta {
                if let Expr::Lit(expr) = &name.value {
                    if let Lit::Str(str) = &expr.lit {
                        return str.value();
                    }
                }
            }
            break;
        }
    }
    "Missing desc attribute for variant".to_string()
}

fn gen_desc_item(struct_name: &Ident, name: String, desc: String) -> proc_macro2::TokenStream {
    quote! {
        #struct_name { name: #name, description: #desc }
    }
}

fn gen_opcode(attr_name: &str, variant: &Variant) -> proc_macro2::TokenStream {
    let variant_name = &variant.ident;
    let mut found_field = None;

    if let Fields::Named(fields) = &variant.fields {
        for field in &fields.named {
            if field
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident(&attr_name))
            {
                found_field = Some(field.ident.clone().unwrap());
                break;
            }
        }
    };

    match found_field {
        Some(field_name) => {
            quote! { Self::#variant_name { #field_name, .. } => *#field_name as i32}
        }
        None => quote! { Self::#variant_name { .. } => 0 },
    }
}
