use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Generate opcode methods to eliminate duplicate code
///
/// Instead of manually writing the same pattern for every opcode, this macro
/// automatically creates the methods we need:
///
/// - `to_explain_tuple()` - formats opcodes for the EXPLAIN command
/// - `get_opcode_descriptions()` - provides help text for the CLI
///
/// Just annotate your enum fields with `#[p1]`, `#[p2]`, `#[p3]` to map them
/// to SQLite's parameter system.
///
/// ```ignore
/// #[derive(OpCode)]
/// pub enum Insn {
///     Add {
///         #[p1] lhs: usize,    // becomes P1 in EXPLAIN output
///         #[p2] rhs: usize,    // becomes P2 in EXPLAIN output
///         #[p3] dest: usize,   // becomes P3 in EXPLAIN output
///     },
/// }
/// ```
pub fn derive_opcode(input: TokenStream) -> TokenStream {
    // Turn the macro input into something we can work with
    let input = parse_macro_input!(input as DeriveInput);

    let enum_name = &input.ident;
    let data = match &input.data {
        Data::Enum(data) => data,
        _ => panic!("OpCode can only be derived for enums"),
    };

    // We'll build up match arms for both methods we're generating
    let mut explain_arms = Vec::new();
    let mut description_entries = Vec::new();

    // Walk through each variant in the enum
    for variant in &data.variants {
        let variant_name = &variant.ident;

        // Use the variant name as the opcode name (e.g., "Add", "Subtract")
        let opcode_name = variant_name.to_string();
        let description = format!("Description for {variant_name}");

        // Start with default values for SQLite parameters
        let mut field_patterns = Vec::new();
        let mut p1 = quote! { 0 };
        let mut p2 = quote! { 0 };
        let mut p3 = quote! { 0 };

        // Look for fields with parameter annotations like #[p1], #[p2], etc.
        if let Fields::Named(fields) = &variant.fields {
            for field in &fields.named {
                let field_name = field.ident.as_ref().unwrap();
                field_patterns.push(field_name);

                // Check if this field has a parameter annotation
                for attr in &field.attrs {
                    if attr.path().is_ident("p1") {
                        p1 = quote! { *#field_name as i32 };
                    } else if attr.path().is_ident("p2") {
                        p2 = quote! { *#field_name as i32 };
                    } else if attr.path().is_ident("p3") {
                        p3 = quote! { *#field_name as i32 };
                    }
                }
            }
        }

        // Build the match arm for EXPLAIN output
        let explain_arm = quote! {
            #enum_name::#variant_name { #(#field_patterns,)* } => {
                (
                    #opcode_name,
                    #p1,
                    #p2,
                    #p3,
                    String::new(),  // P4 - we'll add this later if needed
                    0,              // P5 - same here
                    format!("Generated comment for {opcode_name}"),
                )
            },
        };
        explain_arms.push(explain_arm);

        // Build the entry for CLI help descriptions
        let description_entry = quote! {
            (#opcode_name, #description.to_string())
        };
        description_entries.push(description_entry);
    }

    // Put it all together into the final implementation
    let expanded = quote! {
        impl #enum_name {
            /// Format this opcode for EXPLAIN command output
            pub fn to_explain_tuple(&self) -> (&'static str, i32, i32, i32, String, u16, String) {
                match self {
                    #(#explain_arms)*
                }
            }

            /// Get descriptions for all opcodes (used by CLI help)
            pub fn get_opcode_descriptions() -> Vec<(&'static str, String)> {
                vec![
                    #(#description_entries,)*
                ]
            }
        }
    };

    TokenStream::from(expanded)
}
