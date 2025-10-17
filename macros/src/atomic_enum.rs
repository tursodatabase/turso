use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

pub(crate) fn derive_atomic_enum_inner(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let atomic_name = syn::Ident::new(&format!("Atomic{name}"), name.span());

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return syn::Error::new_spanned(input, "AtomicEnum can only be derived for enums")
                .to_compile_error()
                .into();
        }
    };

    // get info about variants to determine how we have to encode them
    let mut has_bool_field = false;
    let mut has_u8_field = false;
    let mut max_discriminant = 0u8;

    for (idx, variant) in variants.iter().enumerate() {
        max_discriminant = idx as u8;
        match &variant.fields {
            Fields::Unit => {}
            Fields::Named(fields) if fields.named.len() == 1 => {
                let field = &fields.named[0];
                if is_bool_type(&field.ty) {
                    has_bool_field = true;
                } else if is_u8_or_i8_type(&field.ty) {
                    has_u8_field = true;
                } else {
                    return syn::Error::new_spanned(
                        field,
                        "AtomicEnum only supports bool, u8, or i8 fields",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field = &fields.unnamed[0];
                if is_bool_type(&field.ty) {
                    has_bool_field = true;
                } else if is_u8_or_i8_type(&field.ty) {
                    has_u8_field = true;
                } else {
                    return syn::Error::new_spanned(
                        field,
                        "AtomicEnum only supports bool, u8, or i8 fields",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            _ => {
                return syn::Error::new_spanned(
                    variant,
                    "AtomicEnum only supports unit variants or variants with a single field",
                )
                .to_compile_error()
                .into();
            }
        }
    }

    let (storage_type, atomic_type) = if has_u8_field || (has_bool_field && max_discriminant > 127)
    {
        // Need u16: 8 bits for discriminant, 8 bits for data
        (quote! { u16 }, quote! { ::std::sync::atomic::AtomicU16 })
    } else {
        // Can use u8: 7 bits for discriminant, 1 bit for bool (if any)
        (quote! { u8 }, quote! { ::std::sync::atomic::AtomicU8 })
    };

    let use_u16 = has_u8_field || (has_bool_field && max_discriminant > 127);

    let to_storage = variants.iter().enumerate().map(|(idx, variant)| {
        let var_name = &variant.ident;
        let disc = idx as u8; // The discriminant here is just the variant's index

        match &variant.fields {
            // Simple unit variant, just store the discriminant
            Fields::Unit => {
                if use_u16 {
                    quote! { #name::#var_name => #disc as u16 }
                } else {
                    quote! { #name::#var_name => #disc }
                }
            }
            Fields::Named(fields) => {
                // Named field variant like `Write { schema_did_change: bool }`
                let field = &fields.named[0];
                let field_name = &field.ident;

                if is_bool_type(&field.ty) {
                    if use_u16 {
                        // Pack as: [discriminant_byte | bool_as_byte]
                        // Example: Write {true} with disc=3 becomes: b100000011
                        quote! {
                            #name::#var_name { ref #field_name } => {
                                (#disc as u16) | ((*#field_name as u16) << 8)
                            }
                        }
                    } else {
                        // Same as above but with u8, so only 1 bit for bool
                        // Example: Write{true} with disc=3 becomes: b10000011
                        quote! {
                            #name::#var_name { ref #field_name } => {
                                #disc | ((*#field_name as u8) << 7)
                            }
                        }
                    }
                } else {
                    // u8/i8 field always uses u16 to have enough bits
                    // Pack as: [discriminant_byte | value_byte]
                    quote! {
                        #name::#var_name { ref #field_name } => {
                            (#disc as u16) | ((*#field_name as u16) << 8)
                        }
                    }
                }
            }
            Fields::Unnamed(_) => {
                // same strategy as above, but for tuple variants like `Write(bool)`
                if is_bool_type(&variant.fields.iter().next().unwrap().ty) {
                    if use_u16 {
                        quote! {
                            #name::#var_name(ref val) => {
                                (#disc as u16) | ((*val as u16) << 8)
                            }
                        }
                    } else {
                        quote! {
                            #name::#var_name(ref val) => {
                                #disc | ((*val as u8) << 7)
                            }
                        }
                    }
                } else {
                    quote! {
                        #name::#var_name(ref val) => {
                            (#disc as u16) | ((*val as u16) << 8)
                        }
                    }
                }
            }
        }
    });

    // Generate the match arms for decoding the storage representation back to enum
    let from_storage = variants.iter().enumerate().map(|(idx, variant)| {
        let var_name = &variant.ident;
        let disc = idx as u8;

        match &variant.fields {
            Fields::Unit => quote! { #disc => #name::#var_name },
            Fields::Named(fields) => {
                let field = &fields.named[0];
                let field_name = &field.ident;

                if is_bool_type(&field.ty) {
                    if use_u16 {
                        // Extract bool from high byte: check if non-zero
                        quote! {
                            #disc => #name::#var_name {
                                #field_name: (val >> 8) != 0
                            }
                        }
                    } else {
                        // check single bool value at bit 7
                        quote! {
                            #disc => #name::#var_name {
                                #field_name: (val & 0x80) != 0
                            }
                        }
                    }
                } else {
                    quote! {
                        #disc => #name::#var_name {
                            // Extract u8/i8 from high byte and cast to appropriate type
                            #field_name: (val >> 8) as _
                        }
                    }
                }
            }
            Fields::Unnamed(_) => {
                if is_bool_type(&variant.fields.iter().next().unwrap().ty) {
                    if use_u16 {
                        quote! { #disc => #name::#var_name((val >> 8) != 0) }
                    } else {
                        quote! { #disc => #name::#var_name((val & 0x80) != 0) }
                    }
                } else {
                    quote! { #disc => #name::#var_name((val >> 8) as _) }
                }
            }
        }
    });

    let discriminant_mask = if use_u16 {
        quote! { 0xFF }
    } else {
        quote! { 0x7F }
    };
    let to_storage_arms_copy = to_storage.clone();

    let expanded = quote! {
        #[derive(Debug)]
        /// Atomic wrapper for #name
        pub struct #atomic_name(#atomic_type);

        impl #atomic_name {
            /// Encode enum into storage representation
            /// Discriminant in lower bits, field data in upper bits
            #[inline]
            fn to_storage(val: &#name) -> #storage_type {
                match val {
                    #(#to_storage_arms_copy),*
                }
            }

            /// Decode storage representation into enum
            /// Panics on invalid discriminant
            #[inline]
            fn from_storage(val: #storage_type) -> #name {
                let discriminant = (val & #discriminant_mask) as u8;
                match discriminant {
                    #(#from_storage,)*
                    _ => panic!(concat!("Invalid ", stringify!(#name), " discriminant: {}"), discriminant),
                }
            }

            /// Create new atomic enum with initial value
            #[inline]
            pub const fn new(val: #name) -> Self {
                // Can't call to_storage in const context, so inline it
                let storage = match val {
                    #(#to_storage),*
                };
                Self(#atomic_type::new(storage))
            }

            #[inline]
            /// Load and convert the current value to expected enum
            pub fn get(&self) -> #name {
                Self::from_storage(self.0.load(::std::sync::atomic::Ordering::SeqCst))
            }

            #[inline]
            /// Convert and store new value
            pub fn set(&self, val: #name) {
                self.0.store(Self::to_storage(&val), ::std::sync::atomic::Ordering::SeqCst)
            }

            #[inline]
            /// Store new value and return previous value
            pub fn swap(&self, val: #name) -> #name {
                let prev = self.0.swap(Self::to_storage(&val), ::std::sync::atomic::Ordering::SeqCst);
                Self::from_storage(prev)
            }
        }

        impl From<#name> for #atomic_name {
            fn from(val: #name) -> Self {
                Self::new(val)
            }
        }
    };

    TokenStream::from(expanded)
}

fn is_bool_type(ty: &Type) -> bool {
    if let Type::Path(path) = ty {
        path.path.is_ident("bool")
    } else {
        false
    }
}

fn is_u8_or_i8_type(ty: &Type) -> bool {
    if let Type::Path(path) = ty {
        path.path.is_ident("u8") || path.path.is_ident("i8")
    } else {
        false
    }
}
