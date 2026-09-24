//! `#[transform]` turns a plain function of shape
//! `fn name(event: &Event<'_>) -> Result<Vec<Event<'static>>, DomainError>`
//! into a registered `Transform` impl, with zero manual wiring:
//!
//! 1. Generates a unit struct `<PascalCaseName>Transform` implementing
//!    `domain::Transform`, whose `apply` just calls the original function.
//! 2. Emits `inventory::submit!` so the struct is discoverable at runtime
//!    via `inventory::iter::<TransformRegistration>()` — no central list
//!    of transforms to keep in sync as plugins are added.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn transform(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let struct_name = format_ident!("{}Transform", to_pascal_case(&fn_name_str));

    let expanded = quote! {
        #input_fn

        #[doc(hidden)]
        struct #struct_name;

        impl ::domain::Transform for #struct_name {
            fn name(&self) -> &str {
                #fn_name_str
            }

            fn apply<'a>(
                &self,
                event: &::domain::Event<'a>,
            ) -> Result<Vec<::domain::Event<'static>>, ::domain::DomainError> {
                #fn_name(event)
            }
        }

        ::inventory::submit! {
            ::domain::TransformRegistration {
                name: #fn_name_str,
                factory: || Box::new(#struct_name),
            }
        }
    };

    expanded.into()
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}