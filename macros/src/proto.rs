use proc_macro as pm;
use proc_macro2 as pm2;
use quote::quote;

pub(super) fn execute(item: pm::TokenStream) -> pm::TokenStream {
    let item: syn::Item = syn::parse_macro_input!(item as syn::Item);
    match item {
        syn::Item::Enum(item) => rewrite_enum(item),
        syn::Item::Struct(item) => rewrite_struct(item),
        _ => panic!("#[macros::proto] expects enum or struct as input"),
    }
}

fn rewrite_enum(mut item: syn::ItemEnum) -> pm::TokenStream {
    let enum_ident = syn::Ident::new(&format!("Enum{}", item.ident), pm2::Span::call_site());
    let struct_ident = std::mem::replace(&mut item.ident, enum_ident.clone());
    item.variants
        .iter_mut()
        .enumerate()
        .for_each(|(tag, variant)| {
            assert!(
                variant.fields.len() == 1,
                "#[macros::proto] expects variant fields to take exactly one argument"
            );
            let field = variant.fields.iter_mut().next().unwrap();
            let (attr, is_unit) = ty_to_prost_attr(&field.ty, Some(tag));
            variant.attrs.push(attr);
            if is_unit {
                field.ty = syn::parse_quote!(Unit)
            }
        });
    let tags = item
        .variants
        .iter()
        .enumerate()
        .map(|(i, _)| format!("{}", i))
        .collect::<Vec<_>>()
        .join(",");
    let attrs = item.attrs.clone();
    let tags = syn::LitStr::new(&tags, pm2::Span::call_site());
    let enum_ident_str = syn::LitStr::new(&enum_ident.to_string(), enum_ident.span());
    quote!(
        #[#(#attrs),*]
        #[derive(prost::Message)]
        pub struct #struct_ident {
            #[prost(oneof = #enum_ident_str, tags = #tags)]
            pub this: Option<#enum_ident>
        }
        #[derive(prost::Oneof)]
        #item
        use #enum_ident::*;
        impl #enum_ident {
            pub fn wrap(self) -> #struct_ident {
                #struct_ident { this: Some(self) }
            }
        }
    )
    .into()
}

fn rewrite_struct(mut item: syn::ItemStruct) -> pm::TokenStream {
    let (params, args): (Vec<_>, Vec<_>) = item
        .fields
        .iter_mut()
        .map(|field| {
            let (attr, is_unit) = ty_to_prost_attr(&field.ty, None);
            field.attrs.push(attr);
            if is_unit {
                field.ty = syn::parse_quote!(Unit)
            }
            let ty = &field.ty.clone();
            let ident = field
                .ident
                .clone()
                .expect("#[macros::proto] expects structs to have named fields");
            let param = quote!(#ident:#ty);
            let arg = quote!(#ident);
            (param, arg)
        })
        .unzip();
    let ident = item.ident.clone();
    quote!(
        #[derive(prost::Message)]
        #item
        impl #ident {
            #[allow(clippy::too_many_arguments)]
            fn new(#(#params),*) -> Self {
                Self { #(#args),* }
            }
        }
    )
    .into()
}

fn ty_to_prost_attr(ty: &syn::Type, tag: Option<usize>) -> (syn::Attribute, bool) {
    let mut is_unit = false;
    let ty = match &ty {
        syn::Type::Path(ty) => {
            let seg = ty.path.segments.iter().next().unwrap();
            match seg.ident.to_string().as_str() {
                "i32" => "int32",
                "i64" => "int64",
                "bool" => "bool",
                "f32" => "float",
                "f64" => "double",
                "u32" => "uint32",
                "u64" => "uint64",
                "String" => "string",
                // This case covers messages which are wrapped in Box<T> as well
                _ => "message",
            }
            .to_string()
        }
        syn::Type::Tuple(ty) if ty.elems.is_empty() => {
            is_unit = true;
            "message".to_string()
        }
        _ => panic!("#[macros::proto] expects all types to be mangled and de-aliased."),
    };
    let ident = syn::Ident::new(&ty, pm2::Span::call_site());
    let attr = if let Some(tag) = tag {
        let lit = syn::LitStr::new(&format!("{}", tag), pm2::Span::call_site());
        syn::parse_quote!(#[prost(#ident, tag = #lit)])
    } else {
        syn::parse_quote!(#[prost(#ident, required)])
    };
    (attr, is_unit)
}
