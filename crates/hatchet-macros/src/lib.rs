use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, GenericArgument, Item, ItemFn, ItemMod, Lit, ReturnType, Type, parse::Parser,
    parse_macro_input, punctuated::Punctuated,
};

#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_task_like(attr, item, false)
}

#[proc_macro_attribute]
pub fn durable_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_task_like(attr, item, true)
}

#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    let workflow_name = parse_workflow_name(attr);
    let module = parse_macro_input!(item as ItemMod);
    let module_ident = module.ident.clone();

    let Some((_, items)) = &module.content else {
        return syn::Error::new_spanned(&module, "#[workflow] requires an inline module body")
            .to_compile_error()
            .into();
    };

    let mut registrations = Vec::new();
    for item in items {
        let Item::Fn(function) = item else {
            continue;
        };

        let is_task = has_attr(&function.attrs, "task");
        let is_durable = has_attr(&function.attrs, "durable_task");
        if !is_task && !is_durable {
            continue;
        }

        if let Err(error) = function_signature_types(function) {
            return error.to_compile_error().into();
        }

        let fn_name = &function.sig.ident;
        let handle_fn = if is_durable {
            format_ident!("{}_durable_task_handle", fn_name)
        } else {
            format_ident!("{}_task_handle", fn_name)
        };
        let registration = if is_durable {
            quote! {
                builder = builder.register_durable_task(
                    &#module_ident::#handle_fn(client),
                    #module_ident::#fn_name,
                );
            }
        } else {
            quote! {
                builder = builder.register_task(
                    &#module_ident::#handle_fn(client),
                    #module_ident::#fn_name,
                );
            }
        };
        registrations.push(registration);
    }

    let Some(name) = workflow_name else {
        return quote! { #module }.into();
    };

    quote! {
        #module

        pub const WORKFLOW_NAME: &str = #name;

        pub fn register_all<T>(
            client: &::hatchet_sdk::Client<T>,
            mut builder: ::hatchet_sdk::WorkerBuilder<T>,
        ) -> ::hatchet_sdk::WorkerBuilder<T>
        where
            T: ::hatchet_sdk::Transport + 'static,
        {
            #(#registrations)*
            builder
        }
    }
    .into()
}

fn expand_task_like(attr: TokenStream, item: TokenStream, durable: bool) -> TokenStream {
    drop(attr);
    let function = parse_macro_input!(item as ItemFn);

    let (input_type, output_type) = match function_signature_types(&function) {
        Ok(types) => types,
        Err(error) => return error.to_compile_error().into(),
    };

    let fn_name = &function.sig.ident;
    let upper_name = fn_name.to_string().to_ascii_uppercase();
    let name_const = format_ident!("{upper_name}_NAME");
    let handle_fn = if durable {
        format_ident!("{}_durable_task_handle", fn_name)
    } else {
        format_ident!("{}_task_handle", fn_name)
    };
    let task_name = fn_name.to_string();

    let return_type = if durable {
        quote! { ::hatchet_sdk::DurableTaskHandle<#input_type, #output_type> }
    } else {
        quote! { ::hatchet_sdk::TaskHandle<#input_type, #output_type> }
    };

    let handle_body = if durable {
        quote! {
            client.durable_task::<#input_type, #output_type>(#name_const)
        }
    } else {
        quote! {
            client.task::<#input_type, #output_type>(#name_const)
        }
    };

    quote! {
        #function

        pub const #name_const: &str = #task_name;

        pub fn #handle_fn<T>(
            client: &::hatchet_sdk::Client<T>,
        ) -> #return_type
        where
            T: ::hatchet_sdk::Transport,
        {
            #handle_body
        }
    }
    .into()
}

fn parse_workflow_name(attr: TokenStream) -> Option<syn::LitStr> {
    let tokens = proc_macro2::TokenStream::from(attr);
    if tokens.is_empty() {
        return None;
    }

    let parser = Punctuated::<syn::MetaNameValue, syn::Token![,]>::parse_terminated;
    let arguments = parser.parse2(tokens).ok()?;
    for argument in arguments {
        if argument.path.is_ident("name") {
            let syn::Expr::Lit(expr_lit) = argument.value else {
                continue;
            };
            if let Lit::Str(value) = expr_lit.lit {
                return Some(value);
            }
        }
    }
    None
}

fn has_attr(attrs: &[syn::Attribute], ident: &str) -> bool {
    attrs
        .iter()
        .any(|attribute| attribute.path().is_ident(ident))
}

fn function_signature_types(function: &ItemFn) -> syn::Result<(Type, Type)> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "task function must be async",
        ));
    }

    let mut args = function.sig.inputs.iter();
    let Some(FnArg::Typed(input_arg)) = args.next() else {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "task function must take input and context arguments",
        ));
    };
    let Some(FnArg::Typed(_ctx_arg)) = args.next() else {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "task function must take context as second argument",
        ));
    };

    let ReturnType::Type(_, output_type) = &function.sig.output else {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "task function must return Result<Output, _> or Result<Output>",
        ));
    };

    let Some(ok_type) = extract_result_ok_type(output_type) else {
        return Err(syn::Error::new_spanned(
            output_type,
            "task function return type must be Result<Output, _> or Result<Output>",
        ));
    };

    Ok(((*input_arg.ty).clone(), ok_type))
}

fn extract_result_ok_type(output_type: &Type) -> Option<Type> {
    let Type::Path(path) = output_type else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| {
        if let GenericArgument::Type(ty) = argument {
            Some(ty.clone())
        } else {
            None
        }
    })
}
