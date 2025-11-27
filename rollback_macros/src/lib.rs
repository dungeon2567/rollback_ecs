use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{
    braced,
    parse::{Parse, ParseStream},
    parse_macro_input, Block, DeriveInput, Ident, Result, Token, Type,
};

struct ViewArg {
    ident: Ident,
    ty: Type,
    is_mut: bool,
}

fn parse_view_args(input: ParseStream) -> Result<Vec<ViewArg>> {
    let mut args = Vec::new();
    while !input.is_empty() {
        let ident: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty_view: Type = input.parse()?;

        // Check if it's &mut ViewMut<T> or just View<T>/ViewMut<T>
        let (ty_inner, is_mut): (Type, bool) = match ty_view {
            Type::Reference(ref tr) => {
                // Handle &mut ViewMut<T>
                if tr.mutability.is_none() {
                    return Err(input.error("expected &mut ViewMut<T>, not &ViewMut<T>"));
                }
                match &*tr.elem {
                    Type::Path(tp) => {
                        let seg = tp
                            .path
                            .segments
                            .last()
                            .ok_or_else(|| input.error("expected ViewMut<...>"))?;
                        if seg.ident != "ViewMut" {
                            return Err(input.error("expected &mut ViewMut<T>"));
                        }
                        match &seg.arguments {
                            syn::PathArguments::AngleBracketed(ab) => match ab.args.first() {
                                Some(syn::GenericArgument::Type(t)) => (t.clone(), true),
                                _ => return Err(input.error("expected ViewMut<T>")),
                            },
                            _ => return Err(input.error("expected ViewMut<T>")),
                        }
                    }
                    _ => return Err(input.error("expected &mut ViewMut<T>")),
                }
            }
            Type::Path(ref tp) => {
                // Handle View<T> or ViewMut<T> directly
                let seg = tp
                    .path
                    .segments
                    .last()
                    .ok_or_else(|| input.error("expected View/ViewMut<...>"))?;
                let is_mut = seg.ident == "ViewMut";
                if !(seg.ident == "View" || seg.ident == "ViewMut") {
                    return Err(input.error("expected View or ViewMut"));
                }
                match &seg.arguments {
                    syn::PathArguments::AngleBracketed(ab) => match ab.args.first() {
                        Some(syn::GenericArgument::Type(t)) => (t.clone(), is_mut),
                        _ => return Err(input.error("expected View<T>")),
                    },
                    _ => return Err(input.error("expected View<T>")),
                }
            }
            _ => return Err(input.error("expected View<T>, ViewMut<T>, or &mut ViewMut<T>")),
        };
        args.push(ViewArg {
            ident,
            ty: ty_inner,
            is_mut,
        });
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        } else {
            break;
        }
    }
    Ok(args)
}

fn parse_type_list_bracketed(input: ParseStream) -> Result<Vec<Type>> {
    let content;
    syn::bracketed!(content in input);
    let mut tys = Vec::new();
    while !content.is_empty() {
        let ty: Type = content.parse()?;
        tys.push(ty);
        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        } else {
            break;
        }
    }
    Ok(tys)
}

struct SystemInput {
    stage_ident: Ident,
    fn_ident: Ident,
    view_args: Vec<ViewArg>,
    all_types: Vec<Type>,
    none_types: Vec<Type>,
    any_types: Vec<Type>,
    changed_types: Vec<Type>,
    remove_types: Vec<Type>,
    parent: Option<Type>,
    after: Vec<Type>,
    before: Vec<Type>,
    body: Block,
}

impl Parse for SystemInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let stage_ident: Ident = input.parse()?;
        let outer;
        braced!(outer in input);
        let inner;
        if outer.peek(Ident) {
            let kw: Ident = outer.parse()?;
            if kw == "query" {
                outer.parse::<Token![!]>()?;
                let q;
                braced!(q in outer);
                inner = q;
            } else {
                return Err(outer.error("expected query!"));
            }
        } else {
            let q;
            braced!(q in outer);
            inner = q;
        }
        inner.parse::<Token![fn]>()?;
        let fn_ident: Ident = inner.parse()?;
        let args_paren;
        syn::parenthesized!(args_paren in inner);
        let view_args = parse_view_args(&args_paren)?;
        let mut all_types = Vec::new();
        let mut none_types = Vec::new();
        let mut any_types = Vec::new();
        let mut changed_types = Vec::new();
        let mut remove_types = Vec::new();
        let mut parent = None;
        let mut after = Vec::new();
        let mut before = Vec::new();
        while inner.peek(Ident) {
            let kw: Ident = inner.parse()?;
            if kw == "All" {
                inner.parse::<Token![=]>()?;
                all_types = parse_type_list_bracketed(&inner)?;
            } else if kw == "None" {
                inner.parse::<Token![=]>()?;
                none_types = parse_type_list_bracketed(&inner)?;
            } else if kw == "Any" {
                inner.parse::<Token![=]>()?;
                any_types = parse_type_list_bracketed(&inner)?;
            } else if kw == "Changed" {
                inner.parse::<Token![=]>()?;
                changed_types = parse_type_list_bracketed(&inner)?;
            } else if kw == "Remove" {
                inner.parse::<Token![=]>()?;
                remove_types = parse_type_list_bracketed(&inner)?;
            } else if kw == "Parent" {
                inner.parse::<Token![=]>()?;
                let parent_type: Type = inner.parse()?;
                parent = Some(parent_type);
            } else if kw == "After" {
                inner.parse::<Token![=]>()?;
                after = parse_type_list_bracketed(&inner)?;
            } else if kw == "Before" {
                inner.parse::<Token![=]>()?;
                before = parse_type_list_bracketed(&inner)?;
            } else {
                break;
            }
        }
        let body: Block = inner.parse()?;
        Ok(SystemInput {
            stage_ident,
            fn_ident,
            view_args,
            all_types,
            none_types,
            any_types,
            changed_types,
            remove_types,
            parent,
            after,
            before,
            body,
        })
    }
}

#[proc_macro]
pub fn system(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as SystemInput);

    let stage_ident = parsed.stage_ident;
    let fn_ident = parsed.fn_ident;
    let view_args = parsed.view_args;
    let all_types = parsed.all_types;
    let none_types = parsed.none_types;
    let any_types = parsed.any_types;
    let changed_types = parsed.changed_types;
    let remove_types = parsed.remove_types;
    let parent = parsed.parent;
    let after = parsed.after;
    let before = parsed.before;
    let body = parsed.body;

    let view_types: Vec<Type> = view_args.iter().map(|v| v.ty.clone()).collect();

    // Build unique storage set per type with mutability if any usage requires it
    let mut unique_types: Vec<Type> = Vec::new();
    let mut unique_mut_flags: Vec<bool> = Vec::new();
    let mut type_index: HashMap<String, usize> = HashMap::new();
    let requires_mut = |t: &Type| -> bool {
        let key = quote!(#t).to_string();
        let is_removed = remove_types.iter().any(|rt| quote!(#rt).to_string() == key);
        let is_mut_view = view_args.iter().any(|va| {
            let vty = &va.ty;
            va.is_mut && quote!(#vty).to_string() == key
        });
        is_removed || is_mut_view
    };
    let mut push_unique = |t: &Type| {
        let key = quote!(#t).to_string();
        if !type_index.contains_key(&key) {
            let idx = unique_types.len();
            unique_types.push(t.clone());
            unique_mut_flags.push(requires_mut(t));
            type_index.insert(key, idx);
        } else {
            // upgrade to mutable if new usage requires mut
            let idx = *type_index.get(&key).expect("Type index should exist in map since we checked contains_key");
            if requires_mut(t) {
                unique_mut_flags[idx] = true;
            }
        }
    };
    for t in &none_types {
        push_unique(t);
    }
    for t in &all_types {
        push_unique(t);
    }
    for t in &any_types {
        push_unique(t);
    }
    for t in &changed_types {
        push_unique(t);
    }
    for t in &remove_types {
        push_unique(t);
    }
    for t in &view_types {
        push_unique(t);
    }
    let unique_idents: Vec<Ident> = (0..unique_types.len())
        .map(|i| format_ident!("storage{}", i + 1))
        .collect();

    let resolve_storage_ident = |t: &Type| -> Ident {
        let key = quote!(#t).to_string();
        let idx = type_index.get(&key).cloned().unwrap_or(0);
        unique_idents[idx].clone()
    };

    let view_storage_idents: Vec<Ident> = view_types.iter().map(resolve_storage_ident).collect();
    let all_storage_idents: Vec<Ident> = all_types.iter().map(resolve_storage_ident).collect();
    let none_storage_idents: Vec<Ident> = none_types.iter().map(resolve_storage_ident).collect();
    let any_storage_idents: Vec<Ident> = any_types.iter().map(resolve_storage_ident).collect();
    let changed_storage_idents: Vec<Ident> =
        changed_types.iter().map(resolve_storage_ident).collect();
    let remove_storage_idents: Vec<Ident> =
        remove_types.iter().map(resolve_storage_ident).collect();

    // Deprecated per-type field idents; using unique storages instead

    // Generate the actual function definition (as an associated function, not a method)
    let fn_inputs = view_args.iter().map(|va| {
        let vi = &va.ident;
        let ty = &va.ty;
        if va.is_mut {
            quote!(#vi: &mut ::rollback_ecs::view::ViewMut<#ty>)
        } else {
            quote!(#vi: ::rollback_ecs::view::View<#ty>)
        }
    });

    let fn_definition = quote! {
        fn #fn_ident(#(#fn_inputs),*) #body
    };

    // Tuple type: one per unique type with mutability as required
    let unique_tuple_types = unique_types.iter().enumerate().map(|(i, t)| {
        if unique_mut_flags[i] {
            quote!(&mut ::rollback_ecs::storage::Storage<#t>)
        } else {
            quote!(&::rollback_ecs::storage::Storage<#t>)
        }
    });
    let _args_tuple_type = quote!( ( #( #unique_tuple_types ),* ) );

    // Destructure into unique storages only
    let _args_destructure = quote!( let ( #( #unique_idents ),* ) = args; );

    // Bind category aliases from unique storages - use reborrowing to avoid conflicts

    let outer_intersections = {
        let view_intersections = view_storage_idents
            .iter()
            .map(|si| quote!( outer_mask &= #si.root.presence_mask; ));

        let all_intersections = all_storage_idents
            .iter()
            .map(|si| quote!( outer_mask &= #si.root.presence_mask; ));

        quote! {
            #( #view_intersections )*
            #( #all_intersections )*
        }
    };
    
    let outer_none = if none_types.is_empty() {
        quote!()
    } else {
        // For None queries, use absence_mask at root level to skip entire middle blocks
        // that are full of excluded components. If a root block's absence_mask bit is set,
        // that means ALL entities in that middle block (16K entities) have the excluded component.
        // We can use absence_mask directly without checking presence_mask first because:
        // - If block doesn't exist, absence_mask bit won't be set
        // - If block exists but isn't full, absence_mask bit won't be set
        // - If block exists and is full, absence_mask bit will be set
        let per_none_outer = none_storage_idents.iter().map(|ni| {
            quote! {
                // Use absence_mask directly - no need to check presence_mask first
                // If the root block exists and the middle block is full, absence_mask will be set
                none_outer |= #ni.root.absence_mask;
            }
        });
        quote! { let mut none_outer: u128 = 0; #(#per_none_outer)* outer_mask &= !none_outer; }
    };
    let middle_intersections_views = {
        let view_intersections = view_storage_idents.iter().map(|si| {
            quote!( middle_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().presence_mask }; )
        });

        let all_intersections = all_storage_idents.iter().map(|si| {
            quote!( middle_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().presence_mask }; )
        });

        quote! {
            #( #view_intersections )*
            #( #all_intersections )*
        }
    };
    let inner_intersections_views = {
        let view_intersections = view_storage_idents.iter().map(|si| {
            quote!( inner_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().data[mi as usize].assume_init_ref().presence_mask }; )
        });

        let all_intersections = all_storage_idents.iter().map(|si| {
            quote!( inner_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().data[mi as usize].assume_init_ref().presence_mask }; )
        });

        quote! {
            #( #view_intersections )*
            #( #all_intersections )*
        }
    };

    let middle_all = if all_types.is_empty() {
        quote!()
    } else {
        let per_all_regular = all_storage_idents.iter().map(|ai| {
            quote! {
                let rp = #ai.root.presence_mask;
                let mut all_mid_single: u128 = u128::MAX;
                if ((rp >> oi) & 1) != 0 {
                    let ab = unsafe { #ai.root.data[oi as usize].assume_init_ref() };
                    all_mid_single &= ab.presence_mask;
                } else { all_mid_single &= 0; }
                all_mid &= all_mid_single;
            }
        });
        quote! { let mut all_mid: u128 = u128::MAX; #(#per_all_regular)* middle_mask &= all_mid; }
    };

    let middle_none = if none_types.is_empty() {
        quote!()
    } else {
        // For None queries, use absence_mask at middle level to skip entire inner blocks
        // that are full of excluded components. If a middle block's absence_mask bit is set,
        // that means ALL entities in that inner block have the excluded component.
        // We can use absence_mask directly without checking presence_mask first because:
        // - If block doesn't exist, absence_mask will be 0 (safe to read uninitialized)
        // - If block exists but isn't full, absence_mask bit won't be set
        // - If block exists and is full, absence_mask bit will be set
        let per_none_mid = none_storage_idents.iter().map(|ni| {
            quote! {
                // Use absence_mask to skip entire inner blocks (128 entities) that are full
                // Optimized: check root presence_mask once, then access middle absence_mask
                // This efficiently skips 128-entity inner blocks where excluded components are full
                if ((#ni.root.presence_mask >> oi) & 1) != 0 {
                    let nb = unsafe { #ni.root.data[oi as usize].assume_init_ref() };
                    none_mid |= nb.absence_mask;
                }
            }
        });
        quote! { let mut none_mid: u128 = 0; #(#per_none_mid)* middle_mask &= !none_mid; }
    };

    let middle_any = if any_types.is_empty() {
        quote!()
    } else {
        let per_any = any_storage_idents.iter().map(|ai| {
            quote! {
                let rp = #ai.root.presence_mask;
                if ((rp >> oi) & 1) != 0 {
                    let ab = unsafe { #ai.root.data[oi as usize].assume_init_ref() };
                    any_mid |= ab.presence_mask;
                }
            }
        });
        quote! { let mut any_mid: u128 = 0; #(#per_any)* middle_mask &= any_mid; }
    };

    let middle_changed = if changed_types.is_empty() {
        quote!()
    } else {
        let per_changed = changed_storage_idents.iter().map(|ci| {
            quote! {
                let rp = #ci.root.presence_mask;
                if ((rp >> oi) & 1) != 0 {
                    let cb = unsafe { #ci.root.data[oi as usize].assume_init_ref() };
                    changed_mid |= cb.changed_mask;
                }
            }
        });
        quote! { let mut changed_mid: u128 = 0; #(#per_changed)* middle_mask &= changed_mid; }
    };

    let inner_all = if all_types.is_empty() {
        quote!()
    } else {
        let per_all_regular = all_storage_idents.iter().map(|ai| {
            quote! {
                let ab = unsafe { #ai.root.data[oi as usize].assume_init_ref() };
                let mp = ab.presence_mask;
                let mut all_in_single: u128 = u128::MAX;
                if ((mp >> mi) & 1) != 0 {
                    let ib = unsafe { ab.data[mi as usize].assume_init_ref() };
                    all_in_single &= ib.presence_mask;
                } else { all_in_single &= 0; }
                all_in &= all_in_single;
            }
        });
        quote! { let mut all_in: u128 = u128::MAX; #(#per_all_regular)* inner_mask &= all_in; }
    };

    let inner_none = if none_types.is_empty() {
        quote!()
    } else {
        // For None queries, use absence_mask at inner level to filter out entities
        // that have the excluded component. absence_mask tracks "currently occupied"
        // which is exactly what we need for None queries.
        // We can use absence_mask directly without checking presence_mask first because:
        // - If block doesn't exist, absence_mask bit won't be set
        // - If block exists but isn't full, absence_mask bit won't be set at inner level
        // - If entity has component, absence_mask bit will be set
        let per_none = none_storage_idents.iter().map(|ni| {
            quote! {
                // Use absence_mask for performance - check if blocks exist first for safety
                let rp = #ni.root.presence_mask;
                if ((rp >> oi) & 1) != 0 {
                    let nb = unsafe { #ni.root.data[oi as usize].assume_init_ref() };
                    let mp = nb.presence_mask;
                    if ((mp >> mi) & 1) != 0 {
                        let ib = unsafe { nb.data[mi as usize].assume_init_ref() };
                        // Use absence_mask to get entities that currently have the excluded component
                        none_in |= ib.absence_mask;
                    }
                }
            }
        });
        quote! { let mut none_in: u128 = 0; #(#per_none)* inner_mask &= !none_in; }
    };

    let inner_any = if any_types.is_empty() {
        quote!()
    } else {
        let per_any = any_storage_idents.iter().map(|ai| {
            quote! {
                let rp = #ai.root.presence_mask;
                if ((rp >> oi) & 1) != 0 {
                    let ab = unsafe { #ai.root.data[oi as usize].assume_init_ref() };
                    let mp = ab.presence_mask;
                    if ((mp >> mi) & 1) != 0 {
                        let ib = unsafe { ab.data[mi as usize].assume_init_ref() };
                        any_in |= ib.presence_mask;
                    }
                }
            }
        });
        quote! {
            let mut any_in: u128 = 0;
            #(#per_any)*
            inner_mask &= any_in;
        }
    };

    let inner_changed = if changed_types.is_empty() {
        quote!()
    } else {
        let per_changed = changed_storage_idents.iter().map(|ci| {
            quote! {
                let rp = #ci.root.presence_mask;
                if ((rp >> oi) & 1) != 0 {
                    let cb = unsafe { #ci.root.data[oi as usize].assume_init_ref() };
                    let mp = cb.presence_mask;
                    if ((mp >> mi) & 1) != 0 {
                        let ib = unsafe { cb.data[mi as usize].assume_init_ref() };
                        changed_in |= ib.changed_mask;
                    }
                }
            }
        });
        quote! { let mut changed_in: u128 = 0; #(#per_changed)* inner_mask &= changed_in; }
    };

    // Generate function call with View/ViewMut arguments - call for EACH entity in the run
    let call_views = if !view_args.is_empty() {
        // Create View/ViewMut construction for each argument
        let view_constructions = view_args.iter().enumerate().map(|(i, va)| {
            let arg_ident = &va.ident;
            let storage_ident = &view_storage_idents[i];

            if va.is_mut {
                quote! {
                    let mut #arg_ident = {
                        let entity_index = ((oi as u32 * 128 * 128) + (mi as u32 * 128) + ii);

                        ::rollback_ecs::view::ViewMut::new(
                            &mut *#storage_ident,
                            entity_index
                        )
                    };
                }
            } else {
                quote! {
                    let #arg_ident = {
                        let root = unsafe { &#storage_ident.root };
                        let middle = unsafe { root.data[oi as usize].assume_init_ref() };
                        let inner = unsafe { middle.data[mi as usize].assume_init_ref() };

                        ::rollback_ecs::view::View::new(
                            unsafe { inner.data[ii as usize].assume_init_ref() }
                        )
                    };
                }
            }
        });

        let arg_idents: Vec<_> = view_args
            .iter()
            .enumerate()
            .map(|(_i, va)| {
                let ident = &va.ident;
                if va.is_mut {
                    quote!(&mut #ident)
                } else {
                    quote!(#ident)
                }
            })
            .collect();

        let change_checks = view_args.iter().map(|va| {
            if va.is_mut {
                quote! {}
            } else {
                quote!()
            }
        });

        quote! {
            for ii in start..(start + run) {
                #( #view_constructions )*
                #stage_ident::#fn_ident(#(#arg_idents),*);
                #( #change_checks )*
            }
        }
    } else {
        quote! { #stage_ident::#fn_ident(); }
    };

    let remove_components = if !remove_types.is_empty() {
        let remove_logic = remove_types.iter().enumerate().map(|(i, t)| {
            let ident = &remove_storage_idents[i];
            let ty_str = quote!(#t).to_string();
            if ty_str.ends_with("Entity") {
                quote! {
                    {
                        let root = &mut #ident.root;
                        if ((root.presence_mask >> oi) & 1) != 0 {
                            let middle = unsafe { root.data[oi as usize].assume_init_mut() };
                            if ((middle.presence_mask >> mi) & 1) != 0 {
                                let inner = unsafe { middle.data[mi as usize].assume_init_mut() };
                                let ptr = inner.data.as_ptr();
                                // range_mask represents occupied entities from query
                                // presence_mask is sufficient - absence_mask is kept in sync
                                let mut mask = range_mask & inner.presence_mask;
                                while mask != 0 {
                                    let ii = mask.trailing_zeros();
                                    unsafe { ptr.add(ii as usize).read().assume_init_drop(); }
                                    mask &= !(1u128 << ii);
                                }
                                inner.presence_mask &= !range_mask;
                                inner.absence_mask &= !range_mask;
                                if inner.absence_mask != u128::MAX {
                                    middle.absence_mask &= !(1u128 << mi);
                                }
                                if middle.absence_mask != u128::MAX {
                                    root.absence_mask &= !(1u128 << oi);
                                }
                            }
                        }
                    }
                }
            } else {
                quote! {
                    {
                        let root = &mut #ident.root;
                        if ((root.presence_mask >> oi) & 1) != 0 {
                            let middle = unsafe { root.data[oi as usize].assume_init_mut() };
                            if ((middle.presence_mask >> mi) & 1) != 0 {
                                let inner = unsafe { middle.data[mi as usize].assume_init_mut() };
                                let ptr = inner.data.as_ptr();
                                // range_mask represents occupied entities from query
                                // presence_mask is sufficient - absence_mask is kept in sync
                                let mut mask = range_mask & inner.presence_mask;
                                while mask != 0 {
                                    let ii = mask.trailing_zeros();
                                    unsafe { ptr.add(ii as usize).read().assume_init_drop(); }
                                    mask &= !(1u128 << ii);
                                }
                                inner.presence_mask &= !range_mask;
                                inner.absence_mask &= !range_mask;
                                if inner.absence_mask != u128::MAX {
                                    middle.absence_mask &= !(1u128 << mi);
                                }
                                if middle.absence_mask != u128::MAX {
                                    root.absence_mask &= !(1u128 << oi);
                                }
                            }
                        }
                    }
                }
            }
        });
        quote! { #(#remove_logic)* }
    } else {
        quote!()
    };

    // Stage fields: one per unique type
    let struct_fields_unique = unique_types.iter().enumerate().map(|(i, t)| {
        let id = &unique_idents[i];
        quote!( pub #id: std::rc::Rc<std::cell::UnsafeCell<::rollback_ecs::storage::Storage<#t>>> , )
    });

    // Build run args from unique storages (unsafe access)

    // Access once per unique type into locals - always mutably if any usage requires it
    let borrow_locals: Vec<proc_macro2::TokenStream> = unique_types
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let id = &unique_idents[i];
            if unique_mut_flags[i] {
                quote!( let mut #id = unsafe { &mut *self.#id.get() }; )
            } else {
                quote!( let #id = unsafe { &*self.#id.get() }; )
            }
        })
        .collect();

    let create_fields_unique = unique_types.iter().enumerate().map(|(i, t)| {
        let id = &unique_idents[i];
        quote!( #id: world.get_storage::<#t>() )
    });

    let reads_unique = unique_types.iter().enumerate().filter_map(|(i, t)| {
        if unique_mut_flags[i] {
            None
        } else {
            Some(quote!( std::any::TypeId::of::<#t>() ))
        }
    });
    let writes_unique = unique_types.iter().enumerate().filter_map(|(i, t)| {
        if unique_mut_flags[i] {
            Some(quote!( std::any::TypeId::of::<#t>() ))
        } else {
            None
        }
    });

    // Generate parent(), after(), and before() implementations if specified
    // Default to SimulationGroup if no parent is specified
    let parent_impl = if let Some(ref parent_ty) = parent {
        quote! {
            fn parent(&self) -> ::std::option::Option<::std::any::TypeId> {
                ::std::option::Option::Some(::std::any::TypeId::of::<#parent_ty>())
            }
        }
    } else {
        // Default to SimulationGroup
        quote! {
            fn parent(&self) -> ::std::option::Option<::std::any::TypeId> {
                ::std::option::Option::Some(::std::any::TypeId::of::<::rollback_ecs::scheduler::SimulationGroup>())
            }
        }
    };

    let after_impl = if !after.is_empty() {
        let after_types: Vec<_> = after
            .iter()
            .map(|ty| quote!(::std::any::TypeId::of::<#ty>()))
            .collect();
        quote! {
            fn after(&self) -> &'static [::std::any::TypeId] {
                static AFTER: &[::std::any::TypeId] = &[#(#after_types),*];
                AFTER
            }
        }
    } else {
        quote!()
    };

    let before_impl = if !before.is_empty() {
        let before_types: Vec<_> = before
            .iter()
            .map(|ty| quote!(::std::any::TypeId::of::<#ty>()))
            .collect();
        quote! {
            fn before(&self) -> &'static [::std::any::TypeId] {
                static BEFORE: &[::std::any::TypeId] = &[#(#before_types),*];
                BEFORE
            }
        }
    } else {
        quote!()
    };

    // query_impl defined above with full implementation

    let expanded = quote! {
        pub struct #stage_ident { #( #struct_fields_unique )* }
        impl #stage_ident {
            #fn_definition
        }
        impl ::rollback_ecs::scheduler::PipelineStage for #stage_ident {
            fn type_id(&self) -> ::std::any::TypeId {
                ::std::any::TypeId::of::<Self>()
            }

            fn run(&self) {
                #( #borrow_locals )*

                let mut outer_mask: u128 = u128::MAX;
                #outer_intersections
                // Apply outer_none AFTER intersections to filter out full middle blocks efficiently
                // This skips entire 16k-entity middle blocks where excluded components are full
                #outer_none
                while outer_mask != 0 {
                    let oi = outer_mask.trailing_zeros();
                    let mut middle_mask: u128 = u128::MAX;
                    #middle_intersections_views
                    #middle_all
                    #middle_none
                    #middle_any
                    #middle_changed
                    while middle_mask != 0 {
                        let mi = middle_mask.trailing_zeros();
                        let mut inner_mask: u128 = u128::MAX;
                        #inner_intersections_views
                        #inner_all
                        #inner_none
                        #inner_any
                        #inner_changed
                        while inner_mask != 0 {
                            let start = inner_mask.trailing_zeros();
                            let run = (inner_mask >> start).trailing_ones();
                            #call_views


                            let range_mask = if run == 128 { u128::MAX } else { ((1u128 << run) - 1) << start };
                            #remove_components
                            inner_mask &= !range_mask;
                        }
                        middle_mask &= !(1u128 << mi);
                    }
                    outer_mask &= !(1u128 << oi);
                }
            }
            fn create(world: &mut ::rollback_ecs::world::World) -> Self {
                Self { #( #create_fields_unique ),* }
            }
            fn reads(&self) -> &'static [std::any::TypeId] {
                static READS: &[std::any::TypeId] = &[ #( #reads_unique ),* ];
                READS
            }
            fn writes(&self) -> &'static [std::any::TypeId] {
                static WRITES: &[std::any::TypeId] = &[ #( #writes_unique ),* ];
                WRITES
            }

            #parent_impl
            #after_impl
            #before_impl
        }

        unsafe impl ::std::marker::Send for #stage_ident {}
        unsafe impl ::std::marker::Sync for #stage_ident {}
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Component)]
pub fn component_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let cleanup_name = syn::Ident::new(&format!("{}CleanupSystem", name), name.span());

    let gen = quote! {
        // Use absolute paths that work both inside and outside the crate
        impl ::rollback_ecs::component::Resource for #name {
            fn type_index() -> usize {
                static TYPE_INDEX: ::std::sync::OnceLock<usize> = ::std::sync::OnceLock::new();

                *TYPE_INDEX.get_or_init(|| ::rollback_ecs::component::next_id())
            }
        }

        impl ::rollback_ecs::component::Component for #name {
            fn cleanup_system(world: &mut ::rollback_ecs::world::World) -> Box<dyn ::rollback_ecs::scheduler::PipelineStage> {
                Box::new(<#cleanup_name as ::rollback_ecs::scheduler::PipelineStage>::create(world))
            }
        }

        pub struct #cleanup_name(::rollback_ecs::system::ComponentCleanupSystem<#name>);

        impl ::rollback_ecs::scheduler::PipelineStage for #cleanup_name {
            fn type_id(&self) -> ::std::any::TypeId {
                ::std::any::TypeId::of::<Self>()
            }

            fn create(world: &mut ::rollback_ecs::world::World) -> Self {
                #cleanup_name(::rollback_ecs::system::ComponentCleanupSystem::create(world))
            }

            fn reads(&self) -> &'static [::std::any::TypeId] {
                self.0.reads()
            }

            fn writes(&self) -> &'static [::std::any::TypeId] {
                static WRITES: &[::std::any::TypeId] = &[::std::any::TypeId::of::<#name>()];
                WRITES
            }

            fn parent(&self) -> ::std::option::Option<::std::any::TypeId> {
                ::std::option::Option::Some(::std::any::TypeId::of::<::rollback_ecs::scheduler::CleanupGroup>())
            }

            fn run(&self){
                self.0.run();
            }
        }

        unsafe impl ::std::marker::Send for #cleanup_name {}
        unsafe impl ::std::marker::Sync for #cleanup_name {}
    };

    gen.into()
}

#[proc_macro_derive(Tag)]
pub fn tag_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as syn::DeriveInput);
    let name = &ast.ident;
    let gen = quote! {
        impl crate::component::Component for #name {}
    };
    gen.into()
}

#[proc_macro]
pub fn variadic_system(input: TokenStream) -> TokenStream {
    system(input)
}

// Parse attributes like After=[A, B] or Before=[C, D]
struct PipelineGroupAttrs {
    after: Vec<Type>,
    before: Vec<Type>,
    parent: Option<Type>,
}

impl Parse for PipelineGroupAttrs {
    fn parse(input: ParseStream) -> std::result::Result<Self, syn::Error> {
        let mut after = Vec::new();
        let mut before = Vec::new();
        let mut parent = None;

        while !input.is_empty() {
            let ident: Ident = input.parse()?;

            if ident == "After" {
                input.parse::<Token![=]>()?;
                let types = parse_type_list_bracketed(input)?;
                after.extend(types);
            } else if ident == "Before" {
                input.parse::<Token![=]>()?;
                let types = parse_type_list_bracketed(input)?;
                before.extend(types);
            } else if ident == "Parent" {
                input.parse::<Token![=]>()?;
                let ty: Type = input.parse()?;
                parent = Some(ty);
            } else {
                return Err(input.error(format!("unknown attribute: {}", ident)));
            }

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(PipelineGroupAttrs {
            after,
            before,
            parent,
        })
    }
}

#[proc_macro_attribute]
pub fn pipeline_group(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as DeriveInput);

    // Parse the attribute arguments from the args token stream
    // The args should be like: After=[A, B], Before=[C, D]
    let attrs = if args.is_empty() {
        PipelineGroupAttrs {
            after: Vec::new(),
            before: Vec::new(),
            parent: None,
        }
    } else {
        // Convert proc_macro::TokenStream to proc_macro2::TokenStream for parsing
        let proc_macro2_args: proc_macro2::TokenStream = args.into();

        // Parse using syn's parser
        syn::parse2(proc_macro2_args).unwrap_or_else(|e| {
            panic!("Failed to parse pipeline_group attributes: {}", e);
        })
    };

    let name = &item.ident;

    // Generate after() implementation
    let after_impl = if attrs.after.is_empty() {
        quote!()
    } else {
        let after_types: Vec<_> = attrs
            .after
            .iter()
            .map(|ty| quote!(::std::any::TypeId::of::<#ty>()))
            .collect();
        quote! {
            fn after(&self) -> &'static [::std::any::TypeId] {
                static AFTER: &[::std::any::TypeId] = &[#(#after_types),*];
                AFTER
            }
        }
    };

    // Generate before() implementation
    let before_impl = if attrs.before.is_empty() {
        quote!()
    } else {
        let before_types: Vec<_> = attrs
            .before
            .iter()
            .map(|ty| quote!(::std::any::TypeId::of::<#ty>()))
            .collect();
        quote! {
            fn before(&self) -> &'static [::std::any::TypeId] {
                static BEFORE: &[::std::any::TypeId] = &[#(#before_types),*];
                BEFORE
            }
        }
    };

    // Generate parent() implementation
    let parent_impl = if let Some(parent_ty) = &attrs.parent {
        quote! {
            fn parent(&self) -> ::std::option::Option<::std::any::TypeId> {
                ::std::option::Option::Some(::std::any::TypeId::of::<#parent_ty>())
            }
        }
    } else {
        quote!()
    };

    // Remove the pipeline_group attribute so it doesn't appear in the output
    item.attrs
        .retain(|attr| !attr.path().is_ident("pipeline_group"));

    // Generate instance() implementation for PipelineGroup
    let instance_impl = quote! {
        fn instance() -> &'static Self {
            static INSTANCE: #name = #name;
            &INSTANCE
        }
    };

    // Generate only PipelineGroup impl (not PipelineStage)
    let expanded = quote! {
        #item

        impl ::rollback_ecs::scheduler::PipelineGroup for #name {
            #instance_impl
            #before_impl
            #after_impl
            #parent_impl
        }
    };

    TokenStream::from(expanded)
}
