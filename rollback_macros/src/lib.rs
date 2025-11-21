use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{braced, parse::{Parse, ParseStream}, parse_macro_input, Block, DeriveInput, Ident, Result, Token, Type};

struct ViewArg { ident: Ident, ty: Type, is_mut: bool }

fn parse_view_args(input: ParseStream) -> Result<Vec<ViewArg>> {
    let mut args = Vec::new();
    while !input.is_empty() {
        let ident: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty_view: Type = input.parse()?;
        let (ty_inner, is_mut): (Type, bool) = match ty_view {
            Type::Path(ref tp) => {
                let seg = tp.path.segments.last().ok_or_else(|| input.error("expected View/Mut<...>"))?;
                let is_mut = seg.ident == "ViewMut";
                if !(seg.ident == "View" || seg.ident == "ViewMut") { return Err(input.error("expected View or ViewMut")); }
                match &seg.arguments {
                    syn::PathArguments::AngleBracketed(ab) => {
                        match ab.args.first() {
                            Some(syn::GenericArgument::Type(t)) => (t.clone(), is_mut),
                            _ => return Err(input.error("expected View<T>")),
                        }
                    }
                    _ => return Err(input.error("expected View<T>")),
                }
            }
            _ => return Err(input.error("expected View<T>")),
        };
        args.push(ViewArg { ident, ty: ty_inner, is_mut });
        if input.peek(Token![,]) { input.parse::<Token![,]>()?; } else { break; }
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
        if content.peek(Token![,]) { content.parse::<Token![,]>()?; } else { break; }
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
                let q; braced!(q in outer); inner = q;
            } else {
                return Err(outer.error("expected query!"));
            }
        } else {
            let q; braced!(q in outer); inner = q;
        }
        inner.parse::<Token![fn]>()?;
        let fn_ident: Ident = inner.parse()?;
        let args_paren; syn::parenthesized!(args_paren in inner);
        let view_args = parse_view_args(&args_paren)?;
        let mut all_types = Vec::new();
        let mut none_types = Vec::new();
        let mut any_types = Vec::new();
        let mut changed_types = Vec::new();
        let mut remove_types = Vec::new();
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
            } else { break; }
        }
        let body: Block = inner.parse()?;
        Ok(SystemInput { stage_ident, fn_ident, view_args, all_types, none_types, any_types, changed_types, remove_types, body })
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
    let body = parsed.body;

    let view_types: Vec<Type> = view_args.iter().map(|v| v.ty.clone()).collect();

    // Build unique storage set per type with mutability if any usage requires it
    let mut unique_types: Vec<Type> = Vec::new();
    let mut unique_mut_flags: Vec<bool> = Vec::new();
    let mut type_index: HashMap<String, usize> = HashMap::new();
    let requires_mut = |t: &Type| -> bool {
        let key = quote!(#t).to_string();
        remove_types.iter().any(|rt| quote!(#rt).to_string() == key)
            || view_args.iter().any(|va| va.is_mut && quote!(#(&va.ty)).to_string() == key)
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
            let idx = *type_index.get(&key).unwrap();
            if requires_mut(t) { unique_mut_flags[idx] = true; }
        }
    };
    for t in &none_types { push_unique(t); }
    for t in &all_types { push_unique(t); }
    for t in &any_types { push_unique(t); }
    for t in &changed_types { push_unique(t); }
    for t in &remove_types { push_unique(t); }
    for t in &view_types { push_unique(t); }
    let unique_idents: Vec<Ident> = (0..unique_types.len()).map(|i| format_ident!("storage{}", i+1)).collect();

    let resolve_storage_ident = |t: &Type| -> Ident {
        let key = quote!(#t).to_string();
        let idx = type_index.get(&key).cloned().unwrap_or(0);
        unique_idents[idx].clone()
    };

    let view_storage_idents: Vec<Ident> = view_types.iter().map(resolve_storage_ident).collect();
    let all_storage_idents: Vec<Ident> = all_types.iter().map(resolve_storage_ident).collect();
    let none_storage_idents: Vec<Ident> = none_types.iter().map(resolve_storage_ident).collect();
    let any_storage_idents: Vec<Ident> = any_types.iter().map(resolve_storage_ident).collect();
    let changed_storage_idents: Vec<Ident> = changed_types.iter().map(resolve_storage_ident).collect();
    let remove_storage_idents: Vec<Ident> = remove_types.iter().map(resolve_storage_ident).collect();

    // Deprecated per-type field idents; using unique storages instead

    let _fn_inputs = view_args.iter().map(|va| {
        let vi = &va.ident; let ty = &va.ty;
        if va.is_mut { quote!(#vi: crate::view::ViewMut<#ty>) } else { quote!(#vi: crate::view::View<#ty>) }
    });
    // No function definition needed - removal handled automatically by macro

    // Tuple type: one per unique type with mutability as required
    let unique_tuple_types = unique_types.iter().enumerate().map(|(i, t)| {
        if unique_mut_flags[i] { quote!(&mut crate::storage::BitsetStorage<#t>) } else { quote!(&crate::storage::BitsetStorage<#t>) }
    });
    let _args_tuple_type = quote!( ( #( #unique_tuple_types ),* ) );

    // Destructure into unique storages only
    let _args_destructure = quote!( let ( #( #unique_idents ),* ) = args; );

    // Bind category aliases from unique storages - use reborrowing to avoid conflicts


    let outer_intersections = {
        let view_intersections = view_storage_idents.iter().enumerate().map(|(i, si)| {
            let ty = &view_types[i];
            let ty_key = quote!(#ty).to_string();
            // Check if this type is also in changed_types
            let is_changed = changed_types.iter().any(|ct| quote!(#ct).to_string() == ty_key);
            
            if is_changed {
                // Use presence & changed_mask for types that are in Changed filter
                quote!( outer_mask &= #si.root.presence_mask & #si.root.changed_mask; )
            } else {
                quote!( outer_mask &= #si.root.presence_mask; )
            }
        });
        
        let all_intersections = all_storage_idents.iter().map(|si| {
            quote!( outer_mask &= #si.root.presence_mask; )
        });
        
        quote! {
            #( #view_intersections )*
            #( #all_intersections )*
        }
    };
    let middle_intersections_views = {
        let view_intersections = view_storage_idents.iter().enumerate().map(|(i, si)| {
            let ty = &view_types[i];
            let ty_key = quote!(#ty).to_string();
            let is_changed = changed_types.iter().any(|ct| quote!(#ct).to_string() == ty_key);
            
            if is_changed {
                quote!( middle_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().presence_mask & #si.root.data[oi as usize].assume_init_ref().changed_mask }; )
            } else {
                quote!( middle_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().presence_mask }; )
            }
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
        let view_intersections = view_storage_idents.iter().enumerate().map(|(i, si)| {
            let ty = &view_types[i];
            let ty_key = quote!(#ty).to_string();
            let is_changed = changed_types.iter().any(|ct| quote!(#ct).to_string() == ty_key);
            
            if is_changed {
                quote!( inner_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().data[mi as usize].assume_init_ref().presence_mask & #si.root.data[oi as usize].assume_init_ref().data[mi as usize].assume_init_ref().changed_mask }; )
            } else {
                quote!( inner_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().data[mi as usize].assume_init_ref().presence_mask }; )
            }
        });
        
        let all_intersections = all_storage_idents.iter().map(|si| {
            quote!( inner_mask &= unsafe { #si.root.data[oi as usize].assume_init_ref().data[mi as usize].assume_init_ref().presence_mask }; )
        });
        
        quote! {
            #( #view_intersections )*
            #( #all_intersections )*
        }
    };

    let middle_all = if all_types.is_empty() { quote!() } else {
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

    let middle_none = if none_types.is_empty() { quote!() } else {
        let per_none = none_storage_idents.iter().map(|ni| {
            quote! {
                let rp = #ni.root.presence_mask;
                if ((rp >> oi) & 1) != 0 {
                    let nb = unsafe { #ni.root.data[oi as usize].assume_init_ref() };
                    none_mid |= nb.absence_mask;
                }
            }
        });
        quote! { let mut none_mid: u128 = 0; #(#per_none)* middle_mask &= !none_mid; }
    };

    let middle_any = if any_types.is_empty() { quote!() } else {
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

    let middle_changed = if changed_types.is_empty() { quote!() } else {
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


    let inner_all = if all_types.is_empty() { quote!() } else {
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

    let inner_none = if none_types.is_empty() { quote!() } else {
        let per_none = none_storage_idents.iter().map(|ni| {
            quote! {
                let nb = unsafe { #ni.root.data[oi as usize].assume_init_ref() };
                let mp = nb.presence_mask;
                if ((mp >> mi) & 1) != 0 {
                    let ib = unsafe { nb.data[mi as usize].assume_init_ref() };
                    none_in |= ib.absence_mask;
                }
            }
        });
        quote! { let mut none_in: u128 = 0; #(#per_none)* inner_mask &= !none_in; }
    };

    let inner_any = if any_types.is_empty() { quote!() } else {
        let per_any = any_storage_idents.iter().enumerate().map(|(i, ai)| {
            quote! {
                let rp = #ai.root.presence_mask;
                {
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open("macro_debug.txt") {
                        writeln!(file, "DEBUG: Any[{}] root presence: {:b}, oi: {}", #i, rp, oi).ok();
                    }
                }
                if ((rp >> oi) & 1) != 0 {
                    let ab = unsafe { #ai.root.data[oi as usize].assume_init_ref() };
                    let mp = ab.presence_mask;
                    {
                        use std::io::Write;
                        if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open("macro_debug.txt") {
                            writeln!(file, "DEBUG: Any[{}] middle presence: {:b}, mi: {}", #i, mp, mi).ok();
                        }
                    }
                    if ((mp >> mi) & 1) != 0 {
                        let ib = unsafe { ab.data[mi as usize].assume_init_ref() };
                        any_in |= ib.presence_mask;
                        {
                            use std::io::Write;
                            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open("macro_debug.txt") {
                                writeln!(file, "DEBUG: Any[{}] added mask: {:b}, new any_in: {:b}", #i, ib.presence_mask, any_in).ok();
                            }
                        }
                    }
                }
            }
        });
        quote! { 
            let mut any_in: u128 = 0; 
            #(#per_any)* 
            {
                use std::io::Write;
                if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open("macro_debug.txt") {
                    writeln!(file, "DEBUG: Final any_in: {:b}, inner_mask: {:b}", any_in, inner_mask).ok();
                }
            }
            inner_mask &= any_in; 
        }
    };

    let inner_changed = if changed_types.is_empty() { quote!() } else {
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
                    let #arg_ident = {
                        let root = unsafe { &mut #storage_ident.root };
                        let middle = unsafe { root.data[oi as usize].assume_init_mut() };
                        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };
                        crate::view::ViewMut::new(
                            unsafe { inner.data[ii as usize].assume_init_mut() }
                        )
                    };
                }
            } else {
                quote! {
                    let #arg_ident = {
                        let root = unsafe { &#storage_ident.root };
                        let middle = unsafe { root.data[oi as usize].assume_init_ref() };
                        let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                        crate::view::View::new(
                            unsafe { inner.data[ii as usize].assume_init_ref() }
                        )
                    };
                }
            }
        });
        
        quote! {
            for ii in start..(start + run) {
                #( #view_constructions )*
                #body
            }
        }
    } else {
        quote! { #body }
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
    } else { quote!() };



    // Stage fields: one per unique type
    let struct_fields_unique = unique_types.iter().enumerate().map(|(i, t)| {
        let id = &unique_idents[i];
        quote!( pub #id: std::rc::Rc<std::cell::RefCell<crate::storage::BitsetStorage<#t>>> , )
    });

    // Build run args from unique storages (borrow references)


    // Borrow once per unique type into locals - always borrow mutably if any usage requires it
    let borrow_locals: Vec<proc_macro2::TokenStream> = unique_types.iter().enumerate().map(|(i, _)| {
        let id = &unique_idents[i];
        if unique_mut_flags[i] { 
            quote!( let mut #id = self.#id.borrow_mut(); ) 
        } else { 
            quote!( let #id = self.#id.borrow(); ) 
        }
    }).collect();
    


    let create_fields_unique = unique_types.iter().enumerate().map(|(i, t)| {
        let id = &unique_idents[i]; quote!( #id: world.get::<#t>() )
    });

    let reads_unique = unique_types.iter().enumerate().filter_map(|(i, t)| {
        if unique_mut_flags[i] { None } else { Some(quote!( std::any::TypeId::of::<#t>() )) }
    });
    let writes_unique = unique_types.iter().enumerate().filter_map(|(i, t)| {
        if unique_mut_flags[i] { Some(quote!( std::any::TypeId::of::<#t>() )) } else { None }
    });

    // query_impl defined above with full implementation

    let expanded = quote! {
        pub struct #stage_ident { #( #struct_fields_unique )* }
        impl crate::scheduler::PipelineStage for #stage_ident {
            fn run(&self) {
                #( #borrow_locals )*

                let mut outer_mask: u128 = u128::MAX;
                #outer_intersections
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
            fn create(world: &mut crate::world::World) -> Self {
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
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Component)]
pub fn component_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let cleanup_name = syn::Ident::new(&format!("{}CleanupSystem", name), name.span());

    let gen = quote! {
        // 2️⃣ Implement Component trait
        impl crate::component::Resource for #name {
            fn type_index() -> usize {
                static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

                *TYPE_INDEX.get_or_init(|| crate::component::next_id())
            }
        }

        impl crate::component::Component for #name {

        }

        struct #cleanup_name(crate::system::ComponentCleanupSystem<#name>);

        impl crate::scheduler::PipelineStage for #cleanup_name {

            fn create(world: &mut crate::world::World) -> Self {
                #cleanup_name(crate::system::ComponentCleanupSystem::create(world))
            }

            fn reads(&self) -> &'static [std::any::TypeId] {
                self.0.reads()
            }

            fn writes(&self) -> &'static [std::any::TypeId] {
                static WRITES: &[std::any::TypeId] = &[std::any::TypeId::of::<#name>()];
                WRITES
            }

            fn run(&self){
                self.0.run();
            }
        }
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
