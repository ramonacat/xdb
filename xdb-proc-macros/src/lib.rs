use std::collections::HashMap;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::Parse;
use syn::{
    Block, Expr, FnArg, Ident, LitInt, ReturnType, Token, Visibility, braced, parenthesized,
    parse_macro_input,
};

struct Item {
    name: Ident,
    size: u32,
}

impl Parse for Item {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![:]>()?;
        let size = input.parse::<LitInt>()?.base10_parse()?;

        Ok(Self { name, size })
    }
}

struct Declaration {
    visibility: Visibility,
    name: Ident,
    items: Vec<Item>,
}

impl Parse for Declaration {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let visibility = input.parse()?;
        let name = input.parse()?;
        let items;
        braced!(items in input);

        let items = items.parse_terminated(Item::parse, Token![,])?;

        Ok(Declaration {
            visibility,
            name,
            items: items.into_iter().collect(),
        })
    }
}

struct AccessorBody {
    action: Block,
    ok: Option<Block>,
    err: Option<Block>,
}

impl Parse for AccessorBody {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        input.parse::<keywords::action>()?;
        input.parse::<Token![:]>()?;
        let action = input.parse()?;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
        let ok = if input.peek(keywords::ok) {
            input.parse::<keywords::ok>()?;
            input.parse::<Token![:]>()?;

            let ok = input.parse()?;

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }

            Some(ok)
        } else {
            None
        };

        let err = if input.peek(keywords::err) {
            input.parse::<keywords::err>()?;
            input.parse::<Token![:]>()?;

            let err = input.parse()?;

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }

            Some(err)
        } else {
            None
        };

        Ok(Self { action, ok, err })
    }
}

enum Accessor {
    Update {
        visibility: Visibility,
        name: Ident,
        arguments: Vec<FnArg>,
        output: ReturnType,
        body: AccessorBody,
        set_ordering: Expr,
        fetch_ordering: Box<Expr>,
    },
    Query {
        visibility: Visibility,
        name: Ident,
        ordering: Expr,
    },
}

impl Parse for Accessor {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let visibility = input.parse()?;
        if input.peek(keywords::update) {
            input.parse::<keywords::update>()?;

            let orderings;
            parenthesized!(orderings in input);
            let set_ordering = orderings.parse::<Expr>()?;
            orderings.parse::<Token![,]>()?;
            let fetch_ordering = orderings.parse::<Expr>()?;

            let name = input.parse()?;
            let arguments;
            parenthesized!(arguments in input);
            let arguments = arguments.parse_terminated(FnArg::parse, Token![,])?;
            let arguments = arguments.into_iter().collect();

            let output = input.parse()?;

            let body;
            braced!(body in input);
            let body = body.parse()?;

            Ok(Self::Update {
                visibility,
                set_ordering,
                fetch_ordering: Box::new(fetch_ordering),
                name,
                arguments,
                output,
                body,
            })
        } else {
            input.parse::<keywords::query>()?;

            let name = input.parse()?;
            let ordering;
            parenthesized!(ordering in input);
            let ordering = ordering.parse()?;

            input.parse::<Token![;]>()?;

            Ok(Self::Query {
                visibility,
                name,
                ordering,
            })
        }
    }
}

struct AtomicState {
    declaration: Declaration,
    accessors: Vec<Accessor>,
}

impl Parse for AtomicState {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let declaration = input.parse()?;

        let mut accessors = vec![];

        while !input.is_empty() {
            accessors.push(input.parse()?);
        }

        Ok(AtomicState {
            declaration,
            accessors,
        })
    }
}

#[proc_macro]
pub fn atomic_state(input: TokenStream) -> TokenStream {
    let AtomicState {
        declaration,
        accessors,
    } = parse_macro_input!(input as AtomicState);

    let name = declaration.name;
    let value_name = format_ident!("{}Value", &name);

    let sizes = declaration
        .items
        .iter()
        .map(|x| (x.name.clone(), x.size))
        .collect::<HashMap<_, _>>();

    let internal_methods = generate_internal_methods(declaration.items);
    let visibility = declaration.visibility;

    let methods = generate_accessors(value_name.clone(), sizes, accessors);

    quote! {
        #[derive(Debug)] // TODO a nicer debug representation
        #[repr(transparent)]
        #visibility struct #name(crate::platform::futex::Futex, ::std::marker::PhantomPinned);

        impl #name {
            pub const fn new() -> Self {
                Self(crate::platform::futex::Futex::new(0), ::std::marker::PhantomPinned)
            }

            const fn futex(self: ::std::pin::Pin<&Self>) -> ::std::pin::Pin<&crate::platform::futex::Futex> {
                unsafe { ::std::pin::Pin::new_unchecked(&self.get_ref().0) }
            }

            fn wait(self: ::std::pin::Pin<&Self>, previous: #value_name) {
                self.futex().wait(previous.0, None);
            }

            fn wait_timeout(self: ::std::pin::Pin<&Self>, previous: #value_name, timeout: ::std::time::Duration) {
                self.futex().wait(previous.0, Some(timeout));
            }

            // TODO we should probably have some way of choosing who to wake (waiters for write, waiters
            // for read, etc.)
            fn wake_all(self: ::std::pin::Pin<&Self>) {
                self.futex().wake_all();
            }

            #(#methods)*
        }

        // TODO add an implementation of debug which shows all the fields
        #[derive(Debug, Clone, Copy)]
        #visibility struct #value_name(u32);

        impl #value_name {
            #(#internal_methods)*
        }
    }
    .into()
}

fn generate_accessors(
    value_name: Ident,
    sizes: HashMap<Ident, u32>,
    accessors: Vec<Accessor>,
) -> Vec<proc_macro2::TokenStream> {
    let mut result = vec![];

    for accessor in accessors {
        match accessor {
            Accessor::Update {
                visibility,
                name,
                arguments,
                output,
                body,
                set_ordering,
                fetch_ordering,
            } => {
                let AccessorBody { action, ok, err } = body;

                let err = err.clone().map_or_else(
                    || quote! { unreachable!(); },
                    |x| {
                        quote! { #x }
                    },
                );

                result.push(quote! {
            #[allow(unused_braces)]
            #visibility fn #name(self: ::std::pin::Pin<&Self>, #(#arguments),*) #output {
                match self.futex().atomic().fetch_update(#set_ordering, #fetch_ordering, |state| {
                    let state = #value_name(state);

                    let result = #action;

                    result.map(|x| x.0)
                }) {
                    Ok(state) => {
                        let state = #value_name(state);

                        #ok
                    },
                    Err(state) => {
                        let state = #value_name(state);

                        #err
                    }
                }
            }
                    });
            }
            Accessor::Query {
                visibility,
                name,
                ordering,
            } => {
                let is_bit = *sizes.get(&name).unwrap() == 1;
                let output = if is_bit {
                    quote! { bool }
                } else {
                    quote! { u32 }
                };

                result.push(quote! {
                    #visibility fn #name(self: ::std::pin::Pin<&Self>) -> #output {
                        let value = self.futex().atomic().load(#ordering);
                        let value = #value_name(value);

                        value.#name()
                    }
                });
            }
        }
    }

    result
}

fn generate_internal_methods(items: Vec<Item>) -> Vec<proc_macro2::TokenStream> {
    let mut result = vec![];

    let mut offset = 31u32;

    for item in items {
        let item_size = item.size;
        assert!(
            offset >= item_size,
            "atomic_state cannot use more than 32 bits"
        );

        let setter_name = format_ident!("with_{}", &item.name);
        let getter_name = item.name;

        if item.size == 1 {
            let bitmask = quote! {(1 << #offset)};

            result.push(quote! {
                pub fn #setter_name(self, value: bool) -> Self {
                    if value {
                        Self(self.0 | #bitmask)
                    } else {
                        Self(self.0 & !#bitmask)
                    }
                }

                pub fn #getter_name(self) -> bool {
                    (self.0 & #bitmask) > 0
                }
            });
        } else {
            let mut bitmask = 0u32;

            for i in 0..item_size {
                bitmask |= 1 << (offset - i);
            }

            let shift = (offset - item_size) + 1;

            result.push(quote! {
                pub fn #setter_name(self, value: u32) -> Self {
                    Self((self.0 & !#bitmask) | (value << #shift))
                }

                pub fn #getter_name(self) -> u32 {
                    (self.0 & #bitmask) >> #shift
                }
            });
        }

        offset -= item_size;
    }

    result
}

mod keywords {
    use syn::custom_keyword;

    custom_keyword!(action);
    custom_keyword!(err);
    custom_keyword!(ok);
    custom_keyword!(query);
    custom_keyword!(update);
}
