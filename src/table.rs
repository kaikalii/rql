/// Trait that is implemented for column types
pub trait Column {
    /// The type of the column data
    type Type;
}

/// Macro for defining table schema
#[macro_export]
macro_rules! table {
    (
        $(#[$attr:meta])*
        $name:ident $(<$($lt:lifetime),*>)* {
            $(
                $(#[$field_attr:meta])*
                $field:ident: $type:ty
            ),* $(,)*
        }
    ) => {
        // Compile error if a lifetime bound is used
        $($(
            $lt: loop{};
            compile_error!("Table structs cannot require lifetime bounds");
        )*)*

        // Define struct
        use serde_derive::{Serialize, Deserialize};
        #[derive(Debug, Serialize, Deserialize)]
        $(#[$attr])*
        pub struct $name {
            $(
                $(#[$field_attr])*
                $field: $type
            ),*
        }

        // Define column enum
        use rql::macro_prelude::{mashup, mashup_parser, mashup_macro, mashup_macro_impl};
        mashup! {
            m[$name "Column"] = $name Column;
        }
        m! {
            pub mod $name "Column" {
                use serde_derive::{Serialize, Deserialize};
                use super::*;
                $(
                    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
                    #[allow(non_camel_case_types)]
                    pub struct $field;
                    impl rql::Column for $field {
                        type Type = $type;
                    }
                )*
            }
        }
    };
}

// #[cfg(test)]
mod test {
    mod rql {
        pub use crate::*;
    }
    use super::*;
    // #[test]
    fn compiles() {
        table! {
            User {
                name: String,
                username: String,
            }
        }

        let user = User {
            name: "Dan".into(),
            username: "dantheman".into(),
        };
    }
}
