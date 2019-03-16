use serde_derive::{Deserialize, Serialize};

/**
A `WHERE` clause
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Where<T> {
    /// Check if a column is equal to a value
    Eq(T),
    /// Check if a column is not equal to a value
    Neq(T),
}
