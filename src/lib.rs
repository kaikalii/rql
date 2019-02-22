#![deny(missing_docs)]

/*!
*/

use std::{fmt, ops::Deref};

use hashbrown::{hash_map, HashMap};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

/// A prelude for commonly used imports
pub mod prelude {
    pub use crate::Id;
    pub use crate::Selectable;
    pub use crate::Table;
}

/// An id for indexing rows
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Id(Uuid);

impl Id {
    /// Create a new `Id`
    pub fn new() -> Self {
        Id(Uuid::new_v4())
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_simple())
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_simple())
    }
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

/// A row in a `Table`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row<'a, T> {
    id: Id,
    data: &'a T,
}

impl<'a, T> Deref for Row<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

/// An iterator over rows in a `Table`
pub struct RowIter<'a, T> {
    inner: hash_map::Iter<'a, Id, T>,
}

impl<'a, T> Iterator for RowIter<'a, T> {
    type Item = Row<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(id, data)| Row { id: *id, data })
    }
}

/**
A table abstraction akin to a table in a real database
*/
#[derive(Default, Serialize, Deserialize)]
pub struct Table<T> {
    map: HashMap<Id, T>,
}

impl<T> Table<T> {
    /// Create a new `Table`
    pub fn new() -> Self {
        Table {
            map: HashMap::new(),
        }
    }
    /// Iterate over all rows in the `Table`
    pub fn rows(&self) -> RowIter<T> {
        RowIter {
            inner: self.map.iter(),
        }
    }
}

impl<T> fmt::Debug for Table<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list().entries(self.map.iter()).finish()
    }
}

/**
Things that data can be selected from
*/
pub trait Selectable<F> {
    /// The type of the `Select`
    type Select;
    /// The type of the `Data`
    type Data;
    /// Get the `Select`
    fn select(&self, f: F) -> Self::Select;
}

impl<'a, T, F, R> Selectable<F> for &'a Table<T>
where
    F: Fn(Row<T>) -> R,
{
    type Select = Select<'a, T, RowIter<'a, T>, F, R>;
    type Data = R;
    fn select(&self, f: F) -> Self::Select {
        Select {
            iter: self.rows(),
            selector: f,
        }
    }
}

/**
A `SELECT` statment
*/
pub struct Select<'a, T, I, F, R>
where
    T: 'a,
    I: Iterator<Item = Row<'a, T>>,
    F: Fn(Row<T>) -> R,
{
    iter: I,
    selector: F,
}

impl<'a, T, I, F, R> Select<'a, T, I, F, R>
where
    I: Iterator<Item = Row<'a, T>>,
    F: Fn(Row<T>) -> R,
{
}

impl<'a, T, I, F, R> Iterator for Select<'a, T, I, F, R>
where
    I: Iterator<Item = Row<'a, T>>,
    F: Fn(Row<T>) -> R,
{
    type Item = R;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|row| (self.selector)(row))
    }
}

#[macro_export]
macro_rules! database {
    ($name:ident { $($table:ident: $type:ty),* $(,)? }) => {
        #[derive(Default)]
        struct $name {
            $(pub $table: rql::Table<$type>),*
        }
        database!(impl $name);
    };
    (pub $name:ident { $($table:ident: $type:ty),* $(,)? }) => {
        #[derive(Default)]
        pub struct $name {
            $(pub $table: rql::Table<$type>),*
        }
        database!(impl $name);
    };
    (impl $name:ident) => {
        impl $name {
            pub fn new() -> Self {
                Default::default()
            }
        }
    }
}

/// Test module
// #[cfg(test)]
mod tests {
    /// Reexport everything from `rql` so macros work
    /// as if this is a foreign library
    mod rql {
        pub use super::super::*;
    }
    use super::*;
    // #[test]
    fn it_works() {
        database! {
            Db {
                nums: i64,
                strings: String,
            }
        }
        let db = Db::new();
        let select = db.nums.select(|x| 1);
    }
}
