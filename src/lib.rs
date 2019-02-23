#![deny(missing_docs)]

/*!
# Description

RQL (Rusty Query Language) is a library design to bring sql-like logic to Rust.
However, bear in mind that there is no traditional database here, just some traits
and adapters applied to iterators and hashmaps
*/

use std::{fmt, ops::Deref};

use hashbrown::{hash_map, HashMap};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

/// A prelude for commonly used imports
pub mod prelude {
    pub use crate::{Id, Relatable, Selectable, Table};
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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row<T> {
    id: Id,
    data: T,
}

impl<'a, T> Deref for Row<&'a T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<T> fmt::Debug for Row<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.alternate() {
            write!(f, "{}: {:#?}", self.id, self.data)
        } else {
            write!(f, "{}: {:?}", self.id, self.data)
        }
    }
}

impl<T> fmt::Display for Row<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <T as fmt::Display>::fmt(&self.data, f)
    }
}

/// An iterator over rows in a `Table`
#[derive(Debug, Clone)]
pub struct RowIter<'a, T> {
    inner: hash_map::Iter<'a, Id, T>,
}

impl<'a, T> Iterator for RowIter<'a, T> {
    type Item = Row<&'a T>;
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
    /// Insert a row into the `Table`
    pub fn insert(&mut self, row: T) {
        self.map.insert(Id::new(), row);
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
A trait for things that have rows
*/
pub trait HasRows<'a> {
    /// The row iterator type
    type Iter: Iterator;
    /// Get the row iterator
    fn rows(self) -> Self::Iter;
}

impl<'a, T> HasRows<'a> for &'a Table<T>
where
    T: 'a,
{
    type Iter = RowIter<'a, T>;
    fn rows(self) -> Self::Iter {
        self.rows()
    }
}

/**
An adaptor for selecting data from a `Table`
*/
pub struct Select<I, F> {
    iter: I,
    selector: F,
}

impl<I, F, R> Select<I, F>
where
    I: Iterator,
    F: Fn(I::Item) -> R,
{
}

impl<I, F, R> Iterator for Select<I, F>
where
    I: Iterator,
    F: Fn(I::Item) -> R,
{
    type Item = R;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(&self.selector)
    }
}

/**
A trait for things that can be selected from
*/
pub trait Selectable<'a>: HasRows<'a> + Sized {
    /// Create the `Select`
    fn select<F, R>(self, f: F) -> Select<Self::Iter, F>
    where
        F: Fn(<<Self as HasRows<'a>>::Iter as Iterator>::Item) -> R,
    {
        Select {
            iter: self.rows(),
            selector: f,
        }
    }
}

impl<'a, R> Selectable<'a> for R where R: HasRows<'a> {}

/**
A relationship between two tables
*/
pub struct Relate<A, B, F>
where
    A: Iterator,
    B: Iterator + Clone,
    A::Item: Copy,
    B::Item: Copy,
{
    iter_a: A,
    iter_b: B,
    curr_a: Option<A::Item>,
    curr_b: B,
    relation: F,
}

impl<A, B, F> Relate<A, B, F>
where
    A: Iterator,
    B: Iterator + Clone,
    A::Item: Copy,
    B::Item: Copy,
{
    fn new(mut iter_a: A, iter_b: B, f: F) -> Self {
        let curr_a = iter_a.next();
        Relate {
            iter_a,
            iter_b: iter_b.clone(),
            curr_a,
            curr_b: iter_b,
            relation: f,
        }
    }
}

impl<A, B, F> Iterator for Relate<A, B, F>
where
    A: Iterator,
    B: Iterator + Clone,
    A::Item: Copy,
    B::Item: Copy,
    F: Fn(A::Item, B::Item) -> bool,
{
    type Item = (A::Item, B::Item);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref a) = self.curr_a {
                if let Some(b) = self.curr_b.next() {
                    if (self.relation)(*a, b) {
                        return Some((*a, b));
                    }
                } else if let Some(next_a) = self.iter_a.next() {
                    self.curr_a = Some(next_a);
                    self.curr_b = self.iter_b.clone();
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a, A, B, F> HasRows<'a> for Relate<A, B, F>
where
    A: Iterator + 'a,
    B: Iterator + Clone + 'a,
    A::Item: Copy,
    B::Item: Copy,
    F: Fn(A::Item, B::Item) -> bool + 'a,
{
    type Iter = Relate<A, B, F>;
    fn rows(self) -> Self::Iter {
        self
    }
}

/**
A trait for things that can be related
*/
pub trait Relatable<'a>: HasRows<'a> + Sized {
    /// Relate this `Relatable` to another
    fn relate<R, F>(self, other: R, f: F) -> Relate<Self::Iter, R::Iter, F>
    where
        R: Relatable<'a>,
        R::Iter: Clone,
        <<R as HasRows<'a>>::Iter as Iterator>::Item: Copy,
        <<Self as HasRows<'a>>::Iter as Iterator>::Item: Copy,
        F: Fn(
            <<Self as HasRows<'a>>::Iter as Iterator>::Item,
            <<R as HasRows<'a>>::Iter as Iterator>::Item,
        ) -> bool,
    {
        Relate::new(self.rows(), other.rows(), f)
    }
}

impl<'a, R> Relatable<'a> for R where R: HasRows<'a> {}

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
#[cfg(test)]
mod tests {
    /// Reexport everything from `rql` so macros work
    /// as if this is a foreign library
    mod rql {
        pub use super::super::*;
    }
    use super::*;
    #[test]
    fn _it_works() {
        database! {
            Db {
                nums: usize,
                strings: String,
            }
        }
        let mut db = Db::new();
        db.nums.insert(4);
        db.nums.insert(2);
        db.nums.insert(5);
        db.strings.insert("hi".into());
        db.strings.insert("hello".into());
        db.strings.insert("world".into());
        for s in db
            .nums
            .relate(&db.strings, |i, s| s.len() == *i)
            .select(|(i, s)| format!("{}: {}", s, i))
        {
            println!("{}", s);
        }
    }
}
