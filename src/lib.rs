#![deny(missing_docs)]

/*!
# Description

RQL (Rusty Query Language) is a library design to bring sql-like logic to Rust.
However, bear in mind that there is no traditional database here, just some traits
and adapters applied to iterators and hashmaps
*/

mod row;
pub use row::*;

use std::{
    fmt, fs,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
};

use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

/// A prelude for commonly used imports
pub mod prelude {
    pub use crate::{schema, Database, HasRows, HasRowsMut, Id, Row, RowMut, Table};
    pub use serde_derive::{Deserialize, Serialize};
}

/// An error type for `rql`
#[derive(Debug)]
pub enum Error {
    /// An io error
    Io(std::io::Error),
    /// A serial encoding error
    Encode(rmp_serde::encode::Error),
    /// A serial decoding error
    Decode(rmp_serde::decode::Error),
}

macro_rules! error_from {
    ($variant:ident: $type:ty) => {
        impl From<$type> for Error {
            fn from(e: $type) -> Self {
                Error::$variant(e)
            }
        }
    };
}

error_from!(Io: std::io::Error);
error_from!(Encode: rmp_serde::encode::Error);
error_from!(Decode: rmp_serde::decode::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            Io(e) => write!(f, "{}", e),
            Encode(e) => write!(f, "{}", e),
            Decode(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

/// A result type for `rql`
pub type Result<T> = std::result::Result<T, Error>;

/// An id for indexing rows
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id<T> {
    uuid: Uuid,
    #[serde(skip)]
    pd: PhantomData<T>,
}

impl<T> Id<T> {
    /// Create a new `Id`
    pub fn new() -> Self {
        Id {
            uuid: Uuid::new_v4(),
            pd: PhantomData,
        }
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        Id {
            uuid: self.uuid,
            pd: PhantomData,
        }
    }
}

impl<T> Copy for Id<T> {}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.uuid.eq(&other.uuid)
    }
}

impl<T> Eq for Id<T> {}

impl<T> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.uuid.to_simple())
    }
}

impl<T> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.uuid.to_simple())
    }
}

impl<T> Default for Id<T> {
    fn default() -> Self {
        Self::new()
    }
}

/**
A table abstraction akin to a table in a real schema
*/
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Table<T> {
    map: HashMap<Id<T>, T>,
}

impl<T> Table<T> {
    /// Create a new `Table`
    pub fn new() -> Self {
        Table {
            map: HashMap::new(),
        }
    }
    /// Get the number of rows in the `Table`
    pub fn len(&self) -> usize {
        self.map.len()
    }
    /// Check if the Table` is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    /// Insert a row into the `Table`
    pub fn insert(&mut self, row: T) -> Id<T> {
        let id = Id::new();
        self.map.insert(id, row);
        id
    }
    /// Try to get a reference to the row with the given `Id`
    pub fn get(&self, id: Id<T>) -> Option<&T> {
        self.map.get(&id)
    }
    /// Try to get a mutable reference to the row with the given `Id`
    pub fn get_mut(&mut self, id: Id<T>) -> Option<&mut T> {
        self.map.get_mut(&id)
    }
    /// Remove the row with the given `Id`
    pub fn remove(&mut self, id: Id<T>) -> Option<T> {
        self.map.remove(&id)
    }
    /// Iterate over all rows in the `Table`
    pub fn rows(&self) -> RowIter<T> {
        RowIter {
            inner: self.map.iter(),
        }
    }
    /// Iterate mutably over all rows in the `Table`
    pub fn rows_mut(&mut self) -> RowIterMut<T> {
        RowIterMut {
            inner: self.map.iter_mut(),
        }
    }
    /// Delete a single row based on its `Id`
    pub fn delete_one(&mut self, id: Id<T>) {
        self.remove(id);
    }
    /// Delete rows that satisfy the clause
    pub fn delete_where<F>(&mut self, f: F)
    where
        F: Fn(&T) -> bool,
    {
        self.map.retain(|_, data| !f(data))
    }
    /// Delete rows with ids returned by an iterator
    pub fn delete_iter<'a, F, I, R>(&'a mut self, f: F)
    where
        F: Fn(&'a Self) -> I,
        I: Iterator<Item = R>,
        R: Idd<RowType = T>,
    {
        let ids: Vec<Id<T>> = f(unsafe { (self as *mut Self).as_mut() }.unwrap())
            .map(|row| row.id())
            .collect();
        for id in ids {
            self.remove(id);
        }
    }
}

impl<T> Default for Table<T> {
    fn default() -> Self {
        Table {
            map: Default::default(),
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
A trait for accessing rows
*/
pub trait HasRows: Sized {
    /// The row iterator type
    type Iter: Iterator;
    /// Get the row iterator
    fn rows(self) -> Self::Iter;
    /// Create a selection
    fn select<F, R>(self, f: F) -> Select<Self::Iter, F>
    where
        F: Fn(<Self::Iter as Iterator>::Item) -> R,
    {
        Select {
            iter: self.rows(),
            selector: f,
        }
    }
    /// Relate this `HasRows` to another
    fn relate<R, F>(self, other: R, f: F) -> Relate<Self::Iter, R::Iter, F>
    where
        R: HasRows,
        R::Iter: Clone,
        <Self::Iter as Iterator>::Item: Clone,
        F: Fn(&<Self::Iter as Iterator>::Item, &<R::Iter as Iterator>::Item) -> bool,
    {
        Relate::new(self.rows(), other.rows(), f)
    }
    /// Restricts the rows with a `WHERE` clause
    fn wher<F>(self, f: F) -> Where<Self::Iter, F>
    where
        F: Fn(&<Self::Iter as Iterator>::Item) -> bool,
    {
        Where {
            iter: self.rows(),
            clause: f,
        }
    }
    /// Tries to find a row that satisfies the clause
    fn find<F>(self, f: F) -> Option<<Self::Iter as Iterator>::Item>
    where
        F: Fn(&<Self::Iter as Iterator>::Item) -> bool,
    {
        self.wher(f).next()
    }
}

impl<I> HasRows for I
where
    I: Iterator,
{
    type Iter = I;
    fn rows(self) -> Self::Iter {
        self
    }
}

impl<'a, T> HasRows for &'a Table<T> {
    type Iter = RowIter<'a, T>;
    fn rows(self) -> Self::Iter {
        Table::rows(self)
    }
}

impl<'a, T> HasRows for &'a mut Table<T> {
    type Iter = RowIter<'a, T>;
    fn rows(self) -> Self::Iter {
        Table::rows(self)
    }
}

/**
A trait for mutably accessing rows
*/
pub trait HasRowsMut: Sized {
    /// The row iterator type
    type Iter: Iterator;
    /// Get the row iterator
    fn rows_mut(self) -> Self::Iter;
    /// Update the rows
    fn update(self) -> Self::Iter {
        self.rows_mut()
    }
}

impl<I> HasRowsMut for I
where
    I: Iterator,
{
    type Iter = I;
    fn rows_mut(self) -> Self::Iter {
        self
    }
}

impl<'a, T> HasRowsMut for &'a mut Table<T> {
    type Iter = RowIterMut<'a, T>;
    fn rows_mut(self) -> Self::Iter {
        Table::rows_mut(self)
    }
}

/**
An adaptor for selecting data
*/
pub struct Select<I, F> {
    iter: I,
    selector: F,
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
A relationship between two things
*/
pub struct Relate<A, B, F>
where
    A: Iterator,
    B: Iterator + Clone,
    A::Item: Clone,
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
    A::Item: Clone,
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
    A::Item: Clone,
    F: Fn(&A::Item, &B::Item) -> bool,
{
    type Item = (A::Item, B::Item);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref a) = self.curr_a {
                if let Some(b) = self.curr_b.next() {
                    if (self.relation)(a, &b) {
                        return Some((a.clone(), b));
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

/**
A clause for searching through rows and limiting
the returned values, similar to a sql `WHERE`
*/
pub struct Where<I, F> {
    iter: I,
    clause: F,
}

impl<I, F> Iterator for Where<I, F>
where
    I: Iterator,
    F: Fn(&I::Item) -> bool,
{
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(next) = self.iter.next() {
                if (self.clause)(&next) {
                    return Some(next);
                }
            } else {
                return None;
            }
        }
    }
}

/**
An in-memory pseudo database

The type parameter `S` should be your schema type
*/
#[derive(Default, Serialize, Deserialize)]
pub struct Database<S> {
    tables: S,
}

impl<S> Database<S>
where
    S: Default,
{
    /// Create a new database
    pub fn new() -> Self {
        Database {
            tables: Default::default(),
        }
    }
    /// Get a reference to the `Database`'s tables
    pub fn tables(&self) -> &S {
        &self.tables
    }
    /// Get a mutable reference to the `Database`'s tables
    pub fn tables_mut(&mut self) -> &mut S {
        &mut self.tables
    }
}

impl<S> Database<S>
where
    S: Serialize,
{
    /// Save the database to a file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs::write(path, self.save_to_bytes()?)?;
        Ok(())
    }
    /// Save the database to a byte vector
    pub fn save_to_bytes(&self) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(self)?)
    }
}

impl<S> Database<S>
where
    S: DeserializeOwned,
{
    /// Load a database from a file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        Database::load_from_bytes(fs::read(path)?)
    }
    /// Load a database from a byte array
    pub fn load_from_bytes<B: AsRef<[u8]>>(bytes: B) -> Result<Self> {
        Ok(rmp_serde::from_slice(bytes.as_ref())?)
    }
}

impl<S> Deref for Database<S> {
    type Target = S;
    fn deref(&self) -> &Self::Target {
        &self.tables
    }
}

impl<S> DerefMut for Database<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tables
    }
}

/**
Macro for generating a schema `struct`

`schema!` generates the structure itself as well as necessary
function impls.

# Example
```
use rql::prelude::*;

#[derive(Serialize, Deserialize)]
struct Person {
    name: String,
    age: usize,
}

#[derive(Serialize, Deserialize)]
struct Account {
    owner: Id<Person>,
    balance: f32,
}

// The schema type generated by

schema! {
    SchemaA {
        person: Person,
        account: Account,
    }
}

// is structurally equivalent to

struct SchemaB {
    pub person: Table<Person>,
    pub account: Table<Account>,
}
```
*/
#[macro_export]
macro_rules! schema {
    ($name:ident { $($table:ident: $type:ty),* $(,)? }) => {
        #[derive(Default, rql::prelude::Serialize, rql::prelude::Deserialize)]
        #[serde(default)]
        struct $name {
            $(pub $table: rql::Table<$type>),*
        }
        schema!(impl $name);
    };
    (pub $name:ident { $($table:ident: $type:ty),* $(,)? }) => {
        #[derive(Default, rql::prelude::Serialize, rql::prelude::Deserialize)]
        #[serde(default)]
        pub struct $name {
            $(pub $table: rql::Table<$type>),*
        }
        schema!(impl $name);
    };
    (impl $name:ident) => {}
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
    fn compiles() -> Result<()> {
        schema! {
            Schema {
                nums: usize,
                strings: String,
            }
        }
        let mut db: Database<Schema> = Database::new();
        db.nums.insert(4);
        db.nums.insert(2);
        db.nums.insert(5);
        db.strings.insert("hi".into());
        db.strings.insert("hello".into());
        db.strings.insert("world".into());
        for s in db
            .nums
            .relate(&db.strings, |i, s| s.len() == **i)
            .select(|(i, s)| format!("{}: {}", s, i))
        {
            println!("{:?}", s);
        }
        for s in db.strings.relate(&db.nums, |s, n| s.len() == **n) {
            println!("{:?}", s);
        }
        db.strings.delete_where(|s| s.contains('h'));
        assert_eq!(1, db.strings.len());
        Database::<Schema>::load_from_bytes(db.save_to_bytes()?)?;
        Ok(())
    }
}
