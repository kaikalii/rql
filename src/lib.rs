#![deny(missing_docs)]

/*!
# Description

RQL (Rusty Query Language) is a library design to bring sql-like logic to Rust.
However, bear in mind that there is no traditional database here, just some traits
and adapters applied to iterators and hashmaps
*/

use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use hashbrown::{hash_map, HashMap};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

/// A prelude for commonly used imports
pub mod prelude {
    pub use crate::{database, HasRows, Id, Table};
}

/// An id for indexing rows
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id<T> {
    uuid: Uuid,
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

/// A row in a `Table`
pub struct Row<'a, T> {
    id: Id<T>,
    data: &'a T,
}

impl<'a, T> Row<'a, T> {
    /// Change the data held in a row without changing its id
    pub fn map<F, U>(&self, f: F) -> MappedRow<T, U>
    where
        F: Fn(&T) -> U,
    {
        MappedRow {
            id: self.id,
            data: f(self.data),
        }
    }
}

impl<'a, T> Clone for Row<'a, T> {
    fn clone(&self) -> Self {
        Row {
            id: self.id,
            data: self.data,
        }
    }
}

impl<'a, T> Copy for Row<'a, T> {}

impl<'a, T, U> PartialEq<Row<'a, U>> for Row<'a, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &Row<U>) -> bool {
        self.data.eq(&other.data)
    }
}

impl<'a, T> Eq for Row<'a, T> where T: Eq {}

impl<'a, T, U> PartialEq<RowMut<'a, U>> for Row<'a, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &RowMut<U>) -> bool {
        self.data.eq(other.data)
    }
}

impl<'a, T, U, V> PartialEq<MappedRow<V, U>> for Row<'a, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &MappedRow<V, U>) -> bool {
        self.data.eq(&other.data)
    }
}

impl<'a, T> Deref for Row<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a, T> AsRef<T> for Row<'a, T> {
    fn as_ref(&self) -> &T {
        self.data
    }
}

impl<'a, T> fmt::Debug for Row<'a, T>
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

impl<'a, T> fmt::Display for Row<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <T as fmt::Display>::fmt(&self.data, f)
    }
}

/// A mutable row in a `Table`
pub struct RowMut<'a, T> {
    id: Id<T>,
    data: &'a mut T,
}

impl<'a, T> RowMut<'a, T> {
    /// Change the data held in a row without changing its id
    pub fn map<F, U>(&self, f: F) -> MappedRow<T, U>
    where
        F: Fn(&T) -> U,
    {
        MappedRow {
            id: self.id,
            data: f(self.data),
        }
    }
}

impl<'a, T, U> PartialEq<RowMut<'a, U>> for RowMut<'a, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &RowMut<U>) -> bool {
        self.data.eq(&other.data)
    }
}

impl<'a, T> Eq for RowMut<'a, T> where T: Eq {}

impl<'a, T, U> PartialEq<Row<'a, U>> for RowMut<'a, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &Row<U>) -> bool {
        (self.data as &T).eq(other.data)
    }
}

impl<'a, T, U, V> PartialEq<MappedRow<V, U>> for RowMut<'a, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &MappedRow<V, U>) -> bool {
        (self.data as &T).eq(&other.data)
    }
}

impl<'a, T> Deref for RowMut<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a, T> DerefMut for RowMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

impl<'a, T> AsRef<T> for RowMut<'a, T> {
    fn as_ref(&self) -> &T {
        self.data
    }
}

impl<'a, T> AsMut<T> for RowMut<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        self.data
    }
}

impl<'a, T> fmt::Debug for RowMut<'a, T>
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

impl<'a, T> fmt::Display for RowMut<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <T as fmt::Display>::fmt(&self.data, f)
    }
}

/// A row that was mapped from another and owns its data
pub struct MappedRow<I, T> {
    id: Id<I>,
    data: T,
}

impl<I, T> MappedRow<I, T> {
    /// Change the data held in a row without changing its id
    pub fn map<F, U>(self, f: F) -> MappedRow<I, U>
    where
        F: Fn(T) -> U,
    {
        MappedRow {
            id: self.id,
            data: f(self.data),
        }
    }
}

impl<I, T> Clone for MappedRow<I, T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        MappedRow {
            id: self.id,
            data: self.data.clone(),
        }
    }
}

impl<I, T> Copy for MappedRow<I, T> where T: Copy {}

impl<I, T, U> PartialEq<MappedRow<I, U>> for MappedRow<I, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &MappedRow<I, U>) -> bool {
        self.data.eq(&other.data)
    }
}

impl<'a, T, U, V> PartialEq<Row<'a, U>> for MappedRow<V, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &Row<U>) -> bool {
        self.data.eq(other.data)
    }
}

impl<'a, T, U, V> PartialEq<RowMut<'a, U>> for MappedRow<V, T>
where
    T: PartialEq<U>,
{
    fn eq(&self, other: &RowMut<U>) -> bool {
        self.data.eq(other.data)
    }
}

impl<I, T> Deref for MappedRow<I, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<I, T> AsRef<T> for MappedRow<I, T> {
    fn as_ref(&self) -> &T {
        &self.data
    }
}

impl<I, T> AsMut<T> for MappedRow<I, T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<I, T> DerefMut for MappedRow<I, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<I, T> fmt::Debug for MappedRow<I, T>
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

impl<I, T> fmt::Display for MappedRow<I, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <T as fmt::Display>::fmt(&self.data, f)
    }
}

/// An iterator over rows in a `Table`
#[derive(Debug)]
pub struct RowIter<'a, T> {
    inner: hash_map::Iter<'a, Id<T>, T>,
}

impl<'a, T> Iterator for RowIter<'a, T> {
    type Item = Row<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(id, data)| Row { id: *id, data })
    }
}

impl<'a, T> Clone for RowIter<'a, T> {
    fn clone(&self) -> Self {
        RowIter {
            inner: self.inner.clone(),
        }
    }
}

/// An mutable iterator over rows in a `Table`
#[derive(Debug)]
pub struct RowIterMut<'a, T> {
    inner: hash_map::IterMut<'a, Id<T>, T>,
}

impl<'a, T> Iterator for RowIterMut<'a, T> {
    type Item = RowMut<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(id, data)| RowMut { id: *id, data })
    }
}

/**
A table abstraction akin to a table in a real database
*/
#[derive(Serialize, Deserialize)]
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
    /// Insert a row into the `Table`
    pub fn insert(&mut self, row: T) {
        self.map.insert(Id::new(), row);
    }
    /// Try to get a reference to the row with the given `Id`
    pub fn get(&self, id: Id<T>) -> Option<&T> {
        self.map.get(&id)
    }
    /// Try to get a mutable reference to the row with the given `Id`
    pub fn get_mut(&mut self, id: Id<T>) -> Option<&mut T> {
        self.map.get_mut(&id)
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
A trait for things that have rows
*/
pub trait HasRows<'a>: Sized {
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
        R: HasRows<'a>,
        R::Iter: Clone,
        <Self::Iter as Iterator>::Item: Clone,
        F: Fn(&<Self::Iter as Iterator>::Item, &<R::Iter as Iterator>::Item) -> bool,
    {
        Relate::new(self.rows(), other.rows(), f)
    }
}

impl<'a, I> HasRows<'a> for I
where
    I: Iterator,
{
    type Iter = I;
    fn rows(self) -> Self::Iter {
        self
    }
}

impl<'a, T> HasRows<'a> for &'a Table<T> {
    type Iter = RowIter<'a, T>;
    fn rows(self) -> Self::Iter {
        Table::rows(self)
    }
}

impl<'a, T> HasRows<'a> for &'a mut Table<T> {
    type Iter = RowIter<'a, T>;
    fn rows(self) -> Self::Iter {
        Table::rows(self)
    }
}

/**
A trait for things that have mutable rows
*/
pub trait HasRowsMut<'a>: Sized {
    /// The row iterator type
    type Iter: Iterator;
    /// Get the row iterator
    fn rows_mut(self) -> Self::Iter;
    /// Update the rows
    fn update(self) -> Self::Iter {
        self.rows_mut()
    }
}

impl<'a, I> HasRowsMut<'a> for I
where
    I: Iterator,
{
    type Iter = I;
    fn rows_mut(self) -> Self::Iter {
        self
    }
}

impl<'a, T> HasRowsMut<'a> for &'a mut Table<T> {
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
Macro for generating a database `struct`

`database!` generates the structure itself as well as necessary
function impls.

# Example
```
use rql::prelude::*;

struct Person {
    name: String,
    age: usize,
}

struct Account {
    owner: Id<Person>,
    balance: f32,
}

// The database type generated by

database! {
    DatabaseA {
        person: Person,
        account: Account,
    }
}

// is structurally equivalent to

struct DatabaseB {
    pub person: Table<Person>,
    pub account: Table<Account>,
}
```
*/
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
    fn compiles() {
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
            .relate(&db.strings, |i, s| s.len() == **i)
            .select(|(i, s)| format!("{}: {}", s, i))
        {
            println!("{:?}", s);
        }
        for s in db
            .strings
            .select(|row| row.map(|s| s.len()))
            .relate(&db.nums, |a, b| a == b)
        {
            println!("{:?}", s);
        }
        for mut s in db.strings.update() {
            *s = "wow".into()
        }
    }
}
