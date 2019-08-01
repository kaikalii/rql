#![deny(missing_docs)]

/*!
# Description

RQL (Rusty Query Language) is a library design to bring sql-like logic to Rust.
However, bear in mind that there is no traditional database here, just some traits
and adapters applied to iterators and hashmaps.

### *Important Note*

**`rql` does not provide a real database.** All data is stored in memory. This
pseudo-database can be saved to and loaded from the disk via serialization and
deserialization from `serde`. A number of serialization protocols are supported
so that you may choose one to suit your speed, size, and backward-compatibility
needs.

# Schema

To use an `rql` database, you must first define some schema. Every table is defined
by a struct representing a single entry. In this example, we will define three tables:
User, Group, and Member.

```
use rql::prelude::*;

#[derive(Serialize, Deserialize)]
struct User {
    name: String,
    age: u8,
}

#[derive(Serialize, Deserialize)]
struct Group {
    name: String,
}

#[derive(Serialize, Deserialize)]
struct Member {
    user_id: Id<User>,
    group_id: Id<Group>,
    permission: bool,
}
```

Unique id fields are not necessary, as every entry is automatically
given a unique identifier. References to entries in other tables
are denoted with `Id<T>`.

To make the actual schema, use the `schema!` macro:

```
# use rql::prelude::*;
# #[derive(Serialize, Deserialize)]
# struct User {
#     name: String,
#     age: u8,
# }
# #[derive(Serialize, Deserialize)]
# struct Group {
#     name: String,
# }
# #[derive(Serialize, Deserialize)]
# struct Member {
#     user_id: Id<User>,
#     group_id: Id<Group>,
#     permission: bool,
# }
schema! {
    MySchema {
        user: User,
        group: Group,
        member: Member,
    }
}
```

# Database operations

Below are a few simple ways of interfacing with the database.

```
# use rql::prelude::*;
# #[derive(Serialize, Deserialize)]
# struct User {
#     name: String,
#     age: u8,
# }
# #[derive(Serialize, Deserialize)]
# struct Group {
#     name: String,
# }
# #[derive(Serialize, Deserialize)]
# struct Member {
#     user_id: Id<User>,
#     group_id: Id<Group>,
#     permission: bool,
# }
# schema! {
#     MySchema {
#         user: User,
#         group: Group,
#         member: Member,
#     }
# }

# let _ = std::fs::remove_dir_all("test_database_example");

// Create a new database with the previously defined schema
// We pass a folder name for the database files as well as a representation type
let db = MySchema::new("test_database_example", HumanReadable).unwrap();

// Insert values into the database
// Insertion returns the new row's id
let dan   = db.user_mut().insert(User { name: "Dan".into(),   age: 25 });
let steve = db.user_mut().insert(User { name: "Steve".into(), age: 39 });
let mary  = db.user_mut().insert(User { name: "Mary".into(),  age: 31 });

let admin  = db.group_mut().insert(Group { name: "Admin".into()       });
let normal = db.group_mut().insert(Group { name: "Normal User".into() });

db.member_mut().insert(Member { user_id: dan,   group_id: admin,  permission: true  });
db.member_mut().insert(Member { user_id: steve, group_id: normal, permission: true  });
db.member_mut().insert(Member { user_id: mary,  group_id: normal, permission: false });

// Data can easily be looked up by id
db.user_mut().get_mut(dan).unwrap().age += 1;
let dan_age = db.user().get(dan).unwrap().age;
assert_eq!(dan_age, 26);

// Data can be selected from a table
let ages: Vec<u8> = db.user().select(|user| user.age).collect();

// Use `wher` to filter entries
let can_run_for_president: Vec<String> =
    db.user()
        .wher(|user| user.age >= 35)
        .select(|user| user.name.clone())
        .collect();

// Table intersections are done using `relate`
// A function relating the tables is required
for (user, permission) in db.user()
    .relate(
        &*db.member(),
        |user, member| user.id == member.user_id && member.group_id == normal
    )
    .select(|(user, member)| (&user.data.name, member.permission)) {
    println!("{} is a normal user with permission = {}", user, permission);
}

// Rows can be updated with `update`
for mut user in db.user_mut().update() {
    user.age += 1;
}

// Rows can be deleted in a few ways

// By id
db.user_mut().delete_one(steve);

// With a where clause
db.member_mut().delete_where(|member| member.permission);

// With an iterator over ids
db.user_mut().delete_iter(|_| vec![dan, mary]);

// Changes to the database are automatically saved, so they can be loaded again
let db_copy = MySchema::new("test_database_example", HumanReadable).unwrap();
assert_eq!(db.user().len(),   db_copy.user().len()  );
assert_eq!(db.group().len(),  db_copy.group().len() );
assert_eq!(db.member().len(), db_copy.member().len());
```
*/

mod row;
pub use row::*;
pub mod example_schema;
pub mod repr;

use std::{
    collections::HashMap,
    fmt, fs,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{RwLockReadGuard, RwLockWriteGuard},
};

use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize as Des, Serialize as Ser};
use uuid::Uuid;

pub use crate::repr::*;

/// A prelude for commonly used imports
pub mod prelude {
    pub use crate::{repr::*, schema, HasRows, HasRowsMut, Id, OwnedRow, Row, RowMut, Table};
    pub use serde_derive::{Deserialize, Serialize};
}

#[doc(hidden)]
pub use mashup;

/// An error type for `rql`
#[derive(Debug)]
pub enum Error {
    /// An io error
    Io(std::io::Error),
    /// A serialization/deserialization error
    Serialization(String),
}

macro_rules! error_from {
    ($variant:ident: $type:ty) => {
        impl From<$type> for Error {
            fn from(e: $type) -> Self {
                Error::$variant(e)
            }
        }
    };
    ($variant:ident<$param:ident>: $type:ty) => {
        impl From<$type> for Error<$param> {
            fn from(e: $type) -> Self {
                Error::$variant(e)
            }
        }
    };
}

error_from!(Io: std::io::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            Io(e) => write!(f, "{}", e),
            Serialization(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

/// A result type for `rql`
pub type Result<T> = std::result::Result<T, Error>;

/// An id for indexing rows
#[derive(Ser, Des)]
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
#[derive(Clone, Ser, Des)]
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
    /// Delete a single row based on its `Id` and return the value
    pub fn delete_one(&mut self, id: Id<T>) -> Option<T> {
        self.remove(id)
    }
    /// Delete rows that satisfy the clause
    ///
    /// Returns how many rows where deleted
    pub fn delete_where<F>(&mut self, f: F) -> usize
    where
        F: Fn(&T) -> bool,
    {
        let count = self.map.values().filter(|data| f(data)).count();
        self.map.retain(|_, data| !f(data));
        count
    }
    /// Delete rows with ids returned by an iterator
    pub fn delete_iter<'a, F, I, R>(&'a mut self, f: F)
    where
        F: Fn(&'a Self) -> I,
        I: IntoIterator<Item = R>,
        R: Idd<RowType = T>,
    {
        let ids: Vec<Id<T>> = f(unsafe { (self as *mut Self).as_mut() }.unwrap())
            .into_iter()
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

impl<T> Table<T>
where
    T: Serialize,
{
    /// Save the database to a file
    pub fn save<P>(&self, path: P, repr: Representation) -> Result<()>
    where
        P: AsRef<Path>,
    {
        fs::write(path, self.save_to_bytes(repr)?)?;
        Ok(())
    }
    /// Save the database to a byte vector
    pub fn save_to_bytes(&self, repr: Representation) -> Result<Vec<u8>> {
        repr.serialize(self).map_err(|e| Error::Serialization(e))
    }
}

impl<T> Table<T>
where
    T: DeserializeOwned,
{
    /// Load a database from a file
    pub fn load<P>(path: P, repr: Representation) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Table::load_from_bytes(fs::read(path)?, repr)
    }
    /// Load a database from a byte array
    pub fn load_from_bytes<B: AsRef<[u8]>>(bytes: B, repr: Representation) -> Result<Self> {
        repr.deserialize(bytes).map_err(|e| Error::Serialization(e))
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

/**
An immutable guard to a `Table`
*/
#[derive(Debug)]
pub struct TableGuard<'a, T>(#[doc(hidden)] pub RwLockReadGuard<'a, Table<T>>);

impl<'a, T> Deref for TableGuard<'a, T> {
    type Target = Table<T>;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct SaveParams {
    pub path: PathBuf,
    pub repr: Representation,
}

impl SaveParams {
    #[doc(hidden)]
    pub fn for_table_guard(&self, table: &str) -> Self {
        SaveParams {
            path: self.path.join(table).with_extension("yaml"),
            repr: self.repr,
        }
    }
}

impl Default for SaveParams {
    fn default() -> Self {
        SaveParams {
            path: PathBuf::from("database"),
            repr: HumanReadable,
        }
    }
}

/**
A mutable guard to a `Table`
*/
#[derive(Debug)]
pub struct TableGuardMut<'a, T>
where
    T: Serialize,
{
    #[doc(hidden)]
    pub guard: RwLockWriteGuard<'a, Table<T>>,
    #[doc(hidden)]
    pub params: SaveParams,
}

impl<'a, T> TableGuardMut<'a, T>
where
    T: Serialize,
{
    /// Get the path of the table file
    pub fn path(&self) -> PathBuf {
        self.params.path.clone()
    }
}

impl<'a, T> Deref for TableGuardMut<'a, T>
where
    T: Serialize,
{
    type Target = Table<T>;
    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T> DerefMut for TableGuardMut<'a, T>
where
    T: Serialize,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl<'a, T> Drop for TableGuardMut<'a, T>
where
    T: Serialize,
{
    fn drop(&mut self) {
        let _ = Table::<T>::save(&self.guard, &self.params.path, self.params.repr);
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
A relationship between two tables
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

// Define the schema
schema! {
    SchemaA {
        person: Person,
        account: Account,
    }
}
```
*/
#[macro_export]
macro_rules! schema {
    ($(#[$struct_attr:meta])* $name:ident { $($table:ident: $type:ty),* $(,)* }) => {
        $(#[$struct_attr])*
        struct $name {
            params: rql::SaveParams,
            $($table: std::sync::RwLock<rql::Table<$type>>),*
        }
        schema!(impl $name { $($table: $type),* });
    };
    ($(#[$struct_attr:meta])* pub $name:ident { $($table:ident: $type:ty),* $(,)* }) => {
        $(#[$struct_attr])*
        pub struct $name {
            params: rql::SaveParams,
            $($table: std::sync::RwLock<rql::Table<$type>>),*
        }
        schema!(impl $name { $($table: $type),* });
    };
    (impl $name:ident { $($table:ident: $type:ty),* }) => {
        use rql::mashup::*;
        mashup! {
            $(
                mut_name["mut_name" $table] = $table _mut;
            )*
        }
        impl $name {
            /// Create a new database with this schema or load it if it already exists
            pub fn new<P: AsRef<std::path::Path>>(dir: P, repr: rql::Representation) -> rql::Result<Self> {
                std::fs::create_dir_all(&dir)?;
                let params = rql::SaveParams { path: dir.as_ref().to_path_buf(), repr };
                Ok($name {
                    $(
                        $table: std::sync::RwLock::new(
                            Table::<$type>::load(
                                params.for_table_guard(stringify!($table)).path,
                                repr
                            ).unwrap_or_default()
                        ),
                    )*
                    params,
                })
            }
            $(
                /// Get an immutable guard to the table
                pub fn $table(&self) -> rql::TableGuard<$type> {
                    rql::TableGuard(self.$table.read().expect(concat!("Thread using ", stringify!($table), " table panicked")))
                }
                mut_name! {
                    /// Get a mutable guard to the table
                    pub fn "mut_name" $table (&self) -> rql::TableGuardMut<$type> {
                        rql::TableGuardMut {
                            guard: self.$table.write().expect("Thread using table panicked"),
                            params: self.params.for_table_guard(stringify!($table)),
                        }
                    }
                }
            )*

        }
    }
}

/// Test module
#[cfg(test)]
mod tests {
    /// Reexport everything from `rql` so macros work
    /// as if this is a foreign library
    mod rql {
        pub use crate::*;
    }
    use super::*;
    #[test]
    fn compiles() -> Result<()> {
        let _ = std::fs::remove_dir_all("test_database_compiles");
        schema! {
            Schema {
                nums: usize,
                strings: String,
            }
        }
        let db = Schema::new("test_database_compiles", HumanReadable)?;
        db.nums_mut().insert(4);
        db.nums_mut().insert(2);
        db.nums_mut().insert(5);
        db.strings_mut().insert("hi".into());
        db.strings_mut().insert("hello".into());
        db.strings_mut().insert("world".into());
        for s in db
            .nums()
            .relate(&*db.strings(), |i, s| s.len() == **i)
            .select(|(i, s)| format!("{}: {}", s, i))
        {
            println!("{:?}", s);
        }
        for s in db.strings().relate(&*db.nums(), |s, n| s.len() == **n) {
            println!("{:?}", s);
        }
        db.strings_mut().delete_where(|s| s.contains('h'));
        assert_eq!(1, db.strings().len());
        Ok(())
    }
}
