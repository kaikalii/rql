use std::{
    collections::hash_map,
    fmt,
    ops::{Deref, DerefMut},
};

use crate::Id;

/// A row in a `Table`
pub struct Row<'a, T> {
    /// The `Row`'s id
    pub id: Id<T>,
    /// The `Row`'s data
    pub data: &'a T,
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
    /// The `RowMut`'s id
    pub id: Id<T>,
    /// The `RowMut`'s data
    pub data: &'a mut T,
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

/// An iterator over rows in a `Table`
#[derive(Debug)]
pub struct RowIter<'a, T> {
    pub(crate) inner: hash_map::Iter<'a, Id<T>, T>,
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
    pub(crate) inner: hash_map::IterMut<'a, Id<T>, T>,
}

impl<'a, T> Iterator for RowIterMut<'a, T> {
    type Item = RowMut<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(id, data)| RowMut { id: *id, data })
    }
}

/// A trait for getting row `Id`s
pub trait Idd {
    /// The type of the row
    type RowType;
    /// Get the row's id
    fn id(&self) -> Id<Self::RowType>;
}

impl<T> Idd for Id<T> {
    type RowType = T;
    fn id(&self) -> Id<Self::RowType> {
        *self
    }
}

impl<'a, T> Idd for Row<'a, T> {
    type RowType = T;
    fn id(&self) -> Id<Self::RowType> {
        self.id
    }
}

impl<'a, T> Idd for RowMut<'a, T> {
    type RowType = T;
    fn id(&self) -> Id<Self::RowType> {
        self.id
    }
}
