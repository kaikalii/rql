/*!
Ways of serializing/deserializing data
*/

use std::{error, fmt};

use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize as Des, Serialize as Ser};

/**
A trait for representations
*/
pub trait Representation {
    /// The error type
    type Error: error::Error;
    /// Save the database to a byte vector
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::Error>
    where
        S: Serialize;
    /// Load a database from a byte array
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::Error>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned;
}

/**
A binary format that is not tolerant to schema changes

Use this only if you are absolutely certain that your schema
will not change.

Uses [bincode](https://github.com/TyOverby/bincode)
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Ser, Des)]
pub struct BinaryStable;

impl Representation for BinaryStable {
    type Error = bincode::Error;
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::Error>
    where
        S: Serialize,
    {
        Ok(bincode::serialize(data)?)
    }
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::Error>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned,
    {
        Ok(bincode::deserialize(bytes.as_ref())?)
    }
}

/**
A binary format that is tolerant to certain schema changes

Columns and tables can be added, but not reordered or removed.
Use serde attributes to enable backward compatibility.

Uses [MessagePack](https://github.com/3Hren/msgpack-rust)
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Ser, Des)]
pub struct BinaryDynamic;

/// An error type for the `BinaryDynamic` representation
#[derive(Debug)]
pub enum BinaryDynamicError {
    /// An encoding error
    Encode(rmp_serde::encode::Error),
    /// A decoding error
    Decode(rmp_serde::decode::Error),
}

impl fmt::Display for BinaryDynamicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinaryDynamicError::Encode(e) => write!(f, "{}", e),
            BinaryDynamicError::Decode(e) => write!(f, "{}", e),
        }
    }
}

impl error::Error for BinaryDynamicError {}

impl Representation for BinaryDynamic {
    type Error = BinaryDynamicError;
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::Error>
    where
        S: Serialize,
    {
        Ok(rmp_serde::to_vec(data).map_err(BinaryDynamicError::Encode)?)
    }
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::Error>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned,
    {
        Ok(rmp_serde::from_slice(bytes.as_ref()).map_err(BinaryDynamicError::Decode)?)
    }
}

/**
A human readable format that is tolerant to most schema changes.

Columns and tables can be added, reordered, or removed.
Use serde attributes to enable backward compatibility.

Uses [YAML](https://github.com/dtolnay/serde-yaml)
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Ser, Des)]
pub struct HumanReadable;

impl Representation for HumanReadable {
    type Error = serde_yaml::Error;
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::Error>
    where
        S: Serialize,
    {
        Ok(serde_yaml::to_vec(data)?)
    }
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::Error>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned,
    {
        Ok(serde_yaml::from_slice(bytes.as_ref())?)
    }
}
