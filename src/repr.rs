/*!
Ways of serializing/deserializing data
*/

use std::fmt;

use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};

/**
A trait for representations
*/
pub trait Representation: fmt::Debug {
    /// The encoding error type
    type EncodeError: fmt::Debug + fmt::Display;
    /// The decoding error type
    type DecodeError: fmt::Debug + fmt::Display;
    /// Save the database to a byte vector
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::EncodeError>
    where
        S: Serialize;
    /// Load a database from a byte array
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::DecodeError>
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BinaryStable;

impl Representation for BinaryStable {
    type EncodeError = bincode::Error;
    type DecodeError = bincode::Error;
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::EncodeError>
    where
        S: Serialize,
    {
        Ok(bincode::serialize(data)?)
    }
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::DecodeError>
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BinaryDynamic;

impl Representation for BinaryDynamic {
    type EncodeError = rmp_serde::encode::Error;
    type DecodeError = rmp_serde::decode::Error;
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::EncodeError>
    where
        S: Serialize,
    {
        Ok(rmp_serde::to_vec(data)?)
    }
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::DecodeError>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned,
    {
        Ok(rmp_serde::from_slice(bytes.as_ref())?)
    }
}

/**
A human readable format that is tolerant to most schema changes.

Columns and tables can be added, but not reordered or removed.
Use serde attributes to enable backward compatibility.

Uses [YAML](https://github.com/dtolnay/serde-yaml)
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HumanReadable;

impl Representation for HumanReadable {
    type EncodeError = serde_yaml::Error;
    type DecodeError = serde_yaml::Error;
    fn serialize<S>(data: &S) -> Result<Vec<u8>, Self::EncodeError>
    where
        S: Serialize,
    {
        Ok(serde_yaml::to_vec(data)?)
    }
    fn deserialize<B, D>(bytes: B) -> Result<D, Self::DecodeError>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned,
    {
        Ok(serde_yaml::from_slice(bytes.as_ref())?)
    }
}
