/*!
Ways of serializing/deserializing data
*/

use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize as Des, Serialize as Ser};

/**
Types of data representations
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Ser, Des)]
pub enum Representation {
    /**
    A binary format that is not tolerant to schema changes

    Use this only if you are absolutely certain that your schema
    will not change.

    Uses [bincode](https://github.com/TyOverby/bincode)
    */
    BinaryStable,
    /**
    A binary format that is tolerant to certain schema changes

    Columns and tables can be added, but not reordered or removed.
    Use serde attributes to enable backward compatibility.

    Uses [MessagePack](https://github.com/3Hren/msgpack-rust)
    */
    BinaryDynamic,
    /**
    A human readable format that is tolerant to most schema changes.

    Columns and tables can be added, reordered, or removed.
    Use serde attributes to enable backward compatibility.

    Uses [YAML](https://github.com/dtolnay/serde-yaml)
    */
    HumanReadable,
}

pub use Representation::*;

impl Representation {
    /// Use this `Representation` to serialize some data
    pub fn serialize<S>(self, data: &S) -> Result<Vec<u8>, String>
    where
        S: Serialize,
    {
        use Representation::*;
        Ok(match self {
            BinaryStable => bincode::serialize(data).map_err(|e| e.to_string())?,
            BinaryDynamic => rmp_serde::to_vec(data).map_err(|e| e.to_string())?,
            HumanReadable => serde_yaml::to_vec(data).map_err(|e| e.to_string())?,
        })
    }
    /// Use thie `Representation` to deserialize some data
    pub fn deserialize<B, D>(self, bytes: B) -> Result<D, String>
    where
        B: AsRef<[u8]>,
        D: DeserializeOwned,
    {
        use Representation::*;
        Ok(match self {
            BinaryStable => bincode::deserialize(bytes.as_ref()).map_err(|e| e.to_string())?,
            BinaryDynamic => rmp_serde::from_slice(bytes.as_ref()).map_err(|e| e.to_string())?,
            HumanReadable => serde_yaml::from_slice(bytes.as_ref()).map_err(|e| e.to_string())?,
        })
    }
}
