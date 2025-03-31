use crate::{
    utils::{BufferWriter, Deserialize, FixedSizeSerialize, MutStreamAdapter, Serialize, Stream},
    BlockHash, Root,
};
use primitive_types::U512;
use serde::de::Unexpected;
use std::hash::Hash;

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct QualifiedRoot {
    pub root: Root,
    pub previous: BlockHash,
}

impl QualifiedRoot {
    pub const ZERO: Self = QualifiedRoot::new(Root::zero(), BlockHash::zero());

    pub const fn new(root: Root, previous: BlockHash) -> Self {
        Self { root, previous }
    }

    pub fn to_bytes(&self) -> [u8; 64] {
        let mut buffer = [0; 64];
        let mut stream = MutStreamAdapter::new(&mut buffer);
        self.serialize(&mut stream);
        buffer
    }

    pub fn new_test_instance() -> Self {
        Self::new(Root::from(111), BlockHash::from(222))
    }

    pub fn decode_hex(s: impl AsRef<str>) -> anyhow::Result<Self> {
        let mut bytes = [0u8; 64];
        hex::decode_to_slice(s.as_ref(), &mut bytes)?;
        let root = Root::from_bytes(bytes[0..32].try_into().unwrap());
        let previous = BlockHash::from_bytes(bytes[32..].try_into().unwrap());
        Ok(Self { root, previous })
    }
}

impl Serialize for QualifiedRoot {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        self.root.serialize(writer);
        self.previous.serialize(writer);
    }
}

impl FixedSizeSerialize for QualifiedRoot {
    fn serialized_size() -> usize {
        Root::serialized_size() + BlockHash::serialized_size()
    }
}

impl Deserialize for QualifiedRoot {
    type Target = Self;
    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<QualifiedRoot> {
        let root = Root::deserialize(stream)?;
        let previous = BlockHash::deserialize(stream)?;
        Ok(QualifiedRoot { root, previous })
    }
}

impl From<U512> for QualifiedRoot {
    fn from(value: U512) -> Self {
        let bytes = value.to_big_endian();
        let root = Root::from_slice(&bytes[..32]).unwrap();
        let previous = BlockHash::from_slice(&bytes[32..]).unwrap();
        QualifiedRoot { root, previous }
    }
}

impl serde::Serialize for QualifiedRoot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!(
            "{}{}",
            self.root.encode_hex(),
            self.previous.encode_hex()
        ))
    }
}

impl<'de> serde::Deserialize<'de> for QualifiedRoot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(QualifiedRootVisitor {})
    }
}

struct QualifiedRootVisitor {}

impl<'de> serde::de::Visitor<'de> for QualifiedRootVisitor {
    type Value = QualifiedRoot;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a qualified root")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        QualifiedRoot::decode_hex(v)
            .map_err(|_| serde::de::Error::invalid_value(Unexpected::Str(v), &"a qualified root"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_hex() {
        let hex = "000000000000000000000000000000000000000000000000000000000000007B000000000000000000000000000000000000000000000000000000000000007C";
        let decoded = QualifiedRoot::decode_hex(hex).unwrap();
        assert_eq!(decoded.root, Root::from(123));
        assert_eq!(decoded.previous, BlockHash::from(124));
    }

    #[test]
    fn serialize_json() {
        let root = QualifiedRoot::new(Root::from(0xaabbcc), BlockHash::from(0x112233));
        let json = serde_json::to_string(&root).unwrap();
        assert_eq!(json, "\"0000000000000000000000000000000000000000000000000000000000AABBCC0000000000000000000000000000000000000000000000000000000000112233\"");
    }

    #[test]
    fn deserialize_json() {
        let input =  "\"0000000000000000000000000000000000000000000000000000000000AABBCC0000000000000000000000000000000000000000000000000000000000112233\"";
        let expected = QualifiedRoot::new(Root::from(0xaabbcc), BlockHash::from(0x112233));
        let result: QualifiedRoot = serde_json::from_str(input).unwrap();
        assert_eq!(result, expected);
    }
}
