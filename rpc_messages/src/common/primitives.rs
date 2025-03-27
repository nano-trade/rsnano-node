use serde::{de::Visitor, Deserialize, Serialize};
use std::fmt::Debug;

#[macro_export]
macro_rules! rpc_number {
    ($name:ident, $type:ty, $visitor:ident) => {
        rpc_number_impl!(
            $name,
            $type,
            $visitor,
            visit_u64,
            u64,
            derive(Copy, Clone, PartialEq, Eq, Default, PartialOrd, Ord)
        );
    };
}

#[macro_export]
macro_rules! rpc_float {
    ($name:ident, $type:ty, $visitor:ident) => {
        rpc_number_impl!(
            $name,
            $type,
            $visitor,
            visit_f64,
            f64,
            derive(Copy, Clone, PartialEq, Default, PartialOrd)
        );
    };
}

#[macro_export]
macro_rules! rpc_number_impl {
    ($name:ident, $type:ty, $visitor:ident, $name_visitor:ident, $visitor_type:ty, $derive:meta) => {
        #[$derive]
        pub struct $name($type);

        impl $name{
            pub fn inner(&self) -> $type{
                self.0
            }
        }

        impl From<$type> for $name {
            fn from(value: $type) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $type {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.0, f)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.0.to_string())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_any($visitor {})
            }
        }

        struct $visitor {}

        impl<'de> serde::de::Visitor<'de> for $visitor {
            type Value = $name;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(stringify!($type))
            }


            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = v
                    .parse::<$type>()
                    .map_err(|_| serde::de::Error::custom(stringify!("expected " $type)))?;
                Ok(value.into())
            }

            fn $name_visitor<E>(self, v: $visitor_type) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let val: $type = v.try_into().map_err(|_| serde::de::Error::custom(stringify!("expected " $type)))?;
                Ok(val.into())
            }
        }
    };
}

//serde_json forwards visit_u8, etc to visit_u64, visit_f32 to visit_f64
//this is because serde_json handles integers as u64, and floating points as f64
rpc_number!(RpcU8, u8, RpcU8Visitor);
rpc_number!(RpcU16, u16, RpcU16Visitor);
rpc_number!(RpcU32, u32, RpcU32Visitor);
rpc_number!(RpcU64, u64, RpcU64Visitor);
rpc_float!(RpcF64, f64, RpcF64Visitor);

impl From<RpcU64> for usize {
    fn from(value: RpcU64) -> Self {
        value.inner() as usize
    }
}

/// Bool expressed as "1"=true and "0"=false
#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct RpcBoolNumber(bool);

impl From<bool> for RpcBoolNumber {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

impl From<RpcBoolNumber> for bool {
    fn from(value: RpcBoolNumber) -> Self {
        value.0
    }
}

impl Debug for RpcBoolNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for RpcBoolNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(if self.0 { "1" } else { "0" })
    }
}

impl<'de> Deserialize<'de> for RpcBoolNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let result = deserializer.deserialize_any(BoolVisitor {})?;
        Ok(RpcBoolNumber(result))
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct RpcBool(bool);

impl RpcBool {
    pub fn inner(&self) -> bool {
        self.0
    }
}

impl From<bool> for RpcBool {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

impl From<RpcBool> for bool {
    fn from(value: RpcBool) -> Self {
        value.0
    }
}

impl Debug for RpcBool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for RpcBool {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(if self.0 { "true" } else { "false" })
    }
}

impl<'de> Deserialize<'de> for RpcBool {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let result = deserializer.deserialize_any(BoolVisitor {})?;
        Ok(RpcBool(result))
    }
}

struct BoolVisitor {}

impl<'de> Visitor<'de> for BoolVisitor {
    type Value = bool;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("bool, string of bool, or 0/1 as string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            _ => Err(serde::de::Error::custom("bool expected")),
        }
    }

    //infallible
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }
}

pub fn unwrap_u64_or(i: Option<RpcU64>, default_value: u64) -> u64 {
    i.map(|x| x.into()).unwrap_or(default_value)
}

pub fn unwrap_u64_or_max(i: Option<RpcU64>) -> u64 {
    i.unwrap_or(u64::MAX.into()).into()
}

pub fn unwrap_u64_or_zero(i: Option<RpcU64>) -> u64 {
    i.unwrap_or_default().into()
}

pub fn unwrap_bool_or_false(i: Option<RpcBool>) -> bool {
    i.unwrap_or_default().into()
}

pub fn unwrap_bool_or_true(i: Option<RpcBool>) -> bool {
    i.unwrap_or(true.into()).into()
}

#[cfg(test)]
mod tests {
    use serde::de::DeserializeOwned;

    use super::*;

    #[test]
    fn u8_serialize() {
        let value = RpcU8::from(123);
        assert_eq!(format!("{:?}", value), "123");
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"123\"");
    }

    #[test]
    fn u8_deserialize() {
        assert_deserialize::<RpcU8>("\"123\"", 123);
        assert_deserialize::<RpcU8>("123", 123);
        assert_deserialize_fails::<RpcU8>("123.0");
        assert_deserialize_fails::<RpcU8>("");
        assert_deserialize_fails::<RpcU8>("a");
        assert_deserialize_fails::<RpcU8>("true");
        assert_deserialize_fails::<RpcU8>("256");
        assert_deserialize_fails::<RpcU8>("-1");
    }

    #[test]
    fn u16_serialize() {
        let value = RpcU16::from(123);
        assert_eq!(format!("{:?}", value), "123");

        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"123\"");
    }

    #[test]
    fn u16_deserialize() {
        assert_deserialize::<RpcU16>("\"123\"", 123);
        assert_deserialize::<RpcU16>("123", 123);
        assert_deserialize_fails::<RpcU16>("123.0");
        assert_deserialize_fails::<RpcU16>("");
        assert_deserialize_fails::<RpcU16>("a");
        assert_deserialize_fails::<RpcU16>("true");
        assert_deserialize_fails::<RpcU16>("66000");
        assert_deserialize_fails::<RpcU16>("-1");
    }

    #[test]
    fn u32_serialize() {
        let value = RpcU32::from(123);
        assert_eq!(format!("{:?}", value), "123");

        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"123\"");
    }

    #[test]
    fn u32_deserialize() {
        assert_deserialize::<RpcU32>("\"123\"", 123);
        assert_deserialize::<RpcU32>("123", 123);
        assert_deserialize_fails::<RpcU32>("123.0");
        assert_deserialize_fails::<RpcU32>("");
        assert_deserialize_fails::<RpcU32>("a");
        assert_deserialize_fails::<RpcU32>("true");
        assert_deserialize_fails::<RpcU32>("4000000000000000");
        assert_deserialize_fails::<RpcU32>("-1");
    }

    #[test]
    fn u64_serialize() {
        let value = RpcU64::from(123);
        assert_eq!(format!("{:?}", value), "123");

        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"123\"");
    }

    #[test]
    fn u64_deserialize() {
        assert_deserialize::<RpcU64>("\"123\"", 123);
        assert_deserialize::<RpcU64>("123", 123);
        assert_deserialize_fails::<RpcU64>("123.0");
        assert_deserialize_fails::<RpcU64>("");
        assert_deserialize_fails::<RpcU64>("a");
        assert_deserialize_fails::<RpcU64>("true");
    }

    #[test]
    fn f64_serialize() {
        let value = RpcF64::from(1.23);
        assert_eq!(format!("{:?}", value), "1.23");

        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"1.23\"");
    }

    #[test]
    fn f64_deserialize() {
        assert_deserialize::<RpcF64>("\"1.23\"", 1.23);
        assert_deserialize::<RpcF64>("1.23", 1.23);
        assert_deserialize_fails::<RpcF64>("");
        assert_deserialize_fails::<RpcF64>("1");
        assert_deserialize_fails::<RpcF64>("a");
    }

    #[test]
    fn bool_number_serialize() {
        let true_value = RpcBoolNumber::from(true);
        let false_value = RpcBoolNumber::from(false);
        assert_eq!(format!("{:?}", true_value), "true");
        assert_eq!(format!("{:?}", false_value), "false");
        let json = serde_json::to_string(&true_value).unwrap();
        assert_eq!(json, "\"1\"");
        let json = serde_json::to_string(&false_value).unwrap();
        assert_eq!(json, "\"0\"");
    }

    #[test]
    fn bool_number_deserialize() {
        assert_deserialize::<RpcBoolNumber>("\"1\"", true);
        assert_deserialize::<RpcBoolNumber>("\"0\"", false);
        assert_deserialize::<RpcBoolNumber>("\"true\"", true);
        assert_deserialize::<RpcBoolNumber>("\"false\"", false);

        assert_deserialize::<RpcBoolNumber>("true", true);
        assert_deserialize::<RpcBoolNumber>("false", false);

        assert_deserialize_fails::<RpcBoolNumber>("\"x\"");
        assert_deserialize_fails::<RpcBoolNumber>("x");
        assert_deserialize_fails::<RpcBoolNumber>("2");
    }

    #[test]
    fn bool_serialize() {
        let true_value = RpcBool::from(true);
        let false_value = RpcBool::from(false);
        assert_eq!(format!("{:?}", true_value), "true");
        assert_eq!(format!("{:?}", false_value), "false");

        let json = serde_json::to_string(&true_value).unwrap();
        assert_eq!(json, "\"true\"");
        let json = serde_json::to_string(&false_value).unwrap();
        assert_eq!(json, "\"false\"");
    }

    #[test]
    fn bool_deserialize() {
        assert_deserialize::<RpcBool>("\"1\"", true);
        assert_deserialize::<RpcBool>("\"0\"", false);

        assert_deserialize::<RpcBool>("\"true\"", true);
        assert_deserialize::<RpcBool>("\"false\"", false);

        assert_deserialize::<RpcBool>("true", true);
        assert_deserialize::<RpcBool>("false", false);

        assert_deserialize_fails::<RpcBool>("\"x\"");
        assert_deserialize_fails::<RpcBool>("x");
        assert_deserialize_fails::<RpcBool>("2");
        assert_deserialize_fails::<RpcBool>("");
    }

    fn assert_deserialize<T: DeserializeOwned + Debug + PartialEq>(
        input: &str,
        expected: impl Into<T>,
    ) {
        let deserialized: T = serde_json::from_str(input).unwrap();
        assert_eq!(deserialized, expected.into());
    }

    fn assert_deserialize_fails<T: DeserializeOwned>(input: &str) {
        let deserialized = serde_json::from_str::<T>(input);
        assert!(deserialized.is_err());
    }
}
