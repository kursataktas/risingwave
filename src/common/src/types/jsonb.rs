// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::hash::Hash;

use bytes::Buf;
use jsonbb::{Value, ValueRef};

use super::Serial;
use crate::estimate_size::EstimateSize;
use crate::for_all_scalar_variants;
use crate::types::{
    Date, Decimal, Int256Ref, Interval, ListRef, Scalar, ScalarRef, ScalarRefImpl, StructRef, Time,
    Timestamp, Timestamptz, ToText, F32, F64,
};
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonbVal(pub(crate) Value);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct JsonbRef<'a>(pub(crate) ValueRef<'a>);

impl EstimateSize for JsonbVal {
    fn estimated_heap_size(&self) -> usize {
        self.0.capacity()
    }
}

/// The display of `JsonbVal` is pg-compatible format which has slightly different from
/// `serde_json::Value`.
impl fmt::Display for JsonbVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::types::to_text::ToText::write(&self.as_scalar_ref(), f)
    }
}

/// The display of `JsonbRef` is pg-compatible format which has slightly different from
/// `serde_json::Value`.
impl fmt::Display for JsonbRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::types::to_text::ToText::write(self, f)
    }
}

impl Scalar for JsonbVal {
    type ScalarRefType<'a> = JsonbRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        JsonbRef(self.0.as_ref())
    }
}

impl<'a> ScalarRef<'a> for JsonbRef<'a> {
    type ScalarType = JsonbVal;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        JsonbVal(self.0.into())
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash(state)
    }
}

impl PartialOrd for JsonbVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_scalar_ref().cmp(&other.as_scalar_ref())
    }
}

impl PartialOrd for JsonbRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // We do not intend to support ordering `jsonb` type.
        // Before #7981 is done, we do not panic but just compare its string representation.
        // Note that `serde_json` without feature `preserve_order` uses `BTreeMap` for json object.
        // So its string form always have keys sorted.
        //
        // In PostgreSQL, Object > Array > Boolean > Number > String > Null.
        // But here we have Object > true > Null > false > Array > Number > String.
        // Because in ascii: `{` > `t` > `n` > `f` > `[` > `9` `-` > `"`.
        //
        // This is just to keep consistent with the memcomparable encoding, which uses string form.
        // If we implemented the same typed comparison as PostgreSQL, we would need a corresponding
        // memcomparable encoding for it.
        self.0.to_string().cmp(&other.0.to_string())
    }
}

impl crate::types::to_text::ToText for JsonbRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        struct FmtToIoUnchecked<F>(F);
        impl<F: std::fmt::Write> std::io::Write for FmtToIoUnchecked<F> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let s = unsafe { std::str::from_utf8_unchecked(buf) };
                self.0.write_str(s).map_err(|_| std::io::ErrorKind::Other)?;
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        // Use custom [`ToTextFormatter`] to serialize. If we are okay with the default, this can be
        // just `write!(f, "{}", self.0)`
        use serde::Serialize as _;
        let mut ser =
            serde_json::ser::Serializer::with_formatter(FmtToIoUnchecked(f), ToTextFormatter);
        self.0.serialize(&mut ser).map_err(|_| std::fmt::Error)
    }

    fn write_with_type<W: std::fmt::Write>(
        &self,
        _ty: &crate::types::DataType,
        f: &mut W,
    ) -> std::fmt::Result {
        self.write(f)
    }
}

impl crate::types::to_binary::ToBinary for JsonbRef<'_> {
    fn to_binary_with_type(
        &self,
        _ty: &crate::types::DataType,
    ) -> crate::error::Result<Option<bytes::Bytes>> {
        Ok(Some(self.value_serialize().into()))
    }
}

impl std::str::FromStr for JsonbVal {
    type Err = <Value as std::str::FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl JsonbVal {
    /// Returns a jsonb `null`.
    pub fn null() -> Self {
        Self(Value::null())
    }

    /// Returns an empty array `[]`.
    pub fn empty_array() -> Self {
        Self(Value::array([]))
    }

    /// Returns an empty array `{}`.
    pub fn empty_object() -> Self {
        Self(Value::object([]))
    }

    /// Deserialize from a memcomparable encoding.
    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl bytes::Buf>,
    ) -> memcomparable::Result<Self> {
        let v = <String as serde::Deserialize>::deserialize(deserializer)?
            .parse()
            .map_err(|_| memcomparable::Error::Message("invalid json".into()))?;
        Ok(Self(v))
    }

    /// Deserialize from a pgwire "BINARY" encoding.
    pub fn value_deserialize(mut buf: &[u8]) -> Option<Self> {
        if buf.is_empty() || buf.get_u8() != 1 {
            return None;
        }
        Value::from_text(buf).ok().map(Self)
    }

    /// Convert the value to a [`serde_json::Value`].
    pub fn take(self) -> serde_json::Value {
        self.0.into()
    }
}

impl From<jsonbb::Value> for JsonbVal {
    fn from(v: Value) -> Self {
        Self(v)
    }
}

impl From<serde_json::Value> for JsonbVal {
    fn from(v: serde_json::Value) -> Self {
        Self(v.into())
    }
}

impl From<bool> for JsonbVal {
    fn from(v: bool) -> Self {
        Self(v.into())
    }
}

impl From<i16> for JsonbVal {
    fn from(v: i16) -> Self {
        Self(v.into())
    }
}

impl From<i32> for JsonbVal {
    fn from(v: i32) -> Self {
        Self(v.into())
    }
}

impl From<i64> for JsonbVal {
    fn from(v: i64) -> Self {
        Self(v.into())
    }
}

impl From<F32> for JsonbVal {
    fn from(v: F32) -> Self {
        Self(v.into())
    }
}

impl From<F32> for Value {
    fn from(v: F32) -> Self {
        if v.0 == f32::INFINITY {
            "Infinity".into()
        } else if v.0 == f32::NEG_INFINITY {
            "-Infinity".into()
        } else if v.0.is_nan() {
            "NaN".into()
        } else {
            v.0.into()
        }
    }
}

// NOTE: Infinite or NaN values are not JSON numbers. They are stored as strings in Postgres.
impl From<F64> for JsonbVal {
    fn from(v: F64) -> Self {
        Self(v.into())
    }
}

impl From<F64> for Value {
    fn from(v: F64) -> Self {
        if v.0 == f64::INFINITY {
            "Infinity".into()
        } else if v.0 == f64::NEG_INFINITY {
            "-Infinity".into()
        } else if v.0.is_nan() {
            "NaN".into()
        } else {
            v.0.into()
        }
    }
}

impl From<&str> for JsonbVal {
    fn from(v: &str) -> Self {
        Self(v.into())
    }
}

impl From<&[u8]> for JsonbVal {
    fn from(v: &[u8]) -> Self {
        Self(Value::from_bytes(v))
    }
}

impl From<JsonbRef<'_>> for JsonbVal {
    fn from(v: JsonbRef<'_>) -> Self {
        Self(v.0.to_owned())
    }
}

impl From<JsonbRef<'_>> for Value {
    fn from(v: JsonbRef<'_>) -> Self {
        v.0.into()
    }
}



impl From<ListRef<'_>> for JsonbVal {
    fn from(v: ListRef<'_>) -> Self {
        // let values: Vec<Value> = v
        //     .iter()
        //     .map(|x| match x {
        //         Some(x) => Value::from(x),
        //         None => Value::null(),
        //     })
        //     .collect();
        // JsonbVal(Value::array(values.iter().map(|v| v.as_ref())))
        todo!()
    }
}

impl From<StructRef<'_>> for JsonbVal {
    fn from(v: StructRef<'_>) -> Self {
        todo!()
    }
}

impl From<ListRef<'_>> for Value {
    fn from(v: ListRef<'_>) -> Self {
        todo!()
    }
}

impl From<StructRef<'_>> for Value {
    fn from(v: StructRef<'_>) -> Self {
        todo!()
    }
}

impl From<Date> for Value {
    fn from(v: Date) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Date> for JsonbVal {
    fn from(v: Date) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Time> for Value {
    fn from(v: Time) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Time> for JsonbVal {
    fn from(v: Time) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Interval> for Value {
    fn from(v: Interval) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Interval> for JsonbVal {
    fn from(v: Interval) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Timestamp> for Value {
    fn from(v: Timestamp) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Timestamp> for JsonbVal {
    fn from(v: Timestamp) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Timestamptz> for Value {
    fn from(v: Timestamptz) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Timestamptz> for JsonbVal {
    fn from(v: Timestamptz) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Decimal> for Value {
    fn from(v: Decimal) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Decimal> for JsonbVal {
    fn from(v: Decimal) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Int256Ref<'_>> for Value {
    fn from(v: Int256Ref<'_>) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Int256Ref<'_>> for JsonbVal {
    fn from(v: Int256Ref<'_>) -> Self {
        Self::from(v.to_text().as_str())
    }
}

impl From<Serial> for Value {
    fn from(v: Serial) -> Self {
        v.to_text().as_str().into()
    }
}

impl From<Serial> for JsonbVal {
    fn from(v: Serial) -> Self {
        Self::from(v.to_text().as_str())
    }
}

// macro_rules! impl_jsonb_from {
//     ($scalar_type:ty) => {
//         impl From<$scalar_type> for Value {
//             fn from(v: $scalar_type) -> Self {
//                 v.to_text().as_str().into()
//             }
//         }
//     };

//     ($scalar_type:ty) => {
//         impl From<$scalar_type> for JsonbVal {
//             fn from(v: $scalar_type) -> Self {
//                 Self::from(v.to_text().as_str())
//             }
//         }
//     };
// }

// impl_jsonb_from!(Decimal);
// impl_jsonb_from!(Serial);
// impl_jsonb_from!(Int256Ref<'_>);
// impl_jsonb_from!(Date);
// impl_jsonb_from!(Time);
// impl_jsonb_from!(Interval);
// impl_jsonb_from!(Timestamp);
// impl_jsonb_from!(Timestamptz);

impl<'a> From<JsonbRef<'a>> for ValueRef<'a> {
    fn from(v: JsonbRef<'a>) -> Self {
        v.0
    }
}

impl<'a> JsonbRef<'a> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        // As mentioned with `cmp`, this implementation is not intended to be used.
        // But before #7981 is done, we do not want to `panic` here.
        let s = self.0.to_string();
        serde::Serialize::serialize(&s, serializer)
    }

    /// Serialize to a pgwire "BINARY" encoding.
    pub fn value_serialize(&self) -> Vec<u8> {
        use std::io::Write;
        // Reuse the pgwire "BINARY" encoding for jsonb type.
        // It is not truly binary, but one byte of version `1u8` followed by string form.
        // This version number helps us maintain compatibility when we switch to more efficient
        // encoding later.
        let mut buf = Vec::with_capacity(self.0.capacity());
        buf.push(1);
        write!(&mut buf, "{}", self.0).unwrap();
        buf
    }

    /// Returns true if this is a jsonb `null`.
    pub fn is_jsonb_null(&self) -> bool {
        self.0.as_null().is_some()
    }

    /// Returns the type name of this jsonb.
    ///
    /// Possible values are: `null`, `boolean`, `number`, `string`, `array`, `object`.
    pub fn type_name(&self) -> &'static str {
        match self.0 {
            ValueRef::Null => "null",
            ValueRef::Bool(_) => "boolean",
            ValueRef::Number(_) => "number",
            ValueRef::String(_) => "string",
            ValueRef::Array(_) => "array",
            ValueRef::Object(_) => "object",
        }
    }

    /// Returns the length of this json array.
    pub fn array_len(&self) -> Result<usize, String> {
        let array = self
            .0
            .as_array()
            .ok_or_else(|| format!("cannot get array length of a jsonb {}", self.type_name()))?;
        Ok(array.len())
    }

    /// If the JSON is a boolean, returns the associated bool.
    pub fn as_bool(&self) -> Result<bool, String> {
        self.0
            .as_bool()
            .ok_or_else(|| format!("cannot cast jsonb {} to type boolean", self.type_name()))
    }

    /// Attempt to read jsonb as a JSON number.
    ///
    /// According to RFC 8259, only number within IEEE 754 binary64 (double precision) has good
    /// interoperability. We do not support arbitrary precision like PostgreSQL `numeric` right now.
    pub fn as_number(&self) -> Result<f64, String> {
        self.0
            .as_number()
            .ok_or_else(|| format!("cannot cast jsonb {} to type number", self.type_name()))?
            .as_f64()
            .ok_or_else(|| "jsonb number out of range".into())
    }

    /// This is part of the `->>` or `#>>` syntax to access a child as string.
    ///
    /// * It is not `as_str`, because there is no runtime error when the jsonb type is not string.
    /// * It is not same as [`std::fmt::Display`] or [`super::ToText`] (cast to string) in the
    ///   following 2 cases:
    ///   * Jsonb null is displayed as 4-letter `null` but treated as sql null here.
    ///       * This function writes nothing and the caller is responsible for checking
    ///         [`Self::is_jsonb_null`] to differentiate it from an empty string.
    ///   * Jsonb string is displayed with quotes but treated as its inner value here.
    pub fn force_str<W: std::fmt::Write>(&self, writer: &mut W) -> std::fmt::Result {
        match self.0 {
            ValueRef::String(v) => writer.write_str(v),
            ValueRef::Null => Ok(()),
            ValueRef::Bool(_) | ValueRef::Number(_) | ValueRef::Array(_) | ValueRef::Object(_) => {
                use crate::types::to_text::ToText as _;
                self.write_with_type(&crate::types::DataType::Jsonb, writer)
            }
        }
    }

    pub fn force_string(&self) -> String {
        let mut s = String::new();
        self.force_str(&mut s).unwrap();
        s
    }

    pub fn access_object_field(&self, field: &str) -> Option<Self> {
        self.0.get(field).map(Self)
    }

    pub fn access_array_element(&self, idx: usize) -> Option<Self> {
        self.0.get(idx).map(Self)
    }

    /// Returns an iterator over the elements if this is an array.
    pub fn array_elements(self) -> Result<impl Iterator<Item = JsonbRef<'a>>, String> {
        let array = self
            .0
            .as_array()
            .ok_or_else(|| format!("cannot extract elements from a jsonb {}", self.type_name()))?;
        Ok(array.iter().map(Self))
    }

    /// Returns an iterator over the keys if this is an object.
    pub fn object_keys(self) -> Result<impl Iterator<Item = &'a str>, String> {
        let object = self.0.as_object().ok_or_else(|| {
            format!(
                "cannot call jsonb_object_keys on a jsonb {}",
                self.type_name()
            )
        })?;
        Ok(object.keys())
    }

    /// Returns an iterator over the key-value pairs if this is an object.
    pub fn object_key_values(
        self,
    ) -> Result<impl Iterator<Item = (&'a str, JsonbRef<'a>)>, String> {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| format!("cannot deconstruct a jsonb {}", self.type_name()))?;
        Ok(object.iter().map(|(k, v)| (k, Self(v))))
    }
}

/// A custom implementation for [`serde_json::ser::Formatter`] to match PostgreSQL, which adds extra
/// space after `,` and `:` in array and object.
struct ToTextFormatter;

impl serde_json::ser::Formatter for ToTextFormatter {
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(b": ")
    }
}

// macro_rules! impl_convert_to_jsonb_val {
//     ($({ $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty }),*) => {
//         impl From<ScalarRefImpl<'_>> for Value {
//             fn from(val: ScalarRefImpl<'_>) -> Self {
//                 match val {
//                     $(ScalarRefImpl::$variant_name(inner) => inner.into()),*
//                 }
//             }
//         }
//     };
// }

// for_all_scalar_variants! { impl_convert_to_jsonb_val }
