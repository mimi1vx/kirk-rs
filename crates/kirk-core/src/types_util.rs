//! Typed dictionary extraction mirroring `kirk/libkirk/types.py`.
//!
//! [`dict_item`] extracts a value from a JSON object, enforcing the expected
//! type. As upstream, only `int` and `float` targets coerce (`i64` and `f64`
//! here); every other target requires a strict type match.

use serde_json::{Map, Value};

/// Error raised when a dictionary value has an unexpected type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("dict value must be a {expected} but it's {actual}")]
pub struct DictTypeError {
    expected: &'static str,
    actual: &'static str,
}

/// Python-style type name of a JSON value, used in error messages.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "int"
            } else {
                "float"
            }
        }
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// Truncate a float toward zero, mirroring Python `int(x)`.
#[allow(
    clippy::cast_possible_truncation,
    reason = "mirrors Python int() float truncation"
)]
fn float_to_int(value: f64) -> i64 {
    value as i64
}

/// A type that can be extracted with [`dict_item`].
pub trait DictItemValue: Sized {
    /// Python type name used in error messages.
    const TYPE_NAME: &'static str;

    /// Convert a JSON value, enforcing a strict type match.
    ///
    /// # Errors
    ///
    /// Returns [`DictTypeError`] when the value has an unexpected type.
    fn from_value(value: &Value) -> Result<Self, DictTypeError>;
}

impl DictItemValue for String {
    const TYPE_NAME: &'static str = "str";

    fn from_value(value: &Value) -> Result<Self, DictTypeError> {
        match value {
            Value::String(s) => Ok(s.clone()),
            other => Err(DictTypeError {
                expected: Self::TYPE_NAME,
                actual: json_type_name(other),
            }),
        }
    }
}

impl DictItemValue for bool {
    const TYPE_NAME: &'static str = "bool";

    fn from_value(value: &Value) -> Result<Self, DictTypeError> {
        match value {
            Value::Bool(b) => Ok(*b),
            other => Err(DictTypeError {
                expected: Self::TYPE_NAME,
                actual: json_type_name(other),
            }),
        }
    }
}

impl DictItemValue for i64 {
    const TYPE_NAME: &'static str = "int";

    fn from_value(value: &Value) -> Result<Self, DictTypeError> {
        match value {
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
                .or_else(|| n.as_f64().map(float_to_int))
                .ok_or(DictTypeError {
                    expected: Self::TYPE_NAME,
                    actual: json_type_name(value),
                }),
            other => Err(DictTypeError {
                expected: Self::TYPE_NAME,
                actual: json_type_name(other),
            }),
        }
    }
}

impl DictItemValue for f64 {
    const TYPE_NAME: &'static str = "float";

    fn from_value(value: &Value) -> Result<Self, DictTypeError> {
        match value {
            Value::Number(n) => n.as_f64().ok_or(DictTypeError {
                expected: Self::TYPE_NAME,
                actual: json_type_name(value),
            }),
            other => Err(DictTypeError {
                expected: Self::TYPE_NAME,
                actual: json_type_name(other),
            }),
        }
    }
}

/// Extract a value from a dictionary, ensuring the correct type is returned.
///
/// Returns `Ok(None)` when `key` is missing and `default` is `None`,
/// otherwise the default value. Present values must match `T`, except that
/// `i64` and `f64` targets accept any JSON number.
///
/// # Errors
///
/// Returns [`DictTypeError`] when a present value has an unexpected type.
pub fn dict_item<T: DictItemValue>(
    data: &Map<String, Value>,
    key: &str,
    default: Option<T>,
) -> Result<Option<T>, DictTypeError> {
    match data.get(key) {
        None => Ok(default),
        Some(value) => T::from_value(value).map(Some),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn map_with(key: &str, value: Value) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert(key.to_owned(), value);
        map
    }

    #[test]
    fn str_item() {
        let data = map_with("key", json!("value"));
        assert_eq!(
            dict_item::<String>(&data, "key", None).unwrap(),
            Some("value".to_owned())
        );
    }

    #[test]
    fn wrong_type_fails() {
        let data = map_with("key", json!(10));
        let err = dict_item::<String>(&data, "key", None).unwrap_err();
        assert_eq!(err.to_string(), "dict value must be a str but it's int");
    }

    #[test]
    fn int_coerces_to_float() {
        let data = map_with("key", json!(10));
        let val = dict_item::<f64>(&data, "key", None).unwrap();
        assert!(val.is_some_and(|v| (v - 10.0).abs() < f64::EPSILON));
    }

    #[test]
    fn float_coerces_to_int() {
        let data = map_with("key", json!(10.0));
        assert_eq!(dict_item::<i64>(&data, "key", None).unwrap(), Some(10));
    }

    #[test]
    fn missing_key_default_none() {
        let data = map_with("key2", json!("value"));
        assert_eq!(dict_item::<String>(&data, "key", None).unwrap(), None);
    }

    #[test]
    fn missing_key_default_str() {
        let data = map_with("key2", json!("value"));
        assert_eq!(
            dict_item(&data, "key", Some("ciao".to_owned())).unwrap(),
            Some("ciao".to_owned())
        );
    }
}
