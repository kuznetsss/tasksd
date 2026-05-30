use serde::{Deserialize, Serialize};

use crate::api::error::ApiError;

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(try_from = "String", into = "&str")]
pub struct JsonRpcVersion;

impl JsonRpcVersion {
    const VALUE: &str = "2.0";
    pub fn value(&self) -> &'static str {
        Self::VALUE
    }
}

impl TryFrom<String> for JsonRpcVersion {
    type Error = ApiError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value != Self::VALUE {
            Err(ApiError::InvalidRpcJsonVersion(value))
        } else {
            Ok(Self)
        }
    }
}

impl From<JsonRpcVersion> for &'static str {
    fn from(val: JsonRpcVersion) -> Self {
        val.value()
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Number(i64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_rpc_version_try_from() {
        let err = JsonRpcVersion::try_from("invalid".to_string()).unwrap_err();
        assert!(matches!(err, ApiError::InvalidRpcJsonVersion(_)));

        let ok = JsonRpcVersion::try_from(JsonRpcVersion::VALUE.to_string()).unwrap();
        assert_eq!(ok.value(), JsonRpcVersion::VALUE)
    }

    #[test]
    fn json_rpc_version_into() {
        let s: &str = JsonRpcVersion {}.into();
        assert_eq!(s, JsonRpcVersion::VALUE);
    }
}
