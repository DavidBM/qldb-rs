use crate::types::{QLDBExtractError, QLDBExtractResult};
use ion_binary_rs::{IonExtractionError, IonValue};
use std::{collections::HashMap, convert::TryFrom};

#[derive(Clone)]
pub struct Document {
    info: HashMap<String, IonValue>,
}

impl TryFrom<IonValue> for Document {
    type Error = IonExtractionError;

    fn try_from(value: IonValue) -> Result<Self, Self::Error> {
        match value {
            IonValue::Struct(value) => Ok(value),
            _ => Err(IonExtractionError::TypeNotSupported(value)),
        }
    }
}

impl Document {
    /// Extract a value from the document and tries to transform to the value of the return type.
    /// Fails if the property is not there.
    pub fn extract_value<'a, T>(&self, name: &str) -> QLDBExtractResult<T>
    where
        T: TryFrom<&'a IonValue> + Send + Sync + Clone,
        <T as TryFrom<&'a IonValue>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let element = self.info
            .get(name)
            .ok_or_else(|| QLDBExtractError::MissingProperty(name.to_string()))?;

        match T::try_from(element) {
            Ok(result) => Ok(result),
            Err(err) => Err(QLDBExtractError::BadDataType),
        }
    }

    /// Same as `extract_value` but it returns None if the property is not there.
    pub fn extract_optional_value<'a, T>(&self, name: &str) -> QLDBExtractResult<Option<T>>
    where
        T: TryFrom<&'a IonValue> + Send + Sync + Clone,
        <T as TryFrom<&'a IonValue>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let element = match self.info.get(name) {
            Some(elem) => elem,
            None => return Ok(None),
        };

        match T::try_from(element) {
            Ok(result) => Ok(Some(result)),
            Err(err) => Err(QLDBExtractError::BadDataType),
        }
    }
}
