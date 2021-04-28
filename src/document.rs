use crate::types::{QLDBExtractError, QLDBExtractResult};
use ion_binary_rs::IonValue;
use std::{collections::HashMap, convert::TryFrom};

/// It contains the IonValue representing the QLDB Document.
///
/// It contains methods that will extract and try to transfor
/// the IonValue to your type.
///
/// ```rust,no_run
/// use qldb::{QLDBExtractResult, Document};
///
/// fn test(document: Document) -> QLDBExtractResult<u64> {
///
///     let points: u64 = document.get_value("points")?;
///
///     Ok(points)
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    document: HashMap<String, IonValue>,
}

impl TryFrom<IonValue> for Document {
    type Error = QLDBExtractError;

    fn try_from(value: IonValue) -> Result<Self, Self::Error> {
        match value {
            IonValue::Struct(value) => Ok(Document { document: value }),
            _ => Err(QLDBExtractError::NotADocument(value)),
        }
    }
}

impl Document {
    /// Extract a value from the document and tries to transform to the value of the return type.
    /// Fails if the property is not there.
    pub fn get_value<T>(&self, name: &str) -> QLDBExtractResult<T>
    where
        T: TryFrom<IonValue> + Send + Sync + Clone,
        <T as TryFrom<IonValue>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let element = self
            .document
            .get(name)
            .ok_or_else(|| QLDBExtractError::MissingProperty(name.to_string()))?;

        match T::try_from(element.clone()) {
            Ok(result) => Ok(result),
            Err(err) => Err(QLDBExtractError::BadDataType(Box::new(err))),
        }
    }

    /// Gets the raw IonValue
    pub fn get(&self, name: &str) -> Option<&IonValue> {
        self.document.get(name)
    }

    /// Same as `extract_value` but it returns None if the property is not there.
    pub fn get_optional_value<T>(&self, name: &str) -> QLDBExtractResult<Option<T>>
    where
        T: TryFrom<IonValue> + Send + Sync + Clone,
        <T as TryFrom<IonValue>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let element = match self.document.get(name) {
            Some(elem) => elem,
            None => return Ok(None),
        };

        match T::try_from(element.clone().to_owned()) {
            Ok(result) => Ok(Some(result)),
            Err(err) => Err(QLDBExtractError::BadDataType(Box::new(err))),
        }
    }
}
