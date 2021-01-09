use crate::{document::Document, types::QLDBExtractError};
use ion_binary_rs::IonValue;
use std::convert::TryFrom;
use std::ops::Index;

/// Represents a collection of documents. It implements
/// [](std::iter::IntoIterator) so you can call
/// [](std::iter::IntoIterator::into_iter) in order to
/// use it in for loops or with [](std::iter::Iterator::map).
///
/// It implements [](std::ops::Index) too.
///
/// It adds some utilities methods in order to do common
/// operations.
///
/// You can use the into_iter in order to execute aggregate
/// values or to make other complex operation.
///
/// ```rust,no_run
///
/// use qldb::{DocumentCollection, QLDBExtractResult};
///
/// // Adds all the "points" attributes from each document.
/// // It stops early in case of error extracting the attribute.
/// fn count_points(matches: DocumentCollection) -> QLDBExtractResult<u64> {
///
///     // You can use other types as BigUInt, BigDecimal, etc
///     // in order to avoid overflow
///
///     let result: u64 = matches
///         .into_iter()
///         .map(|doc| doc.get_value::<u64>("points"))
///         .collect::<Result<Vec<u64>, _>>()?
///         .into_iter()
///         .fold(0, |acc, val| acc + val);
///
///     Ok(result)
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentCollection {
    documents: Vec<Document>,
}

impl TryFrom<Vec<IonValue>> for DocumentCollection {
    type Error = QLDBExtractError;

    fn try_from(ion_values_vector: Vec<IonValue>) -> Result<Self, Self::Error> {
        let mut documents_vector: Vec<Document> = Vec::new();

        for ion_value in ion_values_vector {
            let document = Document::try_from(ion_value)?;
            documents_vector.push(document);
        }

        Ok(DocumentCollection::new(documents_vector))
    }
}

impl DocumentCollection {
    pub fn new(documents: Vec<Document>) -> DocumentCollection {
        DocumentCollection { documents }
    }

    pub fn last(&self) -> Option<&Document> {
        self.documents.last()
    }
}

impl IntoIterator for DocumentCollection {
    type Item = Document;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.documents.into_iter()
    }
}

impl Index<usize> for DocumentCollection {
    type Output = Document;

    fn index(&self, index: usize) -> &Self::Output {
        &self.documents[index]
    }
}

impl Extend<Document> for DocumentCollection {

    fn extend<T: IntoIterator<Item=Document>>(&mut self, iter: T) {

        for doc in iter {
            self.documents.push(doc);
        }
    }
}
