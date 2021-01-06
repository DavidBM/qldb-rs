use crate::{
    document::Document,
    types::QLDBExtractError,
};
use ion_binary_rs::IonValue;
use std::convert::TryFrom;
use std::ops::Index;

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
