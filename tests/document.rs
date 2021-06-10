use ion_binary_rs::IonValue;
use qldb::{Document, DocumentCollection, QldbExtractError};
use std::convert::{TryFrom, TryInto};

fn get_qldb_struct() -> IonValue {
    IonValue::Struct(hashmap!(
        "Model".to_string() => IonValue::String("CLK 350".to_string()),
        "Type".to_string() => IonValue::String("Sedan".to_string()),
        "Color".to_string() => IonValue::String("White".to_string()),
        "VIN".to_string() => IonValue::String("1C4RJFAG0FC625797".to_string()),
        "Make".to_string() => IonValue::String("Mercedes".to_string()),
        "Year".to_string() => IonValue::Integer(2019)
    ))
}

#[test]
fn check_document() {
    let qldb_struct = get_qldb_struct();

    let document = Document::try_from(qldb_struct).unwrap();

    let value: String = document.get_value("Model").unwrap();
    assert_eq!(value, "CLK 350");

    let bad_property = "Mode";
    let get_error = document.get_value::<String>(bad_property).unwrap_err();
    if let QldbExtractError::MissingProperty(error_str) = get_error {
        assert_eq!(bad_property, error_str);
    } else {
        panic!()
    }

    let new_value: String = document.get_optional_value("Type").unwrap().unwrap();
    assert_eq!(new_value, "Sedan");

    let is_none = document
        .get_optional_value::<String>("Tipe")
        .unwrap()
        .is_none();
    assert_eq!(is_none, true);
}

#[test]
fn check_document_collection() {
    let structure = get_qldb_struct();

    let vector_structs = vec![structure.clone(), structure.clone(), structure.clone()];

    let documents: DocumentCollection = vector_structs.try_into().unwrap();

    let document: Document = get_qldb_struct().try_into().unwrap();

    let vector_docs = vec![document.clone(), document.clone(), document.clone()];

    let doc_collection = DocumentCollection::new(vector_docs);

    let value = doc_collection
        .clone()
        .into_iter()
        .map(|doc| doc.get_value::<u64>("Year"))
        .collect::<Result<Vec<u64>, _>>()
        .unwrap()
        .into_iter()
        .fold(0, |acc, val| acc + val);

    assert_eq!(documents, doc_collection);
    assert_eq!(value, 2019 * 3);
}

#[macro_export]
macro_rules! hashmap(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);
