use std::collections::HashMap;
use qldb::QLDBClient;
use eyre::Result;
use ion_binary_rs::IonValue;

// In order to check internal steps of this transaction the JS
// implementation was taken as reference. It can be found in 
// test/reference_impl/qldb_transaction.js
#[async_std::test]
async fn qldb_transaction() -> Result<()> {

	let client = QLDBClient::default("rust-crate-test").await?;

	let result = client.transaction_within(|client| async move {

		// If the table already exist we ignore the error
		let result = client.query("CREATE TABLE QldbLibRsTest;", &[]).await;

		result?;

		Ok(())

	}).await;

	println!("{:?}", result);

	client.transaction_within(|client| async move {

		let result = client.query(r#"SELECT COUNT(*) FROM QldbLibRsTest;"#, &[]).await;

		println!("{:?}", result);

		let count = result?;

		let first_count = match &count[0] {
			IonValue::Struct(data) => match data.get("_1") {
				Some(IonValue::Integer(count)) => count,
				_ => panic!("First count returned a non integer"),
			},
			_ => panic!("First count returned a non integer"),
		};

		let value_to_insert = {
			let mut map = HashMap::new();
			map.insert("test_column".to_string(), IonValue::String("test_value".to_string()));
			IonValue::Struct(map)
		};

		let result = client.query("INSERT INTO QldbLibRsTest VALUE ?", &[value_to_insert]).await;

		println!("{:?}", result);

		let count = client.query("SELECT COUNT(*) FROM QldbLibRsTest", &[]).await?;

		match &count[0] {
			IonValue::Struct(data) => match data.get("_1") {
				Some(IonValue::Integer(count)) => assert_eq!(*count, first_count + 1),
				_ => panic!("Second count returned a non integer"),
			},
			_ => panic!("Second count returned a non integer"),
		};

		Ok(())
	}).await?;

	Ok(())
}
