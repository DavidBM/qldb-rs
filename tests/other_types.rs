mod utils;
use bigdecimal::BigDecimal;
use eyre::Result;
use ion_binary_rs::IonValue;
use std::collections::HashMap;
use std::str::FromStr;
use utils::create_type_test;

#[async_std::test]
async fn qldb_type_interger() -> Result<()> {
    create_type_test(get_value_to_insert_intergers(), |values| {
        assert_eq!(values.get("MIN").unwrap(), &IonValue::Integer(i64::MIN));
        assert_eq!(values.get("MAX").unwrap(), &IonValue::Integer(i64::MAX));
        assert_eq!(values.get("0").unwrap(), &IonValue::Integer(0));
        assert_eq!(
            values.get("123456789123456789").unwrap(),
            &IonValue::Integer(123456789123456789)
        );
    })
    .await
}

#[async_std::test]
async fn qldb_type_decimal() -> Result<()> {
    create_type_test(get_value_to_insert_decimals(), |values| {
        assert_eq!(values.get("0").unwrap(), &IonValue::Decimal(BigDecimal::from(0)));
        assert_eq!(values.get("-0").unwrap(), &IonValue::Decimal(BigDecimal::from(-0)));
        assert_eq!(values.get( "123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789").unwrap(), &IonValue::Decimal(BigDecimal::from_str("123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789").unwrap()));
        assert_eq!(values.get("-123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789").unwrap(), &IonValue::Decimal(BigDecimal::from_str("-123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789").unwrap()));
    })
    .await
}

#[async_std::test]
async fn qldb_type_bool() -> Result<()> {
    create_type_test(get_value_to_insert_bools(), |values| {
        assert_eq!(values.get("true").unwrap(), &IonValue::Bool(true));
        assert_eq!(values.get("false").unwrap(), &IonValue::Bool(false));
    })
    .await
}

#[async_std::test]
async fn qldb_type_float() -> Result<()> {
    create_type_test(get_value_to_insert_floats(), |values| {
        // Values are always returned as Float64
        assert_eq!(values.get("-0").unwrap(), &IonValue::Float(-0.0));
        assert_eq!(values.get("23f64").unwrap(), &IonValue::Float(23.0));
        assert_eq!(values.get("MIN64").unwrap(), &IonValue::Float(f64::MIN));
        assert_eq!(values.get("MAX64").unwrap(), &IonValue::Float(f64::MAX));
        assert_eq!(values.get("0").unwrap(), &IonValue::Float(0.0));
        assert_eq!(
            values.get("-123.123123123").unwrap(),
            &IonValue::Float(-123.123123123)
        );
        assert_eq!(
            values.get("123.123123123").unwrap(),
            &IonValue::Float(123.123123123)
        );
    })
    .await
}

#[async_std::test]
async fn qldb_type_string() -> Result<()> {
    create_type_test(get_value_to_insert_strings(), |values| {
        assert_eq!(
            values.get("1").unwrap(),
            &IonValue::String("test".to_string())
        );
        assert_eq!(values.get("2").unwrap(), &IonValue::String("".to_string()));
        assert_eq!(
            values.get("3").unwrap(),
            &IonValue::String("찦차를 타고 온 펲시맨과 쑛다리 똠방각하".to_string())
        );
        assert_eq!(
            values.get("4").unwrap(),
            &IonValue::String(",。・:*:・゜’( ☻ ω ☻ )。・:*:・゜’".to_string())
        );
        assert_eq!(
            values.get("5").unwrap(),
            &IonValue::String("❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙".to_string())
        );
        assert_eq!(
            values.get("6").unwrap(),
            &IonValue::String("᚛ᚄᚓᚐᚋᚒᚄ ᚑᚄᚂᚑᚏᚅ᚜".to_string())
        );
        assert_eq!(
            values.get("7").unwrap(),
            &IonValue::String("𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰".to_string())
        );
        assert_eq!(
            values.get("8").unwrap(),
            &IonValue::String("̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟".to_string())
        );
    })
    .await
}

#[async_std::test]
async fn qldb_type_symbol() -> Result<()> {
    create_type_test(get_value_to_insert_symbols(), |values| {
        assert_eq!(
            values.get("1").unwrap(),
            &IonValue::Symbol("test".to_string())
        );
        assert_eq!(values.get("2").unwrap(), &IonValue::Symbol("".to_string()));
        assert_eq!(
            values.get("3").unwrap(),
            &IonValue::Symbol("찦차를 타고 온 펲시맨과 쑛다리 똠방각하".to_string())
        );
        assert_eq!(
            values.get("4").unwrap(),
            &IonValue::Symbol(",。・:*:・゜’( ☻ ω ☻ )。・:*:・゜’".to_string())
        );
        assert_eq!(
            values.get("5").unwrap(),
            &IonValue::Symbol("❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙".to_string())
        );
        assert_eq!(
            values.get("6").unwrap(),
            &IonValue::Symbol("᚛ᚄᚓᚐᚋᚒᚄ ᚑᚄᚂᚑᚏᚅ᚜".to_string())
        );
        assert_eq!(
            values.get("7").unwrap(),
            &IonValue::Symbol("𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰".to_string())
        );
        assert_eq!(
            values.get("8").unwrap(),
            &IonValue::Symbol("̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟".to_string())
        );
    })
    .await
}

#[async_std::test]
async fn qldb_type_clob() -> Result<()> {
    create_type_test(get_value_to_insert_clob(), |values| {
        assert_eq!(values.get("1").unwrap(), &IonValue::Clob(b"\xdd\xcd\x5e\x1d\x76\x28\x9a\xb8\x5d\xcb\x7f\x7a\x10\x5d\x67\x3f\xea\x25\xb5\x67\x39\x3f\xd1\x3d\xdc\x83\x7b\x19\x5f\x3a\xa9\xa6".to_vec()));
    })
    .await
}

#[async_std::test]
async fn qldb_type_blob() -> Result<()> {
    create_type_test(get_value_to_insert_blob(), |values| {
        assert_eq!(values.get("1").unwrap(), &IonValue::Blob(b"\xdd\xcd\x5e\x1d\x76\x28\x9a\xb8\x5d\xcb\x7f\x7a\x10\x5d\x67\x3f\xea\x25\xb5\x67\x39\x3f\xd1\x3d\xdc\x83\x7b\x19\x5f\x3a\xa9\xa6".to_vec()));
    })
    .await
}

#[async_std::test]
async fn qldb_type_list() -> Result<()> {
    create_type_test(get_value_to_insert_list(), |values| {
        assert_eq!(
            values.get("1").unwrap(),
            &IonValue::List(
                vec!["list", "of", "strings"]
                    .iter()
                    .map(|v| v.into())
                    .collect()
            )
        );
    })
    .await
}

#[async_std::test]
async fn qldb_type_sexpr() -> Result<()> {
    create_type_test(get_value_to_insert_sexpr(), |values| {
        assert_eq!(
            values.get("1").unwrap(),
            &IonValue::SExpr(
                vec!["list", "of", "strings"]
                    .iter()
                    .map(|v| v.into())
                    .collect()
            )
        );
    })
    .await
}

fn get_value_to_insert_intergers() -> IonValue {
    let mut map = HashMap::new();
    map.insert("MIN", IonValue::Integer(i64::MIN));
    map.insert("MAX", IonValue::Integer(i64::MAX));
    map.insert("0", IonValue::Integer(0));
    map.insert("123456789123456789", IonValue::Integer(123456789123456789));
    map.into()
}

fn get_value_to_insert_decimals() -> IonValue {
    let mut map = HashMap::new();
    map.insert("0", IonValue::Decimal(BigDecimal::from(0)));
    map.insert("-0", IonValue::Decimal(BigDecimal::from(-0)));
    map.insert("123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789", IonValue::Decimal(BigDecimal::from_str("123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789").unwrap()));
    map.insert("-123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789", IonValue::Decimal(BigDecimal::from_str("-123459357252544523545234355642433542353957230545243556234525454243567891.2345342452534542334452533445233455424356789").unwrap()));
    map.into()
}

fn get_value_to_insert_bools() -> IonValue {
    let mut map = HashMap::new();
    map.insert("true", IonValue::Bool(true));
    map.insert("false", IonValue::Bool(false));
    map.into()
}

fn get_value_to_insert_floats() -> IonValue {
    let mut map = HashMap::new();
    map.insert("-0", IonValue::Float(-0.0));
    map.insert("23f64", IonValue::Float(23.0));
    map.insert("MIN64", IonValue::Float(f64::MIN));
    map.insert("MAX64", IonValue::Float(f64::MAX));
    map.insert("0", IonValue::Float(0.0));
    map.insert("-123.123123123", IonValue::Float(-123.123123123));
    map.insert("123.123123123", IonValue::Float(123.123123123));
    map.into()
}

fn get_value_to_insert_strings() -> IonValue {
    let mut map = HashMap::new();
    map.insert("1", "test");
    map.insert("2", "");
    map.insert("3", "찦차를 타고 온 펲시맨과 쑛다리 똠방각하");
    map.insert("4", ",。・:*:・゜’( ☻ ω ☻ )。・:*:・゜’");
    map.insert("5", "❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙");
    map.insert("6", "᚛ᚄᚓᚐᚋᚒᚄ ᚑᚄᚂᚑᚏᚅ᚜");
    map.insert("7", "𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰");
    map.insert("8", "̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟");
    map.into()
}

fn get_value_to_insert_symbols() -> IonValue {
    let mut map = HashMap::new();
    map.insert("1", IonValue::Symbol("test".to_string()));
    map.insert("2", IonValue::Symbol("".to_string()));
    map.insert(
        "3",
        IonValue::Symbol("찦차를 타고 온 펲시맨과 쑛다리 똠방각하".to_string()),
    );
    map.insert(
        "4",
        IonValue::Symbol(",。・:*:・゜’( ☻ ω ☻ )。・:*:・゜’".to_string()),
    );
    map.insert(
        "5",
        IonValue::Symbol("❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙".to_string()),
    );
    map.insert("6", IonValue::Symbol("᚛ᚄᚓᚐᚋᚒᚄ ᚑᚄᚂᚑᚏᚅ᚜".to_string()));
    map.insert(
        "7",
        IonValue::Symbol("𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰".to_string()),
    );
    map.insert(
        "8",
        IonValue::Symbol("̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟".to_string()),
    );
    map.into()
}

fn get_value_to_insert_clob() -> IonValue {
    let mut map = HashMap::new();
    map.insert("1", IonValue::Clob(b"\xdd\xcd\x5e\x1d\x76\x28\x9a\xb8\x5d\xcb\x7f\x7a\x10\x5d\x67\x3f\xea\x25\xb5\x67\x39\x3f\xd1\x3d\xdc\x83\x7b\x19\x5f\x3a\xa9\xa6".to_vec()));
    map.into()
}

fn get_value_to_insert_blob() -> IonValue {
    let mut map = HashMap::new();
    map.insert("1", b"\xdd\xcd\x5e\x1d\x76\x28\x9a\xb8\x5d\xcb\x7f\x7a\x10\x5d\x67\x3f\xea\x25\xb5\x67\x39\x3f\xd1\x3d\xdc\x83\x7b\x19\x5f\x3a\xa9\xa6".to_vec());
    map.into()
}

fn get_value_to_insert_list() -> IonValue {
    let mut map = HashMap::new();
    map.insert("1", vec!["list", "of", "strings"]);
    map.into()
}

fn get_value_to_insert_sexpr() -> IonValue {
    let mut map = HashMap::new();
    map.insert(
        "1",
        IonValue::SExpr(
            vec!["list", "of", "strings"]
                .iter()
                .map(|v| IonValue::String(v.to_string()))
                .collect(),
        ),
    );
    map.into()
}
