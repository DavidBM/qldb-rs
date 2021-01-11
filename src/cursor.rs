use crate::DocumentCollection;
use crate::{QLDBResult, QueryBuilder};
use std::convert::TryInto;

/// Cursor allows to get all values from a statement page by page.
///
/// QLDB returns 200 documents for each page.
///
/// You don't need to directly use Cursor in your code. When the
/// method [](crate::QueryBuilder::execute) uses Cursor internally
/// in order to load all values.
///
/// ```rust,no_run
/// use qldb::{QLDBClient, Cursor};
/// # use std::collections::HashMap;
/// # use eyre::Result;
///
/// # async fn test() -> Result<()> {
/// let client = QLDBClient::default("rust-crate-test").await?;
///
/// let mut value_to_insert = HashMap::new();
/// // This will insert a documents with a key "test_column"
/// // with the value "IonValue::String(test_value)"
/// value_to_insert.insert("test_column", "test_value");
///
/// client
///     .transaction_within(|client| async move {   
///         let mut cursor = client
///             .query("SEL/CT * FROM TestTable")
///             .get_cursor()?;
///             
///             while let Some(mut values) = cursor.load_more().await? {
///                 println!("{:?}", values);
///             }
///
///         Ok(())
///     })
///     .await?;
/// # Ok(())
/// # }
/// ```
///
#[derive(Debug)]
pub struct Cursor {
    query_builder: QueryBuilder,
    next_page: Option<String>,
    is_first_page: bool,
}

impl Cursor {
    pub(crate) fn new(query_builder: QueryBuilder) -> Cursor {
        Cursor {
            query_builder,
            next_page: None,
            is_first_page: true,
        }
    }

    /// It loads the next page from a query. It automatically tracks
    /// the next_page_token, so you can call this method again and
    /// again in order to load all pages.
    ///
    /// It returns Ok(Some(_)) when QLDB returns documents.
    ///
    /// It returns Ok(None) when QLDB doesn't return documents,
    /// which means that there isn't more pages to query
    ///
    /// ```rust,no_run
    /// # use qldb::{Cursor, QLDBResult};
    ///
    /// # async fn test(mut cursor: Cursor) ->  QLDBResult<()> {
    ///     while let Some(mut values) = cursor.load_more().await? {
    ///         println!("{:?}", values);
    ///     }
    ///     
    /// #   Ok(())
    /// # }
    ///
    /// ```
    pub async fn load_more(&mut self) -> QLDBResult<Option<DocumentCollection>> {
        let (values, next_page_token) = if self.is_first_page {
            self.query_builder.execute_statement().await?
        } else if let Some(page) = &self.next_page {
            self.query_builder.execute_get_page(&page).await?
        } else {
            self.is_first_page = false;
            return Ok(None);
        };

        self.is_first_page = false;

        self.next_page = next_page_token;

        Ok(Some(values.try_into()?))
    }

    /// Loads all pages from the cursor and consumes it in the process.
    pub async fn load_all(mut self) -> QLDBResult<DocumentCollection> {
        let mut result = DocumentCollection::new(vec![]);

        while let Some(values) = self.load_more().await? {
            result.extend(values.into_iter());

            if self.next_page.is_none() {
                break;
            }
        }

        Ok(result)
    }
}
