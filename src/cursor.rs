use crate::{QLDBResult, QueryBuilder};
use ion_binary_rs::IonValue;

#[derive(Debug)]
pub struct Cursor {
    query_builder: QueryBuilder,
    next_page: Option<String>,
    page: u64,
}

impl Cursor {
    pub(crate) fn new(query_builder: QueryBuilder) -> Cursor {
        Cursor {
            query_builder,
            next_page: None,
            page: 0,
        }
    }

    pub async fn load_more(&mut self) -> QLDBResult<Option<Vec<IonValue>>> {
        let (values, next_page_token) = if self.page == 0 {
            self.query_builder.execute_statement().await?
        } else if let Some(page) = &self.next_page {
            self.query_builder.get_next_page(&page).await?
        } else {
            return Ok(None);
        };

        self.next_page = next_page_token;

        Ok(Some(values))
    }

    pub async fn load_all(&mut self) -> QLDBResult<Vec<IonValue>> {
        let mut result = vec![];

        while let Some(mut values) = self.load_more().await? {
            result.append(&mut values);
        }

        Ok(result)
    }
}
