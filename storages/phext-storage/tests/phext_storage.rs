use {
    async_trait::async_trait, futures::stream::TryStreamExt, gluesql_core::prelude::Glue,
    gluesql_phext_storage::PhextStorage, test_suite::*,
};

struct PhextTester {
    glue: Glue<PhextStorage>,
}

#[async_trait(?Send)]
impl Tester<PhextStorage> for PhextTester {
    async fn new(_: &str) -> Self {
        let storage = PhextStorage::default();
        let glue = Glue::new(storage);

        PhextTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<PhextStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, PhextTester);

generate_alter_table_tests!(tokio::test, PhextTester);

generate_metadata_table_tests!(tokio::test, PhextTester);

generate_custom_function_tests!(tokio::test, PhextTester);

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

macro_rules! test {
    ($glue: ident $sql: expr, $result: expr) => {
        assert_eq!($glue.execute($sql).await, $result);
    };
}

#[tokio::test]
async fn phext_storage_index() {
    use gluesql_core::{
        prelude::{Error, Glue},
        store::{Index, Store},
    };

    let storage = PhextStorage::default();

    assert_eq!(
        storage
            .scan_data("Idx")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .as_ref()
            .map(Vec::len),
        Ok(0),
    );

    assert_eq!(
        storage
            .scan_indexed_data("Idx", "hello", None, None)
            .await
            .map(|_| ()),
        Err(Error::StorageMsg(
            "[PhextStorage] index is not supported".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");
    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[PhextStorage] index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[PhextStorage] index is not supported".to_owned()))
    );
}

#[tokio::test]
async fn phext_storage_transaction() {
    use gluesql_core::prelude::{Error, Glue, Payload};

    let storage = PhextStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[PhextStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Ok(vec![Payload::Commit]));
    test!(glue "ROLLBACK", Ok(vec![Payload::Rollback]));
}
