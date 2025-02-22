#[cfg(feature = "phext-storage")]
mod api_usage {
    use gluesql::{phext_storage::PhextStorage, prelude::Glue};

    pub async fn run() {
        let storage = PhextStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute("DROP TABLE IF EXISTS api_test").await.unwrap();

        glue.execute(
            "CREATE TABLE api_test (
                id INTEGER,
                name TEXT,
                nullable TEXT NULL,
                is BOOLEAN
            )",
        )
        .await
        .unwrap();

        glue.execute(
            "INSERT INTO api_test (
                id,
                name,
                nullable,
                is
            ) VALUES
                (1, 'test1', 'not null', TRUE),
                (2, 'test2', NULL, FALSE)",
        )
        .await
        .unwrap();
    }
}

fn main() {
    #[cfg(feature = "phext-storage")]
    futures::executor::block_on(api_usage::run());
}
