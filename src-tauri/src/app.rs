use sqlx::SqlitePool;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: SqlitePool,
    db_url: String,
}

impl AppState {
    pub(crate) fn new(pool: SqlitePool, db_url: String) -> Self {
        Self { pool, db_url }
    }

    pub(crate) fn db_url(&self) -> &str {
        &self.db_url
    }
}

pub(crate) type CommandResult<T> = Result<T, String>;

pub(crate) fn to_string(error: impl std::fmt::Display) -> String {
    error.to_string()
}
