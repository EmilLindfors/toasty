use std::path::Path;

use anyhow::Result;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use toasty_core::{
    driver::{Capability, Operation},
    schema, sql, stmt, Driver, Schema,
};

use crate::{load, value_from_param};

#[derive(Debug, Clone)]
pub struct AsyncSqlite {
    pool: Pool<SqliteConnectionManager>,
}

impl AsyncSqlite {
    fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        Self { pool }
    }

    pub fn file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::new(manager)?;
        Ok(Self::new(pool))
    }

    pub fn in_memory() -> Result<Self> {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::new(manager)?;
        Ok(Self::new(pool))
    }

    pub async fn migrate(&self, sql: String) -> Result<()> {
        self.execute_query(move |conn| {
            conn.execute_batch(&sql)?;
            Ok(())
        })
        .await
    }

    pub async fn execute_query<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
        let res = tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            f(&conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("{:?}", e))??;

        Ok(res)
    }

    fn create_table(&self, schema: &Schema, table: &schema::Table) -> Result<()> {
        let connection = self.pool.get()?;

        let mut params = vec![];
        let stmt = sql::Statement::create_table(&table).to_sql_string(&schema, &mut params);
        assert!(params.is_empty());

        connection.execute(&stmt, [])?;

        // Create any indices
        for index in &table.indices {
            // The PK has already been created by the table statement
            if index.primary_key {
                continue;
            }

            let stmt = sql::Statement::create_index(index).to_sql_string(&schema, &mut params);
            assert!(params.is_empty());

            connection.execute(&stmt, [])?;
        }
        Ok(())
    }
}

#[toasty_core::async_trait]
impl Driver for AsyncSqlite {
    fn capability(&self) -> &Capability {
        &Capability::Sql
    }

    async fn register_schema(&mut self, _schema: &Schema) -> Result<()> {
        Ok(())
    }
    async fn exec<'stmt>(
        &self,
        schema: &Schema,
        op: Operation<'stmt>,
    ) -> Result<stmt::ValueStream<'stmt>> {
        use Operation::*;

        let connection = self.pool.get()?;

        let sql;
        let ty;

        match &op {
            QuerySql(op) => {
                sql = &op.stmt;
                ty = op.ty.as_ref();
            }
            Insert(op) => {
                sql = op;
                ty = None;
            }
            _ => todo!(),
        }

        let mut params = vec![];
        let sql_str = sql.to_sql_string(schema, &mut params);

        let mut stmt = connection.prepare(&sql_str).unwrap();

        if ty.is_none() {
            let exec = match sql {
                sql::Statement::Update(u) if u.pre_condition.is_some() => false,
                _ => true,
            };

            if exec {
                stmt.execute(rusqlite::params_from_iter(
                    params.iter().map(value_from_param),
                ))
                .unwrap();

                return Ok(stmt::ValueStream::new());
            }
        }

        let mut rows = stmt
            .query(rusqlite::params_from_iter(
                params.iter().map(value_from_param),
            ))
            .unwrap();

        let ty = match ty {
            Some(ty) => ty,
            None => &stmt::Type::Bool,
        };

        let mut ret = vec![];

        loop {
            match rows.next() {
                Ok(Some(row)) => {
                    if let stmt::Type::Record(tys) = ty {
                        let mut items = vec![];

                        for (index, ty) in tys.iter().enumerate() {
                            items.push(load(row, index, ty));
                        }

                        ret.push(stmt::Record::from_vec(items).into());
                    } else if let stmt::Type::Bool = ty {
                        ret.push(stmt::Record::from_vec(vec![]).into());
                    } else {
                        todo!()
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        // Some special handling
        if let sql::Statement::Update(update) = sql {
            if update.pre_condition.is_some() && ret.is_empty() {
                // Just assume the precondition failed here... we will
                // need to make this transactional later.
                anyhow::bail!("pre condition failed");
            } else if update.returning.is_none() {
                return Ok(stmt::ValueStream::new());
            }
        }

        Ok(stmt::ValueStream::from_vec(ret))
    }
    async fn reset_db(&self, schema: &Schema) -> Result<()> {
        for table in schema.tables() {
            self.create_table(schema, table)?;
        }

        Ok(())
    }
}
