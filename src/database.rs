use async_trait::async_trait;
use hbb_common::{bail, log, ResultType};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlConnection},
    sqlite::{SqliteConnectOptions, SqliteConnection},
    ConnectOptions, Connection, Error as SqlxError, Row,
};
use std::{ops::DerefMut, str::FromStr};

#[derive(Clone, Copy, Debug)]
enum DbType {
    Sqlite,
    MySql,
}

impl DbType {
    fn from_url(url: &str) -> DbType {
        if url.starts_with("mysql://") || url.starts_with("mysqlx://") {
            DbType::MySql
        } else {
            DbType::Sqlite
        }
    }
}

type SqlitePool = deadpool::managed::Pool<SqliteDbPool>;
type MySqlPool = deadpool::managed::Pool<MySqlDbPool>;

pub struct SqliteDbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for SqliteDbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<SqliteConnection, SqlxError> {
        let mut opt = SqliteConnectOptions::from_str(&self.url)?;
        opt.log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut SqliteConnection,
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

pub struct MySqlDbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for MySqlDbPool {
    type Type = MySqlConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<MySqlConnection, SqlxError> {
        let mut opt = MySqlConnectOptions::from_str(&self.url)?;
        opt.log_statements(log::LevelFilter::Debug);
        MySqlConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut MySqlConnection,
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    db_type: DbType,
    sqlite_pool: Option<SqlitePool>,
    mysql_pool: Option<MySqlPool>,
}

#[derive(Default)]
pub struct Peer {
    pub guid: Vec<u8>,
    pub id: String,
    pub uuid: Vec<u8>,
    pub pk: Vec<u8>,
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub status: Option<i64>,
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        let db_type = DbType::from_url(url);
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or_else(|_| "1".to_owned())
            .parse()
            .unwrap_or(1);
        log::debug!("MAX_DATABASE_CONNECTIONS={}, DB_TYPE={:?}", n, db_type);

        let (sqlite_pool, mysql_pool) = match db_type {
            DbType::Sqlite => {
                let url = if url.starts_with("sqlite://") {
                    url.strip_prefix("sqlite://").unwrap_or(url).to_string()
                } else {
                    url.to_string()
                };
                if !std::path::Path::new(&url).exists() {
                    std::fs::File::create(&url).ok();
                }
                let pool = SqlitePool::new(SqliteDbPool { url }, n);
                let _ = pool.get().await?; // test
                (Some(pool), None)
            }
            DbType::MySql => {
                let pool = MySqlPool::new(MySqlDbPool { url: url.to_owned() }, n);
                let _ = pool.get().await?; // test
                (None, Some(pool))
            }
        };

        let db = Database {
            db_type,
            sqlite_pool,
            mysql_pool,
        };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        match self.db_type {
            DbType::Sqlite => {
                let pool = self.sqlite_pool.as_ref().unwrap();
                sqlx::query(
                    "
                    create table if not exists peer (
                        guid blob primary key not null,
                        id varchar(100) not null,
                        uuid blob not null,
                        pk blob not null,
                        created_at datetime not null default(current_timestamp),
                        user blob,
                        status tinyint,
                        note varchar(300),
                        info text not null
                    ) without rowid;
                    create unique index if not exists index_peer_id on peer (id);
                    create index if not exists index_peer_user on peer (user);
                    create index if not exists index_peer_created_at on peer (created_at);
                    create index if not exists index_peer_status on peer (status);
                ",
                )
                .execute(pool.get().await?.deref_mut())
                .await?;
            }
            DbType::MySql => {
                let pool = self.mysql_pool.as_ref().unwrap();
                let mut conn = pool.get().await?;
                // MySQL 需要分开执行每个语句
                sqlx::query(
                    "
                    create table if not exists peer (
                        guid binary(16) primary key not null,
                        id varchar(100) not null,
                        uuid blob not null,
                        pk blob not null,
                        created_at datetime not null default current_timestamp,
                        user blob,
                        status tinyint,
                        note varchar(300),
                        info text not null
                    ) engine=InnoDB default charset=utf8mb4
                ",
                )
                .execute(conn.deref_mut())
                .await?;
                
                // MySQL 5.7 不支持 IF NOT EXISTS，尝试创建索引，如果已存在则忽略错误
                // 注意：BLOB/TEXT 列需要指定前缀长度
                let create_indexes = vec![
                    ("create unique index index_peer_id on peer (id)", "index_peer_id"),
                    ("create index index_peer_user on peer (user(255))", "index_peer_user"),
                    ("create index index_peer_created_at on peer (created_at)", "index_peer_created_at"),
                    ("create index index_peer_status on peer (status)", "index_peer_status"),
                ];
                
                for (sql, index_name) in create_indexes {
                    if let Err(e) = sqlx::query(sql).execute(conn.deref_mut()).await {
                        // 1061 表示索引已存在，这是正常的，可以忽略
                        // 其他错误应该被报告
                        let should_warn = match e.as_database_error() {
                            Some(db_err) => {
                                !db_err.code().map(|c| c == "1061").unwrap_or(false)
                            }
                            None => true,
                        };
                        if should_warn {
                            log::warn!("创建索引 {} 失败: {}", index_name, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        match self.db_type {
            DbType::Sqlite => {
                let pool = self.sqlite_pool.as_ref().unwrap();
                if let Some(row) = sqlx::query(
                    "select guid, id, uuid, pk, user, status, info from peer where id = ?",
                )
                .bind(id)
                .fetch_optional(pool.get().await?.deref_mut())
                .await?
                {
                    Ok(Some(Peer {
                        guid: row.get::<Vec<u8>, _>(0),
                        id: row.get::<String, _>(1),
                        uuid: row.get::<Vec<u8>, _>(2),
                        pk: row.get::<Vec<u8>, _>(3),
                        user: row.try_get::<Option<Vec<u8>>, _>(4).ok().flatten(),
                        status: row.try_get::<Option<i64>, _>(5).ok().flatten(),
                        info: row.get::<String, _>(6),
                    }))
                } else {
                    Ok(None)
                }
            }
            DbType::MySql => {
                let pool = self.mysql_pool.as_ref().unwrap();
                if let Some(row) = sqlx::query(
                    "select guid, id, uuid, pk, user, status, info from peer where id = ?",
                )
                .bind(id)
                .fetch_optional(pool.get().await?.deref_mut())
                .await?
                {
                    Ok(Some(Peer {
                        guid: row.get::<Vec<u8>, _>(0),
                        id: row.get::<String, _>(1),
                        uuid: row.get::<Vec<u8>, _>(2),
                        pk: row.get::<Vec<u8>, _>(3),
                        user: row.try_get::<Option<Vec<u8>>, _>(4).ok().flatten(),
                        status: row.try_get::<Option<i64>, _>(5).ok().flatten(),
                        info: row.get::<String, _>(6),
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let uuid_val = uuid::Uuid::new_v4();
        let guid = uuid_val.as_bytes().to_vec();
        let guid_16: [u8; 16] = *uuid_val.as_bytes();
        match self.db_type {
            DbType::Sqlite => {
                let pool = self.sqlite_pool.as_ref().unwrap();
                sqlx::query("insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)")
                    .bind(&guid)
                    .bind(id)
                    .bind(uuid)
                    .bind(pk)
                    .bind(info)
                    .execute(pool.get().await?.deref_mut())
                    .await?;
            }
            DbType::MySql => {
                let pool = self.mysql_pool.as_ref().unwrap();
                sqlx::query("insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)")
                    .bind(&guid_16[..])
                    .bind(id)
                    .bind(uuid)
                    .bind(pk)
                    .bind(info)
                    .execute(pool.get().await?.deref_mut())
                    .await?;
            }
        }
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        match self.db_type {
            DbType::Sqlite => {
                let pool = self.sqlite_pool.as_ref().unwrap();
                sqlx::query("update peer set id=?, pk=?, info=? where guid=?")
                    .bind(id)
                    .bind(pk)
                    .bind(info)
                    .bind(guid)
                    .execute(pool.get().await?.deref_mut())
                    .await?;
            }
            DbType::MySql => {
                let pool = self.mysql_pool.as_ref().unwrap();
                // MySQL 需要确保 guid 是 16 字节的数组
                if guid.len() != 16 {
                    bail!("guid must be 16 bytes for MySQL");
                }
                let mut guid_16 = [0u8; 16];
                guid_16.copy_from_slice(guid);
                sqlx::query("update peer set id=?, pk=?, info=? where guid=?")
                    .bind(id)
                    .bind(pk)
                    .bind(info)
                    .bind(&guid_16[..])
                    .execute(pool.get().await?.deref_mut())
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::tokio;
    #[test]
    fn test_insert() {
        insert();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert() {
        let db = super::Database::new("test.sqlite3").await.unwrap();
        let mut jobs = vec![];
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned
                    .insert_peer(&id, &empty_vec, &empty_vec, "")
                    .await
                    .unwrap();
            });
            jobs.push(a);
        }
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                cloned.get_peer(&id).await.unwrap();
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }

    #[test]
    fn test_mysql_insert() {
        mysql_insert();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn mysql_insert() {
        // 确保测试数据库存在
        let test_db_name = "rustdesk_test";
        
        // 尝试连接到 MySQL 服务器（使用 mysql 系统数据库）并创建测试数据库（如果不存在）
        use sqlx::{mysql::MySqlConnectOptions, ConnectOptions, Connection};
        use std::str::FromStr;
        use hbb_common::log;
        if let Ok(mut opt) = MySqlConnectOptions::from_str("mysql://root:123456@127.0.0.1:3306/mysql") {
            opt.log_statements(log::LevelFilter::Off);
            if let Ok(mut conn) = sqlx::MySqlConnection::connect_with(&opt).await {
                let _ = sqlx::query(&format!("CREATE DATABASE IF NOT EXISTS `{}`", test_db_name))
                    .execute(&mut conn)
                    .await;
                let _ = conn.close().await;
            }
        }

        // MySQL 连接 URL: mysql://user:password@host:port/database
        let mysql_url = format!("mysql://root:tonda123@127.0.0.1:3306/{}", test_db_name);
        let db = match super::Database::new(&mysql_url).await {
            Ok(db) => db,
            Err(e) => {
                eprintln!("无法连接到 MySQL 数据库: {}. 请确保 MySQL 服务正在运行。", e);
                return;
            }
        };

        // 测试插入和查询
        let test_id = "test_mysql_peer_001";
        let test_uuid = b"test-uuid-12345678";
        let test_pk = b"test-public-key-12345";
        let test_info = r#"{"ip":"127.0.0.1"}"#;

        // 清理可能存在的旧数据
        let _ = db.get_peer(test_id).await;

        // 测试插入
        let guid = db
            .insert_peer(test_id, test_uuid, test_pk, test_info)
            .await
            .unwrap();
        assert!(!guid.is_empty(), "插入后应该返回有效的 guid");

        // 测试查询
        let peer = db.get_peer(test_id).await.unwrap();
        assert!(peer.is_some(), "应该能查询到插入的数据");
        let peer = peer.unwrap();
        assert_eq!(peer.id, test_id);
        assert_eq!(peer.uuid, test_uuid);
        assert_eq!(peer.pk, test_pk);
        assert_eq!(peer.info, test_info);
        assert_eq!(peer.guid, guid);

        // 测试更新
        let new_pk = b"updated-public-key-67890";
        let new_info = r#"{"ip":"192.168.1.100"}"#;
        db.update_pk(&guid, test_id, new_pk, new_info).await.unwrap();

        // 验证更新
        let updated_peer = db.get_peer(test_id).await.unwrap().unwrap();
        assert_eq!(updated_peer.pk, new_pk);
        assert_eq!(updated_peer.info, new_info);

        // 并发测试
        let mut jobs = vec![];
        for i in 0..100 {
            let cloned = db.clone();
            let id = format!("mysql_test_{}", i);
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned
                    .insert_peer(&id, &empty_vec, &empty_vec, "")
                    .await
                    .unwrap();
            });
            jobs.push(a);
        }
        for i in 0..100 {
            let cloned = db.clone();
            let id = format!("mysql_test_{}", i);
            let a = tokio::spawn(async move {
                let result = cloned.get_peer(&id).await.unwrap();
                assert!(result.is_some(), "应该能查询到数据");
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}
