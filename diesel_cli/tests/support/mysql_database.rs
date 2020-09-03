extern crate url;

use diesel::connection::SimpleConnection;
use diesel::dsl::sql;
use diesel::mysql::connection::url::ConnectionOptions;
use diesel::sql_types::Bool;
use diesel::*;

pub struct Database {
    url: String,
    connection_options: ConnectionOptions,
}

impl Database {
    pub fn new(url: &str) -> Self {
        let connection_options = ConnectionOptions::parse(url).expect("url parsing failed");
        Database {
            url: url.into(),
            connection_options: connection_options,
        }
    }

    pub fn create(self) -> Self {
        let conn = MysqlConnection::establish(self.information_schema_url().as_ref()).unwrap();
        conn.execute(&format!("CREATE DATABASE `{}`", self.database_name()))
            .unwrap();
        self
    }

    pub fn exists(&self) -> bool {
        MysqlConnection::establish(&self.url).is_ok()
    }

    pub fn table_exists(&self, table: &str) -> bool {
        select(sql::<Bool>(&format!(
            "EXISTS \
                (SELECT 1 \
                 FROM information_schema.tables \
                 WHERE table_name = '{}'
                 AND table_schema = DATABASE())",
            table
        )))
        .get_result(&self.conn())
        .unwrap()
    }

    pub fn conn(&self) -> MysqlConnection {
        MysqlConnection::establish(&self.url)
            .expect(&format!("Failed to open connection to {}", &self.url))
    }

    pub fn execute(&self, command: &str) {
        self.conn()
            .batch_execute(command)
            .expect(&format!("Error executing command {}", command));
    }

    fn database_name(&self) -> String {
        self.connection_options
            .database()
            .expect("database unwrapping failed")
            .to_str()
            .expect("failed to convert Cstr to str")
            .to_string()
    }

    fn information_schema_url(&self) -> String {
        let mut split: Vec<&str> = self.url.split("/").collect();
        let _ = split.pop().unwrap();
        let mysql_url = format!("{}/{}", split.join("/"), "information_schema");
        mysql_url
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let conn = try_drop!(
            MysqlConnection::establish(self.information_schema_url().as_ref()),
            "Couldn't connect to database"
        );
        try_drop!(
            conn.execute(&format!(
                "DROP DATABASE IF EXISTS `{}`",
                self.database_name()
            )),
            "Couldn't drop database"
        );
    }
}
