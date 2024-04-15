use std::env;

use redis_starter_rust::{Config, Db, RedisDB, Server};
use tokio::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = Config::new(env::args()).unwrap_or_else(|err| {
        eprintln!("Problem parsing arguments: {}", err);
        std::process::exit(1);
    });
    let addr = format!("127.0.0.1:{}", config.port);
    let socket_addr = std::net::SocketAddr::V4(addr.parse().unwrap());

    let db = init_db(&config).await;

    let server = Server::new(socket_addr, db, config).await;

    if let Err(err) = server.run().await {
        eprintln!("Error running server: {}", err);
        std::process::exit(1);
    }

    Ok(())
}

async fn init_db(config: &Config) -> Db {
    let rdb_filename = format!("{}/{}", config.dir, config.db_filename);
    let mut rdb = RedisDB::new(rdb_filename);

    match rdb.read_rdb().await {
        Ok(db_from_file) => Db::from_rdb(db_from_file),
        Err(err) => {
            eprintln!("Error reading RDB file: {}", err);
            Db::new()
        }
    }
}
