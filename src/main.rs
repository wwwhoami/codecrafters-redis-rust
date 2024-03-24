use std::env;

use redis_starter_rust::{Config, Db, Server};
use tokio::{
    io::{self},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = Config::new(env::args()).unwrap_or_else(|err| {
        eprintln!("Problem parsing arguments: {}", err);
        std::process::exit(1);
    });
    let addr = format!("127.0.0.1:{}", config.port);

    let listener = TcpListener::bind(addr).await?;
    let db = Db::new();

    let server = Server::new(db, listener, config);

    server.run().await;

    Ok(())
}
