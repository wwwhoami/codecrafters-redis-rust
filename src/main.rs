use std::env;

use redis_starter_rust::{Config, Db, Server};
use tokio::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = Config::new(env::args()).unwrap_or_else(|err| {
        eprintln!("Problem parsing arguments: {}", err);
        std::process::exit(1);
    });
    let addr = format!("127.0.0.1:{}", config.port);
    let socket_addr = std::net::SocketAddr::V4(addr.parse().unwrap());

    let db = Db::new();

    let server = Server::new(socket_addr, db, config).await;

    if let Err(err) = server.run().await {
        eprintln!("Error running server: {}", err);
        std::process::exit(1);
    }

    Ok(())
}
