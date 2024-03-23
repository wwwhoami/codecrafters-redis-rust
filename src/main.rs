use std::env;

use redis_starter_rust::{Command, Config, Connection, Db, Frame};
use tokio::{
    io::{self},
    net::{TcpListener, TcpStream},
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
    println!("Server is listening on port {}...", config.port);

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move { handle(socket, db).await });
    }
}

async fn handle(socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        let response = match Command::from_frame(frame) {
            Ok(command) => match command {
                Command::Echo(echo) => echo.execute(),
                Command::Ping(ping) => ping.execute(),
                Command::Set(set) => set.execute(&db),
                Command::Get(get) => get.execute(&db),
            },
            Err(err) => Frame::Error(err.to_string()),
        };

        connection.write_frame(&response).await.unwrap();

        println!("SENT: {:?}", response);
    }
}
