use redis_starter_rust::{Command, Connection, Frame};
use tokio::{
    io::{self},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Server is listening...");

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move { handle(socket).await });
    }
}

async fn handle(socket: TcpStream) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        let response = match Command::from_frame(frame) {
            Ok(command) => match command {
                Command::Echo(echo) => echo.execute(),
                Command::Ping(ping) => ping.execute(),
            },
            Err(err) => Frame::Error(err.to_string()),
        };

        connection.write_frame(&response).await.unwrap();

        println!("SENT: {:?}", response);
    }
}
