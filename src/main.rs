use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
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

async fn handle(mut socket: TcpStream) {
    let mut buf = [0; 512];

    match socket.read(&mut buf).await {
        Ok(n) => {
            println!("Got: {:?}", &buf[..n]);

            let out_buf = b"+PONG\r\n";
            if socket.write_all(out_buf).await.is_err() {
                eprintln!("Unexpected socket error while writing to buffer")
            }
            println!("Responded: {:?}", out_buf);
        }
        Err(e) => {
            eprintln!("Unexpected socket error: {}", e);
        }
    }
}
