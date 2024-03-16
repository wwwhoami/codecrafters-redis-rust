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
    const PING: &[u8] = b"*1\r\n$4\r\nping\r\n";
    let mut buf = [0; 512];

    loop {
        match socket.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                println!("Got: {:?}", &buf[..n]);

                let out_buf = match &buf[..n] {
                    PING => "+PONG\r\n",
                    _ => "-Error unkown command\r\n",
                };

                if socket.write_all(out_buf.as_bytes()).await.is_err() {
                    eprintln!("Unexpected socket error while writing to buffer")
                }
                println!("Responded: {:?}", out_buf);
            }
            Err(e) => {
                eprintln!("Unexpected socket error: {}", e);
            }
        }
    }
}
