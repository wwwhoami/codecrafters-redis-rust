use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle(stream);
            }
            Err(e) => {
                eprintln!("TCP Listener failed: {}", e);
            }
        }
    }
}
fn handle(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).expect("Failed to read from client");

        if bytes_read == 0 {
            return;
        }

        let out_buf = b"+PONG\r\n";
        stream
            .write_all(out_buf)
            .expect("Failed to write to the client");
    }
}
