use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                stream
                    .write_all(b"+PONG\r\n")
                    .expect("Failed to write to client");
            }
            Err(e) => {
                eprintln!("TCP Listener failed: {}", e);
            }
        }
    }
}
