use std::{
    io::{self, Cursor},
    net::SocketAddr,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
};

use async_recursion::async_recursion;
use bytes::{Buf, Bytes, BytesMut};

use crate::frame::Error as FrameError;
use crate::frame::Frame;

#[derive(Debug)]
pub enum ConnectionMessage {
    ReadFrame(oneshot::Sender<crate::Result<Option<Frame>>>),
    ReadRdb(oneshot::Sender<crate::Result<Option<Frame>>>),
    WriteFrame(Frame, oneshot::Sender<crate::Result<()>>),
}

#[derive(Debug)]
pub struct ConnectionReaderActor {
    id: std::net::SocketAddr,
    stream: BufReader<OwnedReadHalf>,
    buffer: BytesMut,
    receiver: mpsc::Receiver<ConnectionMessage>,
}

impl Drop for ConnectionReaderActor {
    fn drop(&mut self) {
        println!("{:?}: Reader dropped", self.id);
    }
}

impl ConnectionReaderActor {
    pub fn new(
        id: std::net::SocketAddr,
        stream: OwnedReadHalf,
        receiver: mpsc::Receiver<ConnectionMessage>,
    ) -> Self {
        Self {
            id,
            stream: BufReader::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
            receiver,
        }
    }

    pub async fn run(mut self) -> crate::Result<()> {
        while let Some(message) = self.receiver.recv().await {
            match message {
                ConnectionMessage::ReadFrame(sender) => {
                    println!("{:?}: Reading frame", self.id);

                    let frame = self.read_frame().await;
                    let _ = sender.send(frame);

                    println!("{:?}: Frame read", self.id)
                }
                ConnectionMessage::ReadRdb(sender) => {
                    println!("{:?}: Reading RDB", self.id);

                    let frame = self.read_rdb().await;
                    let _ = sender.send(frame);

                    println!("{:?}: RDB read", self.id)
                }
                _ => (),
            }
        }

        Ok(())
    }

    async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err("connection reset by peer".into());
            }
        }
    }

    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // Not enough bytes is present in frame buffer
            // So wait for more data to be received
            Err(FrameError::Incomplete) => Ok(None),
            // Error encountered => connection is invalid
            Err(e) => Err(e.into()),
        }
    }

    /// Read RDB frame from the stream
    /// RDB frame is sent like $<length>\r\n<contents>
    /// Doesn't read any other frame type
    async fn read_rdb(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_rdb()? {
                return Ok(Some(frame));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err("Connection reset by peer".into());
            }
        }
    }

    /// Returns the parse rdb of this [`ConnectionReaderActor`].
    /// Used to parse the rdb payload from the buffer.
    ///
    ///
    /// # Errors
    ///
    /// This function will return an error if the buffer is not enough to parse the rdb.
    fn parse_rdb(&mut self) -> crate::Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check_rdb(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Frame::parse_rdb(&mut buf)?;

                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // Not enough bytes is present in frame buffer
            // So wait for more data to be received
            Err(FrameError::Incomplete) => Ok(None),
            // Error encountered => connection is invalid
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug)]
pub struct ConnectionWriterActor {
    id: std::net::SocketAddr,
    stream: BufWriter<OwnedWriteHalf>,
    receiver: mpsc::Receiver<ConnectionMessage>,
}

impl Drop for ConnectionWriterActor {
    fn drop(&mut self) {
        println!("{:?}: Writer dropped", self.id)
    }
}

impl ConnectionWriterActor {
    pub fn new(
        id: std::net::SocketAddr,
        stream: OwnedWriteHalf,
        receiver: mpsc::Receiver<ConnectionMessage>,
    ) -> Self {
        Self {
            id,
            stream: BufWriter::new(stream),
            receiver,
        }
    }

    pub async fn run(mut self) -> crate::Result<()> {
        while let Some(message) = self.receiver.recv().await {
            if let ConnectionMessage::WriteFrame(frame, sender) = message {
                println!("{:?}: Writing frame", self.id);

                let result = self
                    .write_frame(&frame)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>);

                let _ = sender.send(result);

                println!("{:?}: Frame written", self.id)
            }
        }

        Ok(())
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;

                self.write_decimal(val.len() as u64).await?;

                for entry in val {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        self.stream.flush().await
    }

    #[async_recursion]
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => self.write_simple_string(val).await?,
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Rdb(simple_fullresync, rdb_bytes) => {
                // Write RDB frame as writing a simple string
                // and then writing the rdb payload
                self.write_simple_string(simple_fullresync).await?;
                self.stream.flush().await?;

                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                self.write_rdb(rdb_bytes).await?;
            }
            Frame::RawBytes(bytes) => {
                self.write_rdb(bytes).await?;
            }
            Frame::NoSend => {}
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;

                for entry in val {
                    self.write_value(entry).await?;
                }
            }
        }

        Ok(())
    }

    async fn write_simple_string(&mut self, val: &str) -> io::Result<()> {
        self.stream.write_u8(b'+').await?;
        self.stream.write_all(val.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    /// Write RDB frame to the stream
    /// Sent like $<length>\r\n<contents>
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    async fn write_rdb(&mut self, content: &Bytes) -> io::Result<()> {
        let len = content.len() as u64;

        self.stream.write_u8(b'$').await?;
        self.write_decimal(len).await?;
        self.stream.write_all(content).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Connection {
    id: std::net::SocketAddr,
    write_sender: mpsc::Sender<ConnectionMessage>,
    read_sender: mpsc::Sender<ConnectionMessage>,
    addr: SocketAddr,
}

impl Drop for Connection {
    fn drop(&mut self) {
        println!("{:?}: Connection dropped", self.id)
    }
}

impl Connection {
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        let id = stream.peer_addr().unwrap();
        let (stream_reader, stream_writer) = stream.into_split();

        let (read_tx, read_rx) = mpsc::channel(10);
        let reader_actor = ConnectionReaderActor::new(id, stream_reader, read_rx);

        let (write_tx, write_rx) = mpsc::channel(10);
        let writer_actor = ConnectionWriterActor::new(id, stream_writer, write_rx);

        tokio::spawn(async move {
            if let Err(e) = reader_actor.run().await {
                eprintln!("Connection reader actor error: {:?}", e);
            }
        });

        tokio::spawn(async move {
            if let Err(e) = writer_actor.run().await {
                eprintln!("Connection writer actor error: {:?}", e);
            }
        });

        Self {
            id,
            addr,
            write_sender: write_tx,
            read_sender: read_tx,
        }
    }

    pub async fn read_frame(&self) -> crate::Result<Option<Frame>> {
        let (tx, rx) = oneshot::channel();

        self.read_sender
            .send(ConnectionMessage::ReadFrame(tx))
            .await?;

        rx.await?
    }

    pub async fn read_rdb(&self) -> crate::Result<Option<Frame>> {
        let (tx, rx) = oneshot::channel();

        self.read_sender
            .send(ConnectionMessage::ReadRdb(tx))
            .await?;

        rx.await?
    }

    pub async fn write_frame(&self, frame: Frame) -> crate::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.write_sender
            .send(ConnectionMessage::WriteFrame(frame, tx))
            .await?;

        rx.await?
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
