use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::{TcpListener, TcpStream};

use crate::{
    command::{
        psync::Psync,
        replconf::{ReplConf, ReplConfListeningPort},
        Ping,
    },
    connection::Connection,
    info::Role,
    Command, Config, Db, Frame, Info,
};

#[derive(Debug)]
pub enum Server {
    Master(MasterServer),
    Slave(SlaveServer),
}

impl Server {
    pub async fn new(socket_addr: SocketAddr, db: Db, config: Config) -> Self {
        match config.replica_of.is_none() {
            true => Server::Master(MasterServer::new(socket_addr, db, config).await),
            false => Server::Slave(SlaveServer::new(socket_addr, db, config).await.unwrap()),
        }
    }

    pub async fn run(self) -> crate::Result<()> {
        match self {
            Server::Master(server) => server.run().await,
            Server::Slave(server) => server.run().await,
        }
    }
}

#[derive(Debug)]
pub struct SlaveServer {
    db: Db,
    listener: TcpListener,
    connection: Connection,
    info: Info,
}

impl SlaveServer {
    pub async fn new(socket_addr: SocketAddr, db: Db, config: Config) -> crate::Result<Self> {
        let info = Info::parse_config(&config);

        let connection = SlaveServer::handshake(info.clone(), socket_addr.port()).await?;
        let listener = TcpListener::bind(socket_addr).await.unwrap();

        Ok(Self {
            db,
            connection,
            listener,
            info,
        })
    }

    pub async fn run(self) -> crate::Result<()> {
        let connection = self.connection.clone();

        // Connection to the master server
        self.handle_connection_to_master(connection).await;

        // Incoming connections
        self.run_listener().await
    }

    /// Run listener to accept incoming connections
    async fn run_listener(self) -> crate::Result<()> {
        println!(
            "Server is listening on port {}...",
            self.listener.local_addr()?.port()
        );
        println!("Role: {}", self.info.role().to_string());

        loop {
            println!("Waiting for incoming traffic...");

            let connection = match self.listener.accept().await {
                Ok((stream, addr)) => Connection::new(stream, addr),
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            self.handle_connection(connection).await;
        }
    }

    /// Connection to the master server
    async fn handle_connection_to_master(&self, connection: Connection) {
        let db = self.db.clone();
        let info = self.info.clone();

        // Spawn a task to handle the connection
        tokio::spawn(async move {
            let mut handle = SlaveToMasterHandle {
                connection,
                db,
                info,
            };

            handle.run().await;
        });
    }

    /// Connection to the incoming client
    async fn handle_connection(&self, conneciton: Connection) {
        let db = self.db.clone();
        let info = self.info.clone();

        // Spawn a task to handle the connection
        tokio::spawn(async move {
            let mut handle = SlaveHandle {
                connection: conneciton,
                db,
                info,
            };

            handle.run().await;
        });
    }

    /// The handshake is done by connecting to the master server
    /// and sending the following commands:
    /// 1. PING
    /// 2. REPLCONF
    /// 3. PSYNC
    ///
    /// # Panics
    ///
    /// Panics if the master server is not reachable.
    async fn handshake(info: Info, local_port: u16) -> crate::Result<Connection> {
        if info.role().is_master() {
            return Err("Error establishing handshake: not a slave".into());
        }

        let master = info.get_master().unwrap();

        let addr = format!("{}:{}", master.0, master.1);
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();

        let stream = TcpStream::connect(addr).await?;
        let connection = Connection::new(stream, addr);

        println!("Handshaking with the master server...");

        // PING command to the master server
        let ping = Ping::default();
        let frame = ping.to_frame();

        connection.write_frame(frame.clone()).await.unwrap();
        println!("SENT: {:?}", frame);

        let response = connection.read_frame().await.unwrap().unwrap();
        println!("GOT: {:?}", response);

        // REPLCONF command to the master server
        let replconf = ReplConf::ListeningPort(ReplConfListeningPort(local_port));
        let frames = replconf.to_frame();
        for frame in frames.into_array().unwrap() {
            connection.write_frame(frame.clone()).await.unwrap();
            println!("SENT: {:?}", frame);

            let response = connection.read_frame().await.unwrap().unwrap();
            println!("GOT: {:?}", response);
        }

        // PSYNC command to the master server
        let offset = -1;
        let replid = "?";
        let psync = Psync::new(offset, replid);
        let frame = psync.to_frame();
        connection.write_frame(frame.clone()).await.unwrap();
        println!("SENT: {:?}", frame);

        let response = connection.read_frame().await.unwrap().unwrap();
        println!("GOT: {:?}", response);

        let response = connection.read_rdb().await.unwrap().unwrap();
        println!("GOT: {:?}", response);

        println!("Handshake complete!");

        Ok(connection)
    }
}

#[derive(Debug)]
pub struct MasterServer {
    db: Db,
    listener: TcpListener,
    info: Info,
}

impl MasterServer {
    pub async fn new(socket_addr: SocketAddr, db: Db, config: Config) -> Self {
        let info = Info::parse_config(&config);

        let listener = TcpListener::bind(socket_addr).await.unwrap();

        Self { db, listener, info }
    }

    pub async fn run(self) -> crate::Result<()> {
        println!(
            "Server is listening on port {}...",
            self.listener.local_addr()?.port()
        );
        println!("Role: {}", self.info.role().to_string());

        loop {
            println!("Waiting for incoming traffic...");

            let (connection, _) = match self.listener.accept().await {
                Ok((stream, addr)) => (Connection::new(stream, addr), addr),
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            self.handle_connection(connection).await;
        }
    }

    async fn handle_connection(&self, conneciton: Connection) {
        let db = self.db.clone();
        let info = self.info.clone();

        // Spawn a task to handle the connection
        tokio::spawn(async move {
            let mut handle = MasterHandle {
                connection: conneciton,
                db,
                info,
            };

            handle.run().await;
        });
    }
}

pub struct SlaveToMasterHandle {
    connection: Connection,
    db: Db,
    info: Info,
}

impl SlaveToMasterHandle {
    pub async fn run(&mut self) {
        while let Some(frame) = self.connection.read_frame().await.unwrap() {
            println!("GOT: {:?}", frame);

            let (response, bytes_read) = Command::execute_replica(
                frame.clone(),
                &self.db,
                &mut self.info,
                self.connection.clone(),
            );

            if response != Frame::Null {
                self.write_response(response).await;
            }

            self.info.incr_offset(bytes_read as u64);
        }
    }

    async fn write_response(&mut self, response: Frame) {
        match self.connection.write_frame(response.clone()).await {
            Ok(_) => println!("SENT: {:?}", response),
            Err(e) => eprintln!("Error writing frame: {}", e),
        }
    }
}
pub struct SlaveHandle {
    connection: Connection,
    db: Db,
    info: Info,
}

impl SlaveHandle {
    pub async fn run(&mut self) {
        while let Some(frame) = self.connection.read_frame().await.unwrap() {
            println!("GOT: {:?}", frame);

            let (response, _bytes_read) = Command::execute(
                frame.clone(),
                &self.db,
                &mut self.info,
                self.connection.clone(),
            )
            .await;

            self.write_response(response).await;
        }
    }

    async fn write_response(&mut self, response: Frame) {
        match self.connection.write_frame(response.clone()).await {
            Ok(_) => println!("SENT: {:?}", response),
            Err(e) => eprintln!("Error writing frame: {}", e),
        }
    }
}

pub struct MasterHandle {
    connection: Connection,
    db: Db,
    info: Info,
}

impl MasterHandle {
    pub async fn run(&mut self) {
        while let Some(frame) = self.connection.read_frame().await.unwrap() {
            println!("GOT: {:?}", frame);

            let (response, bytes_read) = Command::execute(
                frame.clone(),
                &self.db,
                &mut self.info,
                self.connection.clone(),
            )
            .await;

            self.propagate(frame, bytes_read).await;

            self.write_response(response).await;
        }
    }

    async fn write_response(&mut self, response: Frame) {
        match self.connection.write_frame(response.clone()).await {
            Ok(_) => println!("SENT: {:?}", response),
            Err(e) => eprintln!("Error writing frame: {}", e),
        }
    }

    async fn propagate(&mut self, frame: Frame, bytes_read: usize) {
        // immidiately return if the command is not a write command
        if !Command::is_propagatable(frame.clone()).unwrap() {
            return;
        }

        // Command will be propagated to all replicas
        // So increment the offset by the bytes read
        self.info.incr_offset(bytes_read as u64);

        // Propagate the command to all replicas
        match &self.info.role() {
            Role::Master(master) => master.propagate_in_seq(frame).await.unwrap(),
            Role::Slave(_) => {}
        }
    }
}
