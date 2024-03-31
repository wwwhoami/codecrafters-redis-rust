use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tokio::net::{TcpListener, TcpStream};

use crate::{
    command::{psync::Psync, replconf::ReplConf, Ping},
    connection::Connection,
    Command, Config, Db, Frame,
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

        let connection = SlaveServer::handshake(info.clone(), &socket_addr).await?;
        let listener = TcpListener::bind(socket_addr).await.unwrap();

        Ok(Self {
            db,
            connection,
            listener,
            info,
        })
    }

    pub async fn run(self) -> crate::Result<()> {
        let db1 = self.db.clone();
        let info1 = self.info.clone();
        let connection = self.connection.clone();

        tokio::spawn(async move {
            let mut handle = Handle {
                connection,
                db: db1,
                info: info1,
            };
            let _ = handle.run().await;
        });

        self.run_listener().await
    }

    pub async fn run_master(&self) -> crate::Result<()> {
        println!("Role: {}", self.info.role.to_string());

        let mut handle = SlaveMasterHandle {
            connection: self.connection.clone(),
            db: self.db.clone(),
            info: self.info.clone(),
        };

        handle.run().await;

        Ok(())
    }

    pub async fn run_listener(self) -> crate::Result<()> {
        println!(
            "Server is listening on port {}...",
            self.listener.local_addr()?.port()
        );
        println!("Role: {}", self.info.role.to_string());

        loop {
            println!("Waiting for incoming traffic...");

            let connection = match self.listener.accept().await {
                Ok((stream, _)) => Connection::new(stream),
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            let db = self.db.clone();
            let info = self.info.clone();

            tokio::spawn(async move {
                let mut handle = Handle {
                    connection,
                    db,
                    info,
                };

                handle.run().await;
            });
        }
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
    async fn handshake(info: Info, socket_addr: &SocketAddr) -> crate::Result<Connection> {
        if info.role.is_master() {
            return Err("Error establishing handshake: not a slave".into());
        }

        let local_port = socket_addr.port();
        let master = info.get_master().unwrap();
        let addr = format!("{}:{}", master.0, master.1);
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        // let stream = socket.connect(addr).await?;
        let stream = TcpStream::connect(addr).await?;
        let connection = Connection::new(stream);

        println!("Handshaking with the master server...");

        // PING command to the master server
        let ping = Ping::default();
        let frame = ping.to_frame();

        connection.write_frame(frame.clone()).await.unwrap();
        println!("SENT: {:?}", frame);

        let response = connection.read_frame().await.unwrap().unwrap();
        println!("GOT: {:?}", response);

        // REPLCONF command to the master server
        let replconf = ReplConf::new(local_port);
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
        println!("Role: {}", self.info.role.to_string());

        loop {
            println!("Waiting for incoming traffic...");

            let connection = match self.listener.accept().await {
                Ok((stream, _)) => Connection::new(stream),
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

        tokio::spawn(async move {
            let mut handle = Handle {
                connection: conneciton,
                db,
                info,
            };

            handle.run().await;
        });
    }
}

pub struct SlaveMasterHandle {
    connection: Connection,
    db: Db,
    info: Info,
}

impl SlaveMasterHandle {
    pub async fn run(&mut self) {
        while let Some(frame) = self.connection.read_frame().await.unwrap() {
            println!("GOT: {:?}", frame);

            let response = Command::execute(
                frame.clone(),
                &self.db,
                &mut self.info,
                self.connection.clone(),
            );

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

pub struct Handle {
    connection: Connection,
    db: Db,
    info: Info,
}

impl Handle {
    pub async fn run(&mut self) {
        while let Some(frame) = self.connection.read_frame().await.unwrap() {
            println!("GOT: {:?}", frame);

            let response = Command::execute(
                frame.clone(),
                &self.db,
                &mut self.info,
                self.connection.clone(),
            );

            if self.info.role.is_master() {
                self.propagate(frame).await;
            }
            self.write_response(response).await;
        }
    }

    async fn write_response(&mut self, response: Frame) {
        match self.connection.write_frame(response.clone()).await {
            Ok(_) => println!("SENT: {:?}", response),
            Err(e) => eprintln!("Error writing frame: {}", e),
        }
    }

    async fn propagate(&mut self, frame: Frame) {
        // immidiately return if the command is not a write command
        if !Command::is_propagatable(frame.clone()).unwrap() {
            return;
        }

        // Propagate the command to all replicas
        match &self.info.role {
            Role::Master(master) => master.propagate(frame).await.unwrap(),
            Role::Slave(_) => {}
        }
    }
}

#[derive(Clone, Debug)]
pub struct Info {
    role: Role,
}

#[derive(Clone, Debug)]
enum Role {
    Master(Master),
    Slave(Slave),
}

impl Role {
    pub fn is_master(&self) -> bool {
        match self {
            Role::Master(_) => true,
            Role::Slave(_) => false,
        }
    }

    pub fn get_master(&self) -> Option<&(String, u16)> {
        match self {
            Role::Master(_) => None,
            Role::Slave(slave) => Some(&slave.master),
        }
    }
}

#[derive(Clone, Debug)]
struct Master {
    replicas: Arc<std::sync::Mutex<Vec<Replica>>>,
    master_replid: String,
    master_repl_offset: u64,
}

impl Master {
    pub fn new(master_replid: String, master_repl_offset: u64) -> Self {
        Self {
            replicas: Arc::new(std::sync::Mutex::new(Vec::new())),
            master_replid,
            master_repl_offset,
        }
    }

    pub fn add_replica(
        &mut self,
        addr: (String, u16),
        connection: Connection,
    ) -> crate::Result<()> {
        let replica = Replica::new(addr, connection);

        self.replicas.lock().unwrap().push(replica);

        Ok(())
    }

    pub async fn propagate(&self, frame: Frame) -> crate::Result<()> {
        let mut replicas = {
            let mut replicas = self.replicas.lock().unwrap();

            replicas
                .iter_mut()
                .map(|replica| replica.clone())
                .collect::<Vec<_>>()
        };

        println!("Replicas: {:?}", replicas.len());

        for replica in replicas.iter_mut() {
            replica.connection.write_frame(frame.clone()).await.unwrap();
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Replica {
    _addr: (String, u16),
    connection: Connection,
}

impl Replica {
    pub fn new(addr: (String, u16), connection: Connection) -> Self {
        Self {
            _addr: addr,
            connection,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Slave {
    master: (String, u16),
}

impl Slave {
    pub fn new(master: (String, u16)) -> Self {
        Self { master }
    }
}

impl Info {
    pub fn parse_config(config: &Config) -> Self {
        let master = config.replica_of.clone();
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
        let master_repl_offset = 0;

        let role = match master {
            Some(master) => Role::Slave(Slave::new(master)),
            None => Role::Master(Master::new(master_replid, master_repl_offset)),
        };

        Self { role }
    }

    pub fn get_master(&self) -> Option<&(String, u16)> {
        self.role.get_master()
    }

    pub fn master_replid(&self) -> Option<&str> {
        match &self.role {
            Role::Master(master) => Some(&master.master_replid),
            Role::Slave(_) => None,
        }
    }

    pub fn add_slave(&mut self, addr: (String, u16), connection: Connection) {
        match &mut self.role {
            Role::Master(master) => {
                master.add_replica(addr, connection).unwrap();
            }
            Role::Slave(_) => panic!("Not a master"),
        }
    }
}

impl ToString for Info {
    fn to_string(&self) -> String {
        match &self.role {
            Role::Master(master) => format!(
                "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                master.master_replid, master.master_repl_offset
            ),
            Role::Slave(_) => "role:slave\r\n".to_string(),
        }
    }
}

impl ToString for Role {
    fn to_string(&self) -> String {
        match self {
            Role::Master(_) => "role:master".into(),
            Role::Slave(_) => "role:slave".into(),
        }
    }
}
