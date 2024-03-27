use std::str::FromStr;
use tokio::net::{TcpListener, TcpStream};

use crate::{
    command::{psync::Psync, replconf::ReplConf, Ping},
    Command, Config, Connection, Db, Frame,
};

pub struct Server {
    db: Db,
    listener: TcpListener,
    info: Info,
}

impl Server {
    pub fn new(db: Db, listener: TcpListener, config: Config) -> Self {
        let info = Info::parse_config(config);

        Self { db, listener, info }
    }

    pub async fn run(self) -> crate::Result<()> {
        println!(
            "Server is listening on port {}...",
            self.listener.local_addr()?.port()
        );
        println!("Role: {}", self.info.role.to_string());

        // Connect to the master server if this server is a slave
        // and send PING command to the master server.
        self.handshake().await;

        loop {
            let (socket, _) = match self.listener.accept().await {
                Ok(connection) => connection,
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            self.handle_connection(socket).await;
        }
    }

    async fn handle_connection(&self, socket: TcpStream) {
        let db = self.db.clone();
        let info = self.info.clone();

        tokio::spawn(async move {
            let connection = Connection::new(socket);

            let mut handle = Handle {
                connection,
                db,
                info,
            };

            handle.run().await;
        });
    }

    /// The handshake is done by connecting to the master server
    /// and sending a PING command to it.
    ///
    /// # Panics
    ///
    /// Panics if the master server is not reachable.
    async fn handshake(&self) {
        if let Some(master) = &self.info.master {
            let addr = format!("{}:{}", master.0, master.1);
            let socket = TcpStream::connect(addr).await.unwrap();
            let mut connection = Connection::new(socket);

            println!("Handshaking with the master server...");

            // PING command to the master server
            let ping = Ping::default();
            let frame = ping.to_frame();

            connection.write_frame(&frame).await.unwrap();
            println!("SENT: {:?}", frame);

            let response = connection.read_frame().await.unwrap().unwrap();
            println!("GOT: {:?}", response);

            // REPLCONF command to the master server
            let replconf = ReplConf::new(self.listener.local_addr().unwrap().port());
            let frames = replconf.to_frame();
            for frame in frames.into_array().unwrap() {
                connection.write_frame(&frame).await.unwrap();
                println!("SENT: {:?}", frame);

                let response = connection.read_frame().await.unwrap().unwrap();
                println!("GOT: {:?}", response);
            }

            // PSYNC command to the master server
            let offset = -1;
            let replid = "?";
            let psync = Psync::new(offset, replid);
            let frame = psync.to_frame();
            connection.write_frame(&frame).await.unwrap();
            println!("SENT: {:?}", frame);

            let response = connection.read_frame().await.unwrap().unwrap();
            println!("GOT: {:?}", response);

            println!("Handshake completed!")
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

            let response = Command::execute(frame, &self.db, &self.info);

            self.write_response(response).await;
        }
    }

    async fn write_response(&mut self, response: Frame) {
        match self.connection.write_frame(&response).await {
            Ok(_) => println!("SENT: {:?}", response),
            Err(e) => eprintln!("Error writing frame: {}", e),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Info {
    role: Role,
    master: Option<(String, u16)>,
    master_replid: String,
    master_repl_offset: u64,
}

#[derive(Clone, Debug)]
enum Role {
    Master,
    Slave,
}

impl Info {
    pub fn parse_config(config: Config) -> Self {
        let master = config.replica_of;
        let role = match master {
            Some(_) => Role::Slave,
            None => Role::Master,
        };
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
        let master_repl_offset = 0;

        Self {
            master,
            role,
            master_replid,
            master_repl_offset,
        }
    }

    pub fn master_replid(&self) -> &str {
        &self.master_replid
    }
}

impl ToString for Info {
    fn to_string(&self) -> String {
        match self.role {
            Role::Master => format!(
                "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                self.master_replid, self.master_repl_offset
            ),
            Role::Slave => format!(
                "role:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                self.master_replid, self.master_repl_offset
            ),
        }
    }
}

impl FromStr for Role {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let role = match s {
            "role:master" => Role::Master,
            "role:slave" => Role::Slave,
            _ => return Err("Invalid role"),
        };

        Ok(role)
    }
}

impl ToString for Role {
    fn to_string(&self) -> String {
        match self {
            Role::Master => "role:master".into(),
            Role::Slave => "role:slave".into(),
        }
    }
}
