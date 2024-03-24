use std::str::FromStr;

use bytes::Bytes;
use tokio::net::TcpListener;

use crate::{Command, Config, Connection, Db, Frame};

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

    pub async fn run(self) {
        println!(
            "Server is listening on port {}...",
            self.listener.local_addr().unwrap().port()
        );

        loop {
            let (socket, _) = self.listener.accept().await.unwrap();
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
    }
}

struct Handle {
    connection: Connection,
    db: Db,
    info: Info,
}

impl Handle {
    pub async fn run(&mut self) {
        while let Some(frame) = self.connection.read_frame().await.unwrap() {
            println!("GOT: {:?}", frame);

            let response = match Command::from_frame(frame) {
                Ok(command) => match command {
                    Command::Echo(echo) => echo.execute(),
                    Command::Ping(ping) => ping.execute(),
                    Command::Set(set) => set.execute(&self.db),
                    Command::Get(get) => get.execute(&self.db),
                    Command::Info(_) => Frame::Bulk(Bytes::from(self.info.to_string())),
                },
                Err(err) => Frame::Error(err.to_string()),
            };

            self.connection.write_frame(&response).await.unwrap();

            println!("SENT: {:?}", response);
        }
    }
}

#[derive(Clone)]
struct Info {
    role: Role,
    master_replid: String,
    master_repl_offset: u64,
}

#[derive(Clone)]
enum Role {
    Master,
    Slave,
}

impl Info {
    pub fn parse_config(config: Config) -> Self {
        let role = match config.replica_of {
            Some(_) => Role::Slave,
            None => Role::Master,
        };
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
        let master_repl_offset = 0;

        Self {
            role,
            master_replid,
            master_repl_offset,
        }
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
