use std::{
    net::SocketAddr,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::task::JoinSet;

use crate::{command::replconf::ReplConf, Config, Connection, Frame};

#[derive(Clone, Debug)]
pub struct Info {
    role: Role,
    offset: u64,
}

impl Info {
    pub fn parse_config(config: &Config) -> Self {
        let master = config.replica_of.clone();
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();

        let role = match master {
            Some(master) => Role::Slave(Slave::new(master)),
            None => Role::Master(Master::new(master_replid)),
        };

        Self { role, offset: 0 }
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

    pub fn get_replica_sock_addrs(&self) -> Vec<SocketAddr> {
        match &self.role {
            Role::Master(master) => {
                let replicas = master.replicas.lock().unwrap();
                replicas
                    .iter()
                    .map(|replica| replica.connection.addr())
                    .collect()
            }
            Role::Slave(_) => Vec::new(),
        }
    }

    pub async fn count_sync_repl(&self, count: u64, timeout: Duration) -> u64 {
        match &self.role {
            Role::Master(master) => master.count_sync_repl(self.offset, count, timeout).await,
            Role::Slave(_) => 0,
        }
    }

    pub fn replicas_count(&self) -> usize {
        match &self.role {
            Role::Master(master) => master.replicas.lock().unwrap().len(),
            Role::Slave(_) => 0,
        }
    }

    pub fn tx_repl_got_ack(&self) -> Option<&Sender<(SocketAddr, u64)>> {
        match &self.role {
            Role::Master(master) => Some(master.tx_repl_got()),
            Role::Slave(_) => None,
        }
    }

    pub fn update_replica_offset(&mut self, sock_addr: SocketAddr, offset: u64) {
        match &mut self.role {
            Role::Master(master) => master.update_replica_offset(sock_addr, offset),
            Role::Slave(_) => {
                panic!("Not a master")
            }
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    pub fn incr_offset(&mut self, offset: u64) {
        self.offset += offset;
    }

    pub fn role(&self) -> &Role {
        &self.role
    }
}

impl ToString for Info {
    fn to_string(&self) -> String {
        match &self.role {
            Role::Master(master) => format!(
                "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                master.master_replid, self.offset
            ),
            Role::Slave(_) => "role:slave\r\n".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Role {
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
pub struct Master {
    replicas: Arc<std::sync::Mutex<Vec<Replica>>>,
    master_replid: String,
    /// Sender to send acks from replicas
    tx_repl_got_ack: Sender<(SocketAddr, u64)>,
    /// Receiver to receive acks from replicas
    rx_repl_got_ack: Arc<Mutex<Receiver<(SocketAddr, u64)>>>,
}

impl Master {
    pub fn new(master_replid: String) -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            replicas: Arc::new(std::sync::Mutex::new(Vec::new())),
            master_replid,
            tx_repl_got_ack: tx,
            rx_repl_got_ack: Arc::new(Mutex::new(rx)),
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

    pub fn update_replica_offset(&mut self, sock_addr: SocketAddr, offset: u64) {
        let mut replicas = self.replicas.lock().unwrap();

        if let Some(replica) = replicas
            .iter_mut()
            .find(|r| r.connection.addr() == sock_addr)
        {
            replica.replication_offset = offset;
        }
    }

    /// Count the number of replicas that have synced with the master
    /// up to the given offset
    /// Function will wait for the timeout duration for the replicas to ack
    /// the offset
    ///
    /// # Returns
    ///
    /// Returns the number of replicas that have synced with the master
    ///
    /// # Arguments
    ///
    /// * `master_offset` - Offset to compare with the replicas offset
    /// * `target_count` - Target number of replicas to sync with the master
    /// * `timeout` - Duration to wait for the replicas to ack the offset
    pub async fn count_sync_repl(
        &self,
        master_offset: u64,
        target_count: u64,
        timeout: Duration,
    ) -> u64 {
        let mut synced_replicas = 0;
        let replicas_count = self.replicas_count() as u64;

        let target_count = target_count.min(replicas_count);

        println!("Target count: {}", target_count);
        println!("Replicas count: {}", replicas_count);
        println!("Master offset: {}", master_offset);
        println!("Timeout: {:?}", timeout);

        // Master has not written any commands
        // So all replicas are synced with the master
        if master_offset == 0 {
            return replicas_count;
        }

        // Propagate the GETACK command to all replicas
        let getack = ReplConf::GetAck;
        let frame = getack.to_frame();
        self.propagate(frame).await.unwrap();

        let rx = self.rx_repl_got_ack.lock().unwrap();

        // Wait for acks from the replicas
        loop {
            match rx.recv_timeout(timeout) {
                Ok((_sock_addr, offset)) => {
                    println!("Received ack");

                    if offset >= master_offset {
                        synced_replicas += 1;
                    }
                    if synced_replicas >= target_count {
                        break;
                    }
                }
                Err(_) => {
                    println!("Timeout");
                    break;
                }
            }
        }

        // Drain the channel buffer for any remaining acks for the next call
        while rx.try_recv().is_ok() {}

        println!("Synced replicas count: {}", synced_replicas);

        synced_replicas
    }

    /// Propagate the given frame to all replicas
    /// This function will send the frame to all replicas
    /// immidiately without waiting for the previous replica to ack
    /// the frame
    pub async fn propagate(&self, frame: Frame) -> crate::Result<()> {
        let connections = {
            let mut replicas = self.replicas.lock().unwrap();

            replicas
                .iter_mut()
                .map(|replica| replica.connection.clone())
                .collect::<Vec<_>>()
        };

        println!("Replicas: {:?}", connections.len());

        let mut tasks = JoinSet::new();

        for connection in connections {
            let frame = frame.clone();
            let task = async move {
                connection.write_frame(frame).await.unwrap();
            };
            tasks.spawn(task);
        }

        // Await all tasks to complete (for every connection to write the frame)
        while tasks.join_next().await.is_some() {}

        Ok(())
    }

    /// Propagate the given frame to all replicas in sequence
    pub async fn propagate_in_seq(&self, frame: Frame) -> crate::Result<()> {
        let connections = {
            let mut replicas = self.replicas.lock().unwrap();

            replicas
                .iter_mut()
                .map(|replica| replica.connection.clone())
                .collect::<Vec<_>>()
        };

        println!("Replicas: {:?}", connections.len());

        for connection in connections {
            connection.write_frame(frame.clone()).await.unwrap();
        }

        Ok(())
    }

    pub fn replicas_count(&self) -> usize {
        self.replicas.lock().unwrap().len()
    }

    pub fn tx_repl_got(&self) -> &mpsc::Sender<(SocketAddr, u64)> {
        &self.tx_repl_got_ack
    }
}

#[derive(Debug, Clone)]
struct Replica {
    addr: (String, u16),
    connection: Connection,
    replication_offset: u64,
}

impl Drop for Replica {
    fn drop(&mut self) {
        println!("Replica dropped: {:?}", self.addr);
    }
}

impl Replica {
    pub fn new(addr: (String, u16), connection: Connection) -> Self {
        Self {
            addr,
            connection,
            replication_offset: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Slave {
    /// Address of the master server
    master: (String, u16),
}

impl Slave {
    pub fn new(master: (String, u16)) -> Self {
        Self { master }
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
