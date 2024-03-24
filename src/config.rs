use std::env;

pub struct Config {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
}

impl Config {
    pub fn new(mut args: impl Iterator<Item = String>) -> crate::Result<Self> {
        let mut port = Self::parse_port_from_env()?;
        let mut replica_of = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-p" | "--port" => {
                    port = Self::match_port(args.next())?;
                }
                "--replicaof" => {
                    replica_of = Self::match_replica_of(args.next(), args.next())?;
                }

                _ => {}
            }
        }

        Ok(Self { port, replica_of })
    }

    fn match_port(port_arg: Option<String>) -> crate::Result<u16> {
        let port = port_arg.ok_or("Port value not found")?;

        port.parse::<u16>().map_err(|_| "Invalid PORT".into())
    }

    fn match_replica_of(
        host: Option<String>,
        port: Option<String>,
    ) -> crate::Result<Option<(String, u16)>> {
        let host = host.ok_or("Host value not found")?;
        let port = port.ok_or("Port value not found")?;

        let port = match port.parse() {
            Ok(port) => port,
            Err(_) => return Err("Invalid PORT".into()),
        };

        Ok(Some((host, port)))
    }

    fn parse_port_from_env() -> crate::Result<u16> {
        let port_str = env::var("REDIS_PORT").unwrap_or("6379".to_string());

        port_str.parse::<u16>().map_err(|_| "Invalid PORT".into())
    }
}
