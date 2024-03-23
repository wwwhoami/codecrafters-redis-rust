use std::env;

pub struct Config {
    pub port: u16,
}

impl Config {
    pub fn new(args: impl Iterator<Item = String>) -> crate::Result<Self> {
        let port = Self::parse_port(args)?;

        Ok(Self { port })
    }

    fn parse_port(mut args: impl Iterator<Item = String>) -> crate::Result<u16> {
        let port_str = env::var("REDIS_PORT").unwrap_or("6379".to_string());

        let mut port = match port_str.parse() {
            Ok(port) => port,
            Err(_) => return Err("Invalid PORT".into()),
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-p" | "--port" => {
                    let port_arg = args.next().ok_or("Port value not found")?;
                    port = match port_arg.parse() {
                        Ok(port) => port,
                        Err(_) => return Err("Invalid PORT".into()),
                    };
                }
                _ => {}
            }
        }

        Ok(port)
    }
}
