mod command;
mod connection;
mod frame;
mod parse;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub use command::Command;
pub use connection::Connection;
pub use frame::Frame;
pub use parse::Parse;
