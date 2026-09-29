use std::io::{Error, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::str::FromStr;

use clap::Parser;
use postcard::{from_bytes, to_allocvec};
use quanta::Instant;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[clap()]
pub struct Args {
    /// Listen on / connect to port
    #[clap(long, short = 'p', default_value_t = 18515)]
    port: u16,
    /// The IB device to use
    #[clap(long, short = 'd')]
    ib_dev: Option<String>,
    /// The port of IB device
    #[clap(long, short = 'i', default_value_t = 1)]
    ib_port: u8,
    /// The size of message to exchange
    #[clap(long, short = 's', default_value_t = 1024)]
    size: u32,
    /// Numbers of receives to post at a time
    #[clap(long, short = 'r', default_value_t = 500)]
    rx_depth: u32,
    /// Numbers of exchanges
    #[clap(long, short = 'n', default_value_t = 1000)]
    iter: u32,
    /// Local port GID index
    #[clap(long, short = 'g', default_value_t = 0)]
    gid_idx: u8,
    /// If no value provided, start a server and wait for connection, otherwise, connect to server at [host]
    #[arg(name = "host")]
    server_ip: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct Message {
    data: Vec<u32>,
}

#[allow(clippy::while_let_on_iterator)]
fn main() {
    let args = Args::parse();

    let mut stream = {
        let ip = IpAddr::from_str(args.server_ip.as_str()).expect("Invalid IP address");
        let server_addr = SocketAddr::from((ip, args.port));
        TcpStream::connect(server_addr).unwrap()
    };

    let local_context = Message {
        data: (0..1024).collect::<Vec<_>>(),
    };
    let mut msg_buf = Vec::new();
    let time_start = Instant::now();
    send(&mut stream, &local_context).unwrap();
    let _remote_context = receive(&mut stream, &mut msg_buf).unwrap();
    dbg!(time_start.elapsed());

    // println!("remote address: data {:?}", _remote_context.data);
}

fn send(stream: &mut TcpStream, dest: &Message) -> Result<(), Error> {
    let msg_buf = to_allocvec(dest).unwrap();
    let size = msg_buf.len().to_be_bytes();
    stream.write_all(&size)?;
    stream.write_all(&msg_buf)?;
    stream.flush()?;

    Ok(())
}
fn receive(stream: &mut TcpStream, msg_buf: &mut Vec<u8>) -> Result<Message, Error> {
    let mut size = usize::to_be_bytes(0);
    stream.read_exact(&mut size)?;
    msg_buf.clear();
    msg_buf.resize(usize::from_be_bytes(size), 0);
    stream.read_exact(&mut *msg_buf)?;
    let dest: Message = from_bytes(msg_buf).unwrap();

    Ok(dest)
}
