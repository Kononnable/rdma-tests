use std::io::{Error, Read, Write};
use std::net::{Ipv6Addr, SocketAddr, TcpListener, TcpStream};

use clap::Parser;
use postcard::{from_bytes, to_allocvec};
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
}

#[derive(Deserialize, Serialize, Debug)]
struct Message {
    data: Vec<u32>,
}

#[allow(clippy::while_let_on_iterator)]
fn main() {
    let args = Args::parse();

    let mut stream = {
        let server_addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, args.port));
        let listener = TcpListener::bind(server_addr).unwrap();
        let (stream, _peer_addr) = listener.accept().unwrap();
        stream
    };

    let mut msg_buf = Vec::new();
    let remote_context = receive_handshake(&mut stream, &mut msg_buf).unwrap();
    // println!("remote address: data {:?}", remote_context.data);

    let local_context = Message {
        data: remote_context.data.iter().map(|&x| x * 2).collect(),
    };
    send_handshake(&mut stream, &local_context).unwrap();
}

fn send_handshake(stream: &mut TcpStream, dest: &Message) -> Result<(), Error> {
    let msg_buf = to_allocvec(dest).unwrap();
    let size = msg_buf.len().to_be_bytes();
    stream.write_all(&size)?;
    stream.write_all(&msg_buf)?;
    stream.flush()?;

    Ok(())
}
fn receive_handshake(stream: &mut TcpStream, msg_buf: &mut Vec<u8>) -> Result<Message, Error> {
    let mut size = usize::to_be_bytes(0);
    stream.read_exact(&mut size)?;
    msg_buf.clear();
    msg_buf.resize(usize::from_be_bytes(size), 0);
    stream.read_exact(&mut *msg_buf)?;
    let dest: Message = from_bytes(msg_buf).unwrap();

    Ok(dest)
}
