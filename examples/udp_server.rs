use std::io::Error;
use std::net::{Ipv6Addr, SocketAddr, UdpSocket};

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

    let mut socket = {
        let server_addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, args.port));
        UdpSocket::bind(server_addr).unwrap()
    };

    let mut msg_buf = [0; 2048];
    let (remote_context, src) = receive(&mut socket, &mut msg_buf).unwrap();
    // println!("remote address: data {:?}", remote_context.data);

    let local_context = Message {
        data: remote_context.data.iter().map(|&x| x * 2).collect(),
    };
    send(&mut socket, &local_context, src).unwrap();
}

fn send(socket: &mut UdpSocket, dest: &Message, addr: SocketAddr) -> Result<(), Error> {
    let msg_buf = to_allocvec(dest).unwrap();
    socket.send_to(&msg_buf, addr)?;

    Ok(())
}
fn receive(socket: &mut UdpSocket, buf: &mut [u8]) -> Result<(Message, SocketAddr), Error> {
    let (_amt, src) = socket.recv_from(buf)?;
    let dest: Message = from_bytes(&buf).unwrap();

    Ok((dest, src))
}
