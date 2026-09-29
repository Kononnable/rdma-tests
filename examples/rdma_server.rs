use std::io::{Error, Read, Write};
use std::net::{Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use clap::Parser;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::address::{AddressHandleAttribute, Gid};
use sideway::ibverbs::completion::{GenericCompletionQueue, WorkCompletionStatus};
use sideway::ibverbs::device::{DeviceInfo, DeviceList};
use sideway::ibverbs::device_context::Mtu;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::queue_pair::{
    BasicQueuePair, PostSendGuard, QueuePair, QueuePairAttribute, QueuePairState,
    SetScatterGatherEntry, WorkRequestFlags,
};

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
struct Handshake {
    lid: u32,
    qp_number: u32,
    packet_seq_number: u32,
    gid: Gid,
}
const MTU: Mtu = Mtu::Mtu4096;
const SEND_WR_ID: u64 = 0;
const RECV_WR_ID: u64 = 1;

#[allow(clippy::while_let_on_iterator)]
fn main() {
    let args = Args::parse();

    let device_list = DeviceList::new().expect("Failed to get IB devices list");
    let device = match args.ib_dev {
        Some(ib_dev) => device_list
            .iter()
            .find(|dev| dev.name().eq(&ib_dev))
            .unwrap_or_else(|| panic!("IB device {ib_dev} not found")),
        None => device_list.iter().next().expect("No IB device found"),
    };

    let context = device
        .open()
        .unwrap_or_else(|_| panic!("Couldn't get context for {}", device.name()));

    let protection_domain = context
        .alloc_pd()
        .unwrap_or_else(|_| panic!("Couldn't allocate PD"));

    let gid = context
        .query_gid(args.ib_port, args.gid_idx.into())
        .unwrap();
    let psn = rand::random::<u32>() & 0xFFFFFF;

    let mut completion_queue_builder = context.create_cq_builder();

    let completion_queue = completion_queue_builder
        .setup_cqe(args.rx_depth + 1)
        .build_ex()
        .unwrap();

    let completion_queue_handle = GenericCompletionQueue::from(Arc::clone(&completion_queue));

    let mut queue_pair_builder = protection_domain.create_qp_builder();

    let mut queue_pair = queue_pair_builder
        .setup_max_inline_data(128)
        .setup_send_cq(completion_queue_handle.clone())
        .setup_recv_cq(completion_queue_handle)
        .setup_max_send_wr(1)
        .setup_max_recv_wr(args.rx_depth)
        .build()
        // .build_ex()
        .expect("Couldn't create QP");

    let mut attr = QueuePairAttribute::new();
    attr.setup_state(QueuePairState::Init)
        .setup_pkey_index(0)
        .setup_port(args.ib_port)
        .setup_access_flags(AccessFlags::LocalWrite | AccessFlags::RemoteWrite);
    queue_pair.modify(&attr).unwrap();

    println!(
        " local address: QPN {:#06x}, PSN {psn:#08x}, GID {gid}",
        queue_pair.qp_number()
    );

    let mut stream = {
        let server_addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, args.port));
        let listener = TcpListener::bind(server_addr).unwrap();
        let (stream, _peer_addr) = listener.accept().unwrap();
        stream
    };

    let mut msg_buf = Vec::new();
    let remote_context = receive_handshake(&mut stream, &mut msg_buf).unwrap();
    println!(
        "remote address: QPN {:#06x}, PSN {:#08x}, GID {}",
        remote_context.qp_number, remote_context.packet_seq_number, remote_context.gid,
    );

    let local_context = Handshake {
        lid: 1,
        qp_number: queue_pair.qp_number(),
        packet_seq_number: psn,
        gid,
    };
    send_handshake(&mut stream, &local_context).unwrap();

    let mut attr = QueuePairAttribute::new();
    attr.setup_state(QueuePairState::ReadyToReceive)
        .setup_path_mtu(MTU)
        .setup_dest_qp_num(remote_context.qp_number)
        .setup_rq_psn(psn)
        .setup_max_dest_read_atomic(0)
        .setup_min_rnr_timer(0);

    // setup address vector
    let mut ah_attr = AddressHandleAttribute::new();

    ah_attr
        .setup_dest_lid(1)
        .setup_port(args.ib_port)
        .setup_grh_src_gid_index(args.gid_idx)
        .setup_grh_dest_gid(&remote_context.gid)
        .setup_grh_hop_limit(1);
    attr.setup_address_vector(&ah_attr);
    queue_pair.modify(&attr).unwrap();

    let mut attr = QueuePairAttribute::new();
    attr.setup_state(QueuePairState::ReadyToSend)
        .setup_sq_psn(remote_context.packet_seq_number)
        .setup_timeout(12)
        .setup_retry_cnt(7)
        .setup_rnr_retry(7)
        .setup_max_read_atomic(0);

    queue_pair.modify(&attr).unwrap();

    let mut send_data: Vec<u32> = vec![1; 1024];
    let send_memory_region = unsafe {
        protection_domain
            .reg_mr(
                send_data.as_ptr() as _,
                send_data.len() * size_of::<u32>(),
                AccessFlags::LocalWrite | AccessFlags::RemoteWrite,
            )
            .unwrap_or_else(|_| panic!("Couldn't register send MR"))
    };

    let mut guard = queue_pair.start_post_recv();

    let recv_handle = guard.construct_wr(RECV_WR_ID);

    let mut recv_data: Vec<u32> = vec![0; 1024];
    let recv_memory_region = unsafe {
        protection_domain
            .reg_mr(
                recv_data.as_ptr() as _,
                recv_data.len() * size_of::<u32>(),
                AccessFlags::LocalWrite | AccessFlags::RemoteWrite,
            )
            .unwrap_or_else(|_| panic!("Couldn't register recv MR"))
    };
    unsafe {
        recv_handle.setup_sge(
            recv_memory_region.lkey(),
            recv_data.as_mut_ptr() as _,
            1024 * size_of::<u32>() as u32,
        );
    };

    guard.post().unwrap();

    'outer: loop {
        match completion_queue.start_poll() {
            Ok(mut poller) => {
                while let Some(wc) = poller.next() {
                    if wc.status() != WorkCompletionStatus::Success as u32 {
                        panic!(
                            "Failed status {:#?} ({}) for wr_id {}",
                            Into::<WorkCompletionStatus>::into(wc.status()),
                            wc.status(),
                            wc.wr_id()
                        );
                    }
                    match wc.wr_id() {
                        SEND_WR_ID => {}
                        RECV_WR_ID => {
                            send_data.iter_mut().zip(recv_data).for_each(|(s, r)| {
                                *s = r * 2;
                            });
                            send_rdma(
                                &mut queue_pair,
                                &send_memory_region,
                                send_data.as_ptr(),
                                (send_data.len() * size_of::<u32>()) as u32,
                            );
                            // dbg!(&recv_data);
                            break 'outer;
                        }
                        _ => {
                            panic!("Unknown error!");
                        }
                    }
                }
            }
            Err(_) => {
                continue;
            }
        }
    }
}

fn send_rdma(
    queue_pair: &mut BasicQueuePair,
    send_memory_region: &Arc<MemoryRegion>,
    data_address: *const u32,
    data_length: u32,
) {
    let mut guard = queue_pair.start_post_send();

    let send_handle = guard
        .construct_wr(SEND_WR_ID, WorkRequestFlags::Signaled)
        .setup_send();

    unsafe {
        send_handle.setup_sge(send_memory_region.lkey(), data_address as _, data_length);
    }

    guard.post().unwrap();
}

fn send_handshake(stream: &mut TcpStream, dest: &Handshake) -> Result<(), Error> {
    let msg_buf = to_allocvec(dest).unwrap();
    let size = msg_buf.len().to_be_bytes();
    stream.write_all(&size)?;
    stream.write_all(&msg_buf)?;
    stream.flush()?;

    Ok(())
}
fn receive_handshake(stream: &mut TcpStream, msg_buf: &mut Vec<u8>) -> Result<Handshake, Error> {
    let mut size = usize::to_be_bytes(0);
    stream.read_exact(&mut size)?;
    msg_buf.clear();
    msg_buf.resize(usize::from_be_bytes(size), 0);
    stream.read_exact(&mut *msg_buf)?;
    let dest: Handshake = from_bytes(msg_buf).unwrap();

    Ok(dest)
}
