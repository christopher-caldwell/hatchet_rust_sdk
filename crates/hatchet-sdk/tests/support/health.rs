use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

// Explicit source ports also work on development hosts whose automatic source
// port range is exhausted by a VPN. No system or VPN settings are changed.
pub async fn connect(port: u16) -> TcpStream {
    static NEXT: AtomicU16 = AtomicU16::new(20000);
    for _ in 20000..49000 {
        let source = 20000 + NEXT.fetch_add(1, Ordering::Relaxed) % 29000;
        let socket = TcpSocket::new_v4().unwrap();
        if socket
            .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, source)))
            .is_ok()
        {
            if let Ok(stream) = socket
                .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
                .await
            {
                return stream;
            }
        }
    }
    panic!("cannot connect to health server on {port}");
}

pub async fn request(port: u16, request: &[u8]) -> String {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut stream = connect(port).await;
        stream.write_all(request).await.unwrap();
        let mut response = String::new();
        if let Err(error) = stream.read_to_string(&mut response).await {
            // Rejecting oversized headers may close with unread request bytes,
            // producing a TCP reset after the complete HTTP error response.
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
            let (headers, body) = response.split_once("\r\n\r\n").expect("complete response");
            let length = headers
                .lines()
                .find_map(|line| line.strip_prefix("Content-Length: "))
                .unwrap()
                .parse::<usize>()
                .unwrap();
            assert_eq!(body.len(), length);
        }
        response
    })
    .await
    .unwrap()
}
