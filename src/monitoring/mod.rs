use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

// Implements monitoring endpoints exposing Prometheus metrics
// using the prometheus crate and exposing a HTTP endpoint via Tokio.
pub fn init_monitoring(port: u16) {
    // Set up Prometheus registry and metrics.
    let mut registry = Registry::default();
    let events_processed: Counter = Counter::default();
    let processed_event_size: Counter = Counter::default();

    registry.register(
        "events_processed",
        "Total number of events processed",
        events_processed,
    );

    registry.register(
        "processed_event_size",
        "Total size of processed events",
        processed_event_size,
    );

    let reg = Arc::new(registry);
    let addr = format!("0.0.0.0:{}", port);

    spawn(async move {
        let listener = TcpListener::bind(&addr)
            .await
            .expect("Failed to bind metrics server");
        log::info!("Prometheus metrics server running on {}", addr);

        loop {
            let (mut socket, _) = listener
                .accept()
                .await
                .expect("Failed to accept connection");

            let mut buffer = String::new();
            encode(&mut buffer, &*reg).unwrap();

            // Split response into header and body.
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                buffer.len()
            );
            socket
                .write_all(header.as_bytes())
                .await
                .expect("Failed to write header");
            socket
                .write_all(&buffer.into_bytes())
                .await
                .expect("Failed to write body");
        }
    });
    log::info!("Monitoring initialized on port: {}", port);
}

//---------------------------------------- Tests ----------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_monitoring_init() {
        // Initialize monitoring on port 9090.
        init_monitoring(9090);
        sleep(Duration::from_secs(1)).await; // allow server to start

        // Connect to the metrics endpoint and check for registered metric.
        let mut stream = TcpStream::connect("127.0.0.1:9090")
            .await
            .expect("Failed to connect");
        let mut buffer = vec![0; 1024];
        let n = stream.read(&mut buffer).await.expect("Failed to read data");
        let response = String::from_utf8_lossy(&buffer[..n]);
        assert!(response.contains("events_processed"));
    }
}
