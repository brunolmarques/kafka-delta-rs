// Monitoring and instrumentation

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::MetricExporter;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering}; 

use crate::config::MonitoringConfig;
use crate::handlers::{AppError, AppResult, MonitoringError};


// Add a static variable to store the meter provider.
static METER_PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();
static SHUTDOWN_CALLED: AtomicBool = AtomicBool::new(false); // added static flag

/// A handle fo r the telemetry system. Store references to the meter, counters, histograms, etc.
#[derive(Clone)]
pub struct Monitoring {
    meter: Meter,
    messages_read_counter: Counter<u64>,
    message_size_counter: Counter<u64>,
    kafka_commit_counter: Counter<u64>,
    offset_lag_gauge: UpDownCounter<i64>,
    dead_letters_counter: Counter<u64>,
    delta_write_counter: Counter<u64>,
    delta_flush_histogram: Histogram<f64>,
}

impl Monitoring {
    /// Initialize the telemetry pipeline and create metric instruments.
    /// Call this once at the start of the application (in `main`).
    pub fn init(config: &MonitoringConfig) -> AppResult<Self> {
        if !config.enabled {
            // If disabled, return a NO_OP handle or a handle that does nothing.
            return Ok(Self::no_op());
        }

        // Build a resource describing this application/process.
        fn get_resource(config: &MonitoringConfig) -> Resource {
            static RESOURCE: OnceLock<Resource> = OnceLock::new();
            RESOURCE
                .get_or_init(|| {
                    Resource::builder()
                        .with_service_name(config.service_name.clone())
                        .build()
                })
                .clone()
        }

        // Set up the OTLP metrics exporter if an endpoint is provided.
        let metrics_exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .build()
            .map_err(|e| {
                log::error!("Failed to create metric exporter: {}", e);
                AppError::Monitoring(MonitoringError::ExporterError(format!(
                    "Failed to create metric exporter: {}",
                    e
                )))
            })?;

        // Create a PeriodicReader that automatically exports metrics on an interval.
        // By default, it flushes every 60 seconds (customizable).
        let reader = PeriodicReader::builder(metrics_exporter).build();

        // Create a meter provider with the OTLP Metric exporter
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(get_resource(config))
            .with_reader(reader)
            .build();

        // Store the meter provider in the static variable.
        let _ = METER_PROVIDER.set(meter_provider.clone());

        // Set the global meter provider to the one created.
        global::set_meter_provider(meter_provider.clone());

        // Acquire the global meter
        let service_name_static: &'static str =
            Box::leak(config.service_name.clone().into_boxed_str());
        let meter = global::meter(service_name_static);

        // Create the instruments we want to record with.
        let messages_read_counter = meter
            .u64_counter("kafka_messages_read")
            .with_description("Number of messages read from Kafka topics")
            .build();

        let message_size_counter = meter
            .u64_counter("kafka_messages_size_bytes")
            .with_description("Total size in bytes of messages read from Kafka")
            .build();

        let kafka_commit_counter = meter
            .u64_counter("kafka_commits")
            .with_description("Number of commits performed on Kafka")
            .build();

        let offset_lag_gauge = meter
            .i64_up_down_counter("kafka_offset_lag")
            .with_description("Tracks the offset lag behind the latest committed offset")
            .build();

        let dead_letters_counter = meter
            .u64_counter("kafka_dead_letters")
            .with_description("Number of messages sent to dead letter topic")
            .build();

        let delta_write_counter = meter
            .u64_counter("delta_messages_written")
            .with_description("Number of messages/records written to Delta table")
            .build();

        let delta_flush_histogram = meter
            .f64_histogram("delta_flush_time_seconds")
            .with_description("Histogram of flush durations (seconds) to Delta table")
            .build();

        Ok(Self {
            meter,
            messages_read_counter,
            message_size_counter,
            kafka_commit_counter,
            offset_lag_gauge,
            dead_letters_counter,
            delta_write_counter,
            delta_flush_histogram,
        })
    }

    /// Construct a NO_OP monitor if monitoring is disabled, or if initialization fails.
    fn no_op() -> Self {
        let meter = global::meter("no-op-meter");
        let no_op_counter = meter.u64_counter("no-op").build();
        let no_op_up_down = meter.i64_up_down_counter("no-op").build();
        let no_op_histogram = meter.f64_histogram("no-op").build();

        Self {
            meter,
            messages_read_counter: no_op_counter.clone(),
            message_size_counter: no_op_counter.clone(),
            kafka_commit_counter: no_op_counter.clone(),
            offset_lag_gauge: no_op_up_down,
            dead_letters_counter: no_op_counter.clone(),
            delta_write_counter: no_op_counter,
            delta_flush_histogram: no_op_histogram,
        }
    }

    /// Record number of messages read.
    pub fn record_kafka_messages_read(&self, count: u64) {
        self.messages_read_counter.add(count, &[KeyValue::new(
            "kafka_messages_read",
            "messages_count",
        )]);
    }

    /// Record total size of messages read from Kafka.
    pub fn record_kafka_messages_size(&self, size_bytes: u64) {
        self.message_size_counter.add(size_bytes, &[KeyValue::new(
            "kafka_messages_size",
            "messages_size_bytes",
        )]);
    }

    /// Record a Kafka commit event.
    pub fn record_kafka_commit(&self) {
        self.kafka_commit_counter
            .add(1, &[KeyValue::new("kafka_commits", "commits_count")]);
    }

    /// Set offset lag gauge. This is: latest_available_offset - current_committed_offset.
    pub fn set_kafka_offset_lag(&self, lag: i64) {
        self.offset_lag_gauge
            .add(lag, &[KeyValue::new("kafka_offset_lag", "offset_value")]);
    }

    /// Record number of messages sent to dead letter topic.
    pub fn record_dead_letters(&self, count: u64) {
        self.dead_letters_counter.add(count, &[KeyValue::new(
            "kafka_dead_letters",
            "dead_letters_count",
        )]);
    }

    /// Record number of messages/records written to Delta.
    pub fn record_delta_write(&self, count: u64) {
        self.delta_write_counter.add(count, &[KeyValue::new(
            "delta_messages_written",
            "messages_count",
        )]);
    }

    /// Observe flush time for writing to Delta.
    pub fn observe_delta_flush_time(&self, seconds: f64) {
        self.delta_flush_histogram.record(seconds, &[KeyValue::new(
            "delta_flush_time_seconds",
            "flush_time_seconds",
        )]);
    }

    /// Shut down the global meter provider, ensuring final metrics are exported.
    /// Useful if your application is about to exit and you want to ensure everything is sent.
    pub fn shutdown() {
        if let Some(provider) = METER_PROVIDER.get() {
            match provider.shutdown() {
                Ok(()) => {
                    SHUTDOWN_CALLED.store(true, Ordering::SeqCst); // mark shutdown as called
                }
                Err(e) => {
                    log::error!("Failed to shut down metrics: {}", e);
                    AppError::Monitoring(MonitoringError::ShutdownError(format!(
                        "Failed to shut down metrics: {}",
                        e
                    )));
                }
            }
        }
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;
    use std::sync::atomic::Ordering; // added for testing

    // Define a helper to create a no-op config.
    fn dummy_no_op_config() -> crate::config::MonitoringConfig {
        crate::config::MonitoringConfig {
            enabled: false,
            endpoint: "".to_string(),
            service_name: "test-service".to_string(),
        }
    }

    // Updated helper to create an enabled config with a valid endpoint.
    fn dummy_enabled_config() -> crate::config::MonitoringConfig {
        crate::config::MonitoringConfig {
            enabled: true,
            endpoint: "http://localhost:4318".to_string(), // valid dummy endpoint
            service_name: "test-service".to_string(),
        }
    }

    #[test]
    fn test_init_no_op() {
        let config = dummy_no_op_config();
        let monitor = Monitoring::init(&config).expect("init failed");
        // Call record functions to verify they do not panic.
        monitor.record_kafka_messages_read(10);
        monitor.record_kafka_messages_size(1024);
        monitor.record_kafka_commit();
        monitor.set_kafka_offset_lag(5);
        monitor.record_delta_write(3);
        monitor.observe_delta_flush_time(0.5);
    }

    #[test]
    fn test_init_enabled() {
        let config = dummy_enabled_config();
        let monitor = Monitoring::init(&config).expect("init failed");
        // Call record functions to verify normal operation.
        monitor.record_kafka_messages_read(20);
        monitor.record_kafka_messages_size(2048);
        monitor.record_kafka_commit();
        monitor.set_kafka_offset_lag(-3);
        monitor.record_delta_write(5);
        monitor.observe_delta_flush_time(1.2);
        // Ensure the static meter provider is set.
        assert!(METER_PROVIDER.get().is_some(), "Meter provider not set");
    }

    #[test]
    fn test_shutdown() {
        // Initialize an enabled monitor to set the static meter provider.
        let _ = Monitoring::init(&dummy_enabled_config()).expect("init failed");
        // Call shutdown and ensure it does not panic.
        Monitoring::shutdown();
        assert!(
            SHUTDOWN_CALLED.load(Ordering::SeqCst),
            "Shutdown was not successful"
        );
    }
}
