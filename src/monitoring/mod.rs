use opentelemetry::global;
use opentelemetry::Key;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream, Temporality};
use opentelemetry_sdk::Resource;
use std::error::Error;

// Configuration for the monitoring subsystem; these values should be loaded from the YAML config.
pub struct MonitoringConfig {
    pub otlp_endpoint: String,
    pub service_name: String,
}

// The Monitor struct holds the OpenTelemetry instruments (counters and histograms).
pub struct Monitor {
    pub kafka_msg_counter: opentelemetry::metrics::Counter<u64>,
    pub kafka_byte_counter: opentelemetry::metrics::Counter<u64>,
    pub kafka_commit_counter: opentelemetry::metrics::Counter<u64>,
    pub delta_msg_counter: opentelemetry::metrics::Counter<u64>,
    pub delta_byte_counter: opentelemetry::metrics::Counter<u64>,
    pub delta_flush_histogram: opentelemetry::metrics::Histogram<f64>,
}

impl Monitor {
    // Initializes the OpenTelemetry meter using the provided MonitoringConfig.
    pub fn init(config: &MonitoringConfig) -> Self {
        // Set up an exporter with the connection details from the config.
        let exporter = opentelemetry_stdout::MetricExporterBuilder::default()
            .tonic()
            .with_endpoint(config.endpoint.clone());

        // Build a metrics controller with a basic processor.
        let controller = controllers::basic(processors::factory(
            selectors::simple::Selector::Exact,
            aggregators::cumulative_temporality_selector(),
        ))
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            config.service_name.clone(),
        )]))
        .with_exporter(exporter)
        .build();

        // Obtain a meter instance from the controller.
        let meter = controller.meter("kafka_delta_rust");

        // Create instruments.
        let kafka_msg_counter = meter
            .u64_counter("kafka_messages_total")
            .with_description("Total number of messages read from Kafka")
            .init();

        let kafka_byte_counter = meter
            .u64_counter("kafka_bytes_total")
            .with_description("Total number of bytes read from Kafka")
            .init();

        let kafka_commit_counter = meter
            .u64_counter("kafka_commits_total")
            .with_description("Total number of commit operations from Kafka consumer")
            .init();

        let delta_msg_counter = meter
            .u64_counter("delta_messages_total")
            .with_description("Total number of messages written to Delta table")
            .init();

        let delta_byte_counter = meter
            .u64_counter("delta_bytes_total")
            .with_description("Total number of bytes written to Delta table")
            .init();

        let delta_flush_histogram = meter
            .f64_histogram("delta_flush_duration_seconds")
            .with_description("Processing time (in seconds) for flushing data to the Delta table")
            .init();

        Monitor {
            kafka_msg_counter,
            kafka_byte_counter,
            kafka_commit_counter,
            delta_msg_counter,
            delta_byte_counter,
            delta_flush_histogram,
        }
    }

    // Records Kafka message read metrics.
    pub fn record_kafka_read(&self, num_messages: u64, num_bytes: u64) {
        self.kafka_msg_counter.add(num_messages, &[]);
        self.kafka_byte_counter.add(num_bytes, &[]);
    }

    // Records Kafka commit metrics.
    pub fn record_kafka_commit(&self, commit_count: u64) {
        self.kafka_commit_counter.add(commit_count, &[]);
    }

    // Records metrics for Delta table writes.
    pub fn record_delta_write(&self, num_messages: u64, num_bytes: u64) {
        self.delta_msg_counter.add(num_messages, &[]);
        self.delta_byte_counter.add(num_bytes, &[]);
    }

    // Records the processing time (in seconds) of a Delta table flush operation.
    pub fn record_delta_flush_duration(&self, duration: f64) {
        self.delta_flush_histogram.record(duration, &[]);
    }
}

// ---------------------------------- Tests -----------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_config() -> MonitoringConfig {
        MonitoringConfig {
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "test_service".to_string(),
        }
    }

    #[test]
    fn test_monitor_init_and_record_kafka_read() {
        let monitor = Monitor::init(dummy_config());
        // Verify that record_kafka_read runs without errors.
        monitor.record_kafka_read(10, 256);
        assert!(true);
    }

    #[test]
    fn test_monitor_record_kafka_commit() {
        let monitor = Monitor::init(dummy_config());
        // Verify that record_kafka_commit runs without errors.
        monitor.record_kafka_commit(5);
        assert!(true);
    }

    #[test]
    fn test_monitor_record_delta_write() {
        let monitor = Monitor::init(dummy_config());
        // Verify that record_delta_write runs without errors.
        monitor.record_delta_write(7, 512);
        assert!(true);
    }

    #[test]
    fn test_monitor_record_delta_flush_duration() {
        let monitor = Monitor::init(dummy_config());
        // Verify that record_delta_flush_duration runs without errors.
        monitor.record_delta_flush_duration(1.23);
        assert!(true);
    }
}
