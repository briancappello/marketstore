#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use marketstore_rust_client::models::*;

    #[test]
    fn test_ohlcv_data_creation() {
        let data = OHLCVData {
            epoch: 1640995200,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1000.0,
        };

        assert_eq!(data.epoch, 1640995200);
        assert_eq!(data.open, 100.0);
        assert_eq!(data.high, 101.0);
        assert_eq!(data.low, 99.0);
        assert_eq!(data.close, 100.5);
        assert_eq!(data.volume, 1000.0);
    }

    #[test]
    fn test_ohlcv_data_serialization() {
        let data = OHLCVData {
            epoch: 1640995200,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1000.0,
        };

        let json = serde_json::to_string(&data).unwrap();
        let deserialized: OHLCVData = serde_json::from_str(&json).unwrap();

        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_query_request_builder() {
        let request = QueryRequest::builder()
            .symbol("BTCUSDT")
            .timeframe("1Min")
            .attr_group("OHLCV")
            .start_time(1640995200)
            .end_time(1640995260)
            .limit(100)
            .build()
            .unwrap();

        assert_eq!(request.destination, "BTCUSDT/1Min/OHLCV");
        assert_eq!(request.epoch_start, Some(1640995200));
        assert_eq!(request.epoch_end, Some(1640995260));
        assert_eq!(request.limit_record_count, Some(100));
    }

    #[test]
    fn test_query_request_builder_without_optional_fields() {
        let request = QueryRequest::builder()
            .symbol("BTCUSDT")
            .timeframe("1Min")
            .attr_group("OHLCV")
            .build()
            .unwrap();

        assert_eq!(request.destination, "BTCUSDT/1Min/OHLCV");
        assert_eq!(request.epoch_start, None);
        assert_eq!(request.epoch_end, None);
        assert_eq!(request.limit_record_count, None);
    }

    #[test]
    fn test_query_request_builder_missing_required_fields() {
        let result = QueryRequest::builder()
            .symbol("BTCUSDT")
            .timeframe("1Min")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_stream_subscription() {
        let subscription = StreamSubscription::new()
            .add_stream("BTCUSDT/1Min/OHLCV")
            .add_stream("ETHUSDT/1Min/OHLCV");

        assert_eq!(subscription.tbks.len(), 2);
        assert!(subscription
            .tbks
            .contains(&"BTCUSDT/1Min/OHLCV".to_string()));
        assert!(subscription
            .tbks
            .contains(&"ETHUSDT/1Min/OHLCV".to_string()));
    }

    #[test]
    fn test_stream_subscription_empty() {
        let subscription = StreamSubscription::new();
        assert_eq!(subscription.tbks.len(), 0);
    }

    #[test]
    fn test_symbol_format_enum() {
        assert_eq!(SymbolFormat::Symbol as i32, 0);
        assert_eq!(SymbolFormat::TimeBucketKey as i32, 1);
    }

    #[test]
    fn test_data_shape_creation() {
        let data_shape = DataShape {
            name: "Price".to_string(),
            data_type: "f4".to_string(),
        };

        assert_eq!(data_shape.name, "Price");
        assert_eq!(data_shape.data_type, "f4");
    }

    #[test]
    fn test_write_request_creation() {
        let data = vec![OHLCVData {
            epoch: 1640995200,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1000.0,
        }];

        let request = WriteRequest::new("BTCUSDT", "1Min", "OHLCV", data);

        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.timeframe, "1Min");
        assert_eq!(request.attr_group, "OHLCV");
        assert_eq!(request.data.len(), 1);
    }
}
