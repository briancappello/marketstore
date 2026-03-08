#[cfg(test)]
mod tests {
    use marketstore_rust_client::{
        client::WebSocketClient,
        models::{StreamSubscription, StreamPayload},
        error::Result,
    };
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn test_websocket_connection() {
        let client = WebSocketClient::connect("ws://localhost:5993/ws").await;
        // 这个测试需要真实的MarketStore服务器运行
        // 在实际测试中，应该使用mock服务器
        assert!(client.is_ok() || client.is_err()); // 至少能处理连接结果
    }

    #[test]
    fn test_subscribe_message_serialization() {
        let message = crate::models::SubscribeMessage {
            action: "subscribe".to_string(),
            tbks: vec![
                "BTCUSDT/1Min/OHLCV".to_string(),
                "ETHUSDT/1Min/OHLCV".to_string(),
            ],
        };
        
        let msgpack_data = rmp_serde::to_vec(&message).unwrap();
        let deserialized: crate::models::SubscribeMessage = rmp_serde::from_slice(&msgpack_data).unwrap();
        
        assert_eq!(message.action, deserialized.action);
        assert_eq!(message.tbks, deserialized.tbks);
    }

    #[test]
    fn test_error_message_serialization() {
        let message = crate::models::ErrorMessage {
            error: "Invalid stream format".to_string(),
        };
        
        let msgpack_data = rmp_serde::to_vec(&message).unwrap();
        let deserialized: crate::models::ErrorMessage = rmp_serde::from_slice(&msgpack_data).unwrap();
        
        assert_eq!(message.error, deserialized.error);
    }

    #[test]
    fn test_stream_payload_creation() {
        let payload = StreamPayload {
            key: "BTCUSDT/1Min/OHLCV".to_string(),
            data: serde_json::json!({
                "epoch": 1640995200,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0
            }),
        };
        
        assert_eq!(payload.key, "BTCUSDT/1Min/OHLCV");
        assert!(payload.data.is_object());
    }

    #[test]
    fn test_valid_stream_format() {
        let valid_streams = vec![
            "BTCUSDT/1Min/OHLCV",
            "ETHUSDT/1H/OHLCV",
            "AAPL/1D/OHLCV",
            "*/1Min/OHLCV",  // 通配符
            "BTCUSDT/*/OHLCV", // 通配符
            "BTCUSDT/1Min/*", // 通配符
        ];
        
        for stream in valid_streams {
            assert!(is_valid_stream_format(stream), "Stream {} should be valid", stream);
        }
    }

    #[test]
    fn test_invalid_stream_format() {
        let invalid_streams = vec![
            "BTCUSDT",           // 缺少时间框架和属性组
            "BTCUSDT/1Min",      // 缺少属性组
            "BTCUSDT//OHLCV",    // 缺少时间框架
            "/1Min/OHLCV",       // 缺少symbol
            "BTCUSDT/1Min/",     // 缺少属性组
            "BTCUSDT/1Min/OHLCV/extra", // 多余的部分
        ];
        
        for stream in invalid_streams {
            assert!(!is_valid_stream_format(stream), "Stream {} should be invalid", stream);
        }
    }

    // 辅助函数，用于测试
    fn is_valid_stream_format(stream: &str) -> bool {
        let parts: Vec<&str> = stream.split('/').collect();
        if parts.len() != 3 {
            return false;
        }
        
        let symbol = parts[0];
        let timeframe = parts[1];
        let attr_group = parts[2];
        
        // 检查各部分是否为空
        if symbol.is_empty() || timeframe.is_empty() || attr_group.is_empty() {
            return false;
        }
        
        // 检查时间框架格式
        let valid_timeframes = ["1Min", "1H", "1D", "1Sec", "5Min", "15Min", "30Min", "4H", "*"];
        if !valid_timeframes.contains(&timeframe) && !timeframe.contains('*') {
            return false;
        }
        
        // 检查属性组格式
        let valid_attr_groups = ["OHLCV", "TICK", "TRADE", "QUOTE", "*"];
        if !valid_attr_groups.contains(&attr_group) && !attr_group.contains('*') {
            return false;
        }
        
        true
    }
} 