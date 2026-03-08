use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;
use futures::{SinkExt, StreamExt};
use crate::{
    error::{Result, MarketStoreError},
    models::{StreamSubscription, StreamPayload, SubscribeMessage, ErrorMessage},
};

pub struct WebSocketClient {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WebSocketClient {
    pub async fn connect(url: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(url).await?;
        
        Ok(Self { stream: ws_stream })
    }
    
    pub async fn subscribe(&mut self, subscription: StreamSubscription) -> Result<()> {
        let message = SubscribeMessage {
            action: "subscribe".to_string(),
            tbks: subscription.tbks,
        };
        
        let msgpack_data = rmp_serde::to_vec(&message)?;
        self.stream.send(tokio_tungstenite::tungstenite::Message::Binary(msgpack_data.into())).await?;
        
        Ok(())
    }
    
    /// 按照RFC 6455 § 5.5.1 实现正确的WebSocket关闭握手
    async fn perform_close_handshake(&mut self) -> Result<()> {
        // 1. 发送关闭帧 (Close Frame)
        let close_frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
            reason: "Normal closure".into(),
        };
        
        tracing::debug!("Sending close frame with code: {:?}", close_frame.code);
        self.stream.send(tokio_tungstenite::tungstenite::Message::Close(Some(close_frame))).await?;
        
        // 2. 等待服务器的关闭帧响应 (RFC 6455 § 5.5.1)
        // 设置超时，避免无限等待
        let timeout = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            self.stream.next()
        );
        
        match timeout.await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(Some(frame))))) => {
                tracing::debug!("Received close frame from server: code={:?}, reason={:?}", 
                    frame.code, frame.reason);
                Ok(())
            }
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(None)))) => {
                tracing::debug!("Received close frame from server (no details)");
                Ok(())
            }
            Ok(Some(Ok(msg))) => {
                tracing::warn!("Received unexpected message while waiting for close: {:?}", msg);
                Ok(())
            }
            Ok(Some(Err(e))) => {
                tracing::warn!("Error while waiting for close response: {}", e);
                Err(MarketStoreError::WebSocket(e.to_string()))
            }
            Ok(None) => {
                tracing::debug!("Stream ended while waiting for close response");
                Ok(())
            }
            Err(_) => {
                tracing::warn!("Timeout waiting for server close response");
                Ok(())
            }
        }
    }
    
    pub async fn subscribe_with_handler<F>(
        mut self,
        subscription: StreamSubscription,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(StreamPayload) -> Result<()> + Send + 'static,
    {
        self.subscribe(subscription).await?;
        
        while let Some(msg) = self.stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                    // 尝试反序列化为不同类型的消息
                    if let Ok(payload) = rmp_serde::from_slice::<StreamPayload>(&data) {
                        if let Err(e) = handler(payload) {
                            tracing::warn!("Handler error: {}", e);
                        }
                    } else if let Ok(subscribe_msg) = rmp_serde::from_slice::<SubscribeMessage>(&data) {
                        tracing::debug!("Received subscribe message: {:?}", subscribe_msg);
                    } else if let Ok(error_msg) = rmp_serde::from_slice::<ErrorMessage>(&data) {
                        tracing::warn!("Received error message: {}", error_msg.error);
                    } else {
                        tracing::warn!("Failed to deserialize message as any known type");
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    tracing::info!("WebSocket connection closed by server: {:?}", frame);
                    // 服务器发起了关闭，我们需要响应关闭帧
                    if let Some(close_frame) = frame {
                        let response_frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: close_frame.code,
                            reason: close_frame.reason.clone(),
                        };
                        if let Err(e) = self.stream.send(tokio_tungstenite::tungstenite::Message::Close(Some(response_frame))).await {
                            tracing::warn!("Failed to send close response: {}", e);
                        }
                    } else {
                        if let Err(e) = self.stream.send(tokio_tungstenite::tungstenite::Message::Close(None)).await {
                            tracing::warn!("Failed to send close response: {}", e);
                        }
                    }
                    break;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                    if let Err(e) = self.stream.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await {
                        tracing::warn!("Failed to send pong: {}", e);
                        break;
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Pong(_)) => {
                    // 忽略pong消息
                }
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    tracing::debug!("Received text message: {}", text);
                }
                Ok(tokio_tungstenite::tungstenite::Message::Frame(_)) => {
                    // 忽略原始帧
                }
                Err(e) => {
                    tracing::warn!("WebSocket error: {}", e);
                    return Err(MarketStoreError::WebSocket(e.to_string()));
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn subscribe_with_handler_and_cancel<F>(
        mut self,
        subscription: StreamSubscription,
        mut handler: F,
        cancel: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()>
    where
        F: FnMut(StreamPayload) -> Result<()> + Send + 'static,
    {
        self.subscribe(subscription).await?;
        
        let mut cancel = cancel;
        
        loop {
            tokio::select! {
                msg = self.stream.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => {
                            // 尝试反序列化为不同类型的消息
                            if let Ok(payload) = rmp_serde::from_slice::<StreamPayload>(&data) {
                                if let Err(e) = handler(payload) {
                                    tracing::warn!("Handler error: {}", e);
                                }
                            } else if let Ok(subscribe_msg) = rmp_serde::from_slice::<SubscribeMessage>(&data) {
                                tracing::debug!("Received subscribe message: {:?}", subscribe_msg);
                            } else if let Ok(error_msg) = rmp_serde::from_slice::<ErrorMessage>(&data) {
                                tracing::warn!("Received error message: {}", error_msg.error);
                            } else {
                                tracing::warn!("Failed to deserialize message as any known type");
                            }
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame))) => {
                            tracing::info!("WebSocket connection closed by server: {:?}", frame);
                            // 服务器发起了关闭，我们需要响应关闭帧
                            if let Some(close_frame) = frame {
                                let response_frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                    code: close_frame.code,
                                    reason: close_frame.reason.clone(),
                                };
                                if let Err(e) = self.stream.send(tokio_tungstenite::tungstenite::Message::Close(Some(response_frame))).await {
                                    tracing::warn!("Failed to send close response: {}", e);
                                }
                            } else {
                                if let Err(e) = self.stream.send(tokio_tungstenite::tungstenite::Message::Close(None)).await {
                                    tracing::warn!("Failed to send close response: {}", e);
                                }
                            }
                            break;
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                            if let Err(e) = self.stream.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await {
                                tracing::warn!("Failed to send pong: {}", e);
                                break;
                            }
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {
                            // 忽略pong消息
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                            tracing::debug!("Received text message: {}", text);
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Frame(_))) => {
                            // 忽略原始帧
                        }
                        Some(Err(e)) => {
                            tracing::warn!("WebSocket error: {}", e);
                            return Err(MarketStoreError::WebSocket(e.to_string()));
                        }
                        None => {
                            tracing::info!("WebSocket stream ended");
                            break;
                        }
                    }
                }
                _ = &mut cancel => {
                    tracing::info!("Received cancel signal, performing RFC 6455 close handshake");
                    // 按照RFC 6455执行正确的关闭握手
                    if let Err(e) = self.perform_close_handshake().await {
                        tracing::warn!("Close handshake failed: {}", e);
                    }
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn send_ping(&mut self) -> Result<()> {
        self.stream.send(tokio_tungstenite::tungstenite::Message::Ping(vec![].into())).await?;
        Ok(())
    }
    
    /// 按照RFC 6455执行优雅关闭
    pub async fn close(&mut self) -> Result<()> {
        self.perform_close_handshake().await
    }
} 