//! Message transports for one Broadcast session. Framing belongs to the adapter;
//! channel authorization, publication and lifecycle belong to the session.
use std::future::Future;

pub use claviron_orbit::link::application::Message;
use claviron_orbit::link::application::{MessageReader, write_message};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// The same message limit used by the native WebSocket service.
pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024;

/// One session's messages, whatever carries them.
///
/// Used generically (`S: MessageStream`), never as `dyn`, so the futures are
/// returned as they are rather than boxed: a box per message would be an
/// allocation on the path every WebSocket message takes.
pub trait MessageStream: Send {
    /// Reads must retain partially received messages when cancelled by select.
    fn receive(
        &mut self
    ) -> impl Future<Output = crate::Result<Option<Message>>> + Send;
    fn send(
        &mut self,
        message: Message,
    ) -> impl Future<Output = crate::Result<()>> + Send;
}

/// A WebSocket whichever door it came through: on a socket or over Orbit the
/// transport has joined fragments, answered pings and answered the peer's close.
#[cfg(feature = "websocket")]
pub struct SocketStream(pub(crate) claviron_transport::websocket::WebSocket);

#[cfg(feature = "websocket")]
impl MessageStream for SocketStream {
    async fn receive(&mut self) -> crate::Result<Option<Message>> {
        Ok(self.0.recv().await)
    }

    async fn send(
        &mut self,
        message: Message,
    ) -> crate::Result<()> {
        // A peer's close has its answer already, and these two never go on a wire.
        if matches!(message, Message::Close { code: 1005 | 1006, .. }) {
            return Ok(());
        }
        Ok(self.0.send(message).await?)
    }
}

/// A reliable WebTransport bidirectional stream carrying the existing
/// Text/Binary/Close records. The accepting H3 connection owns the IO.
pub struct WebTransportMessageStream<S> {
    stream: S,
    records: MessageReader,
}

impl<S> WebTransportMessageStream<S> {
    pub fn new(stream: S) -> Self {
        Self { stream, records: MessageReader::with_limit(MAX_MESSAGE_SIZE) }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> MessageStream
    for WebTransportMessageStream<S>
{
    async fn receive(&mut self) -> crate::Result<Option<Message>> {
        Ok(self.records.read(&mut self.stream).await?)
    }

    async fn send(
        &mut self,
        message: Message,
    ) -> crate::Result<()> {
        write_message(&mut self.stream, &message).await?;
        self.stream.flush().await?;
        if matches!(message, Message::Close { .. }) {
            self.stream.shutdown().await?;
        }
        Ok(())
    }
}
