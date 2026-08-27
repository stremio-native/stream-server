use std::collections::HashMap;
use std::sync::Arc;

use libtorrent_sys::AlertInfo;
use parking_lot::Mutex;
use tokio::sync::broadcast;

const ALERT_CHANNEL_CAPACITY: usize = 512;
type PieceResult = Result<Arc<Vec<u8>>, Arc<str>>;
type PieceKey = (String, i32);

/// Fan-out for torrent alerts consumed by latency-sensitive playback operations.
///
/// Callers subscribe before submitting an asynchronous libtorrent operation, so
/// an acknowledgement cannot be lost between the native call and the wait.
pub(crate) struct LibtorrentAlertHub {
    senders: Mutex<HashMap<String, broadcast::Sender<AlertInfo>>>,
    piece_senders: Mutex<HashMap<PieceKey, broadcast::Sender<PieceResult>>>,
}

impl LibtorrentAlertHub {
    pub(crate) fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            piece_senders: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn subscribe(&self, info_hash: &str) -> broadcast::Receiver<AlertInfo> {
        let info_hash = info_hash.to_lowercase();
        let mut senders = self.senders.lock();
        senders
            .entry(info_hash)
            .or_insert_with(|| broadcast::channel(ALERT_CHANNEL_CAPACITY).0)
            .subscribe()
    }

    pub(crate) fn dispatch(&self, alert: &mut AlertInfo) {
        if alert.info_hash.is_empty() {
            return;
        }

        let info_hash = alert.info_hash.to_lowercase();
        if alert.alert_type == libtorrent_sys::get_read_piece_alert_type() && alert.piece_index >= 0
        {
            let result = if alert.piece_data.is_empty() {
                Err(Arc::<str>::from(format!(
                    "libtorrent returned no data for piece {}",
                    alert.piece_index
                )))
            } else {
                Ok(Arc::new(std::mem::take(&mut alert.piece_data)))
            };
            if let Some(sender) = self
                .piece_senders
                .lock()
                .remove(&(info_hash.clone(), alert.piece_index))
            {
                let _ = sender.send(result);
            }
        } else if alert.alert_type == libtorrent_sys::get_file_error_alert_type() {
            let error = Arc::<str>::from(alert.message.clone());
            let failed = {
                let mut senders = self.piece_senders.lock();
                let keys = senders
                    .keys()
                    .filter(|(hash, _)| hash == &info_hash)
                    .cloned()
                    .collect::<Vec<_>>();
                keys.into_iter()
                    .filter_map(|key| senders.remove(&key))
                    .collect::<Vec<_>>()
            };
            for sender in failed {
                let _ = sender.send(Err(error.clone()));
            }
        }

        let sender = {
            let mut senders = self.senders.lock();
            senders
                .entry(info_hash)
                .or_insert_with(|| broadcast::channel(ALERT_CHANNEL_CAPACITY).0)
                .clone()
        };
        let _ = sender.send(alert.clone());
    }

    pub(crate) fn remove_torrent(&self, info_hash: &str) {
        let info_hash = info_hash.to_lowercase();
        self.senders.lock().remove(&info_hash);
        self.piece_senders
            .lock()
            .retain(|(hash, _), _| hash != &info_hash);
    }

    pub(crate) async fn read_piece(
        &self, mut handle: libtorrent_sys::LibtorrentHandle, info_hash: &str, piece: i32,
        piece_priority: i32, deadline_ms: i32, timeout: std::time::Duration,
    ) -> std::io::Result<Arc<Vec<u8>>> {
        let key = (info_hash.to_lowercase(), piece);
        let (mut receiver, submit_native_request) = {
            let mut senders = self.piece_senders.lock();
            if let Some(sender) = senders.get(&key) {
                (sender.subscribe(), false)
            } else {
                let (sender, receiver) = broadcast::channel(1);
                senders.insert(key.clone(), sender);
                (receiver, true)
            }
        };

        if submit_native_request {
            let already_verified = handle.have_piece(piece);
            if already_verified {
                if let Err(error) = handle.read_piece(piece)
                    && let Some(sender) = self.piece_senders.lock().remove(&key)
                {
                    let _ = sender.send(Err(Arc::<str>::from(error.to_string())));
                }
            } else {
                handle.set_piece_priority(piece, piece_priority);
                handle.set_piece_deadline_with_alert(piece, deadline_ms, true);
            }
            tracing::debug!(
                info_hash = %key.0,
                piece,
                already_verified,
                "submitted coalesced libtorrent piece-byte request"
            );
        }

        let result = tokio::time::timeout(timeout, receiver.recv()).await;
        match result {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(error))) => Err(std::io::Error::other(error.to_string())),
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => Err(std::io::Error::other(format!(
                "piece broker lagged while reading piece {piece}"
            ))),
            Ok(Err(broadcast::error::RecvError::Closed)) => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "libtorrent piece broker closed",
            )),
            Err(_) => {
                let mut senders = self.piece_senders.lock();
                if senders
                    .get(&key)
                    .is_some_and(|sender| sender.receiver_count() <= 1)
                {
                    senders.remove(&key);
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("timed out reading torrent piece {piece}"),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatch_routes_alert_to_matching_torrent() {
        let hub = LibtorrentAlertHub::new();
        let mut receiver = hub.subscribe("ABC");
        let mut alert = AlertInfo {
            alert_type: 97,
            category: 0,
            what: String::new(),
            message: String::new(),
            timestamp: 0,
            info_hash: "abc".to_string(),
            piece_index: -1,
            piece_data: Vec::new(),
        };
        hub.dispatch(&mut alert);

        let alert = receiver.recv().await.expect("matching alert");
        assert_eq!(alert.alert_type, 97);
    }

    #[tokio::test]
    async fn read_piece_alert_moves_bytes_into_shared_broker_result() {
        let hub = LibtorrentAlertHub::new();
        let (sender, mut receiver) = broadcast::channel(1);
        hub.piece_senders
            .lock()
            .insert(("abc".to_string(), 7), sender);
        let mut alert = AlertInfo {
            alert_type: libtorrent_sys::get_read_piece_alert_type(),
            category: 0,
            what: String::new(),
            message: String::new(),
            timestamp: 0,
            info_hash: "ABC".to_string(),
            piece_index: 7,
            piece_data: vec![1, 2, 3, 4],
        };

        hub.dispatch(&mut alert);

        let data = receiver
            .recv()
            .await
            .expect("broker result")
            .expect("bytes");
        assert_eq!(data.as_slice(), [1, 2, 3, 4]);
        assert!(alert.piece_data.is_empty());
        assert!(hub.piece_senders.lock().is_empty());
    }
}
