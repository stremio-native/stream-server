//! In-memory piece cache for streaming-first architecture
//!
//! Downloaded pieces go to memory first for immediate streaming,
//! with optional background writes to disk.

use moka::sync::Cache;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, warn};

/// In-memory cache for downloaded pieces
/// Key: (info_hash, piece_index)
/// Value: Raw piece data
pub type PieceCache = Cache<(String, i32), Arc<Vec<u8>>>;

/// Configuration for piece caching behavior
#[derive(Debug, Clone, Default)]
pub struct PieceCacheConfig {
    /// Maximum memory to use for piece cache (bytes)
    /// 0 = unlimited (dynamic sizing based on available RAM)
    pub max_memory_bytes: u64,
    /// Whether to write pieces to disk in background
    /// When true, pieces are persisted; when false, memory-only caching
    pub disk_cache_enabled: bool,
    /// Path for disk cache (optional)
    pub disk_cache_path: Option<PathBuf>,
}

impl PieceCacheConfig {
    /// Create config from EngineCacheConfig
    pub fn from_engine_config(
        engine_config: &crate::backend::priorities::EngineCacheConfig, cache_path: PathBuf,
    ) -> Self {
        Self {
            // Dynamic: 0 = unlimited, let moka evict based on TTI
            // For disk-backed cache, use 5% of disk cache size up to 512MB
            max_memory_bytes: if engine_config.size > 0 {
                (engine_config.size / 20).min(512 * 1024 * 1024)
            } else {
                0 // Unlimited memory cache
            },
            disk_cache_enabled: engine_config.enabled && engine_config.size > 0,
            disk_cache_path: Some(cache_path),
        }
    }
}

/// Manages piece caching with memory-first, optional disk persistence
pub struct PieceCacheManager {
    cache: PieceCache,
    config: PieceCacheConfig,
    /// Track which pieces have been written to disk
    disk_written: Arc<RwLock<HashSet<(String, i32)>>>,
    /// Track in-flight read_piece requests to avoid duplicates
    pending_requests: Arc<RwLock<HashSet<(String, i32)>>>,
}

impl PieceCacheManager {
    pub fn new(config: PieceCacheConfig) -> Self {
        // Moka cache with advanced eviction policies:
        // 1. Size-based: uses weigher with max_capacity for memory limit (if > 0)
        // 2. Time-to-idle: expire entries not accessed for 5 minutes
        // This ensures memory is freed for pieces not being actively streamed
        let cache = if config.max_memory_bytes > 0 {
            // Bounded cache
            Cache::builder()
                .weigher(|_key: &(String, i32), value: &Arc<Vec<u8>>| value.len() as u32)
                .max_capacity(config.max_memory_bytes)
                .time_to_idle(std::time::Duration::from_secs(300)) // 5 minute idle timeout
                .support_invalidation_closures()
                .build()
        } else {
            // Unbounded cache - only TTI eviction
            Cache::builder()
                .weigher(|_key: &(String, i32), value: &Arc<Vec<u8>>| value.len() as u32)
                .time_to_idle(std::time::Duration::from_secs(300)) // 5 minute idle timeout
                .support_invalidation_closures()
                .build()
        };

        debug!(
            "PieceCacheManager: Created with max_memory={}, disk_enabled={}",
            if config.max_memory_bytes > 0 {
                format!("{}MB", config.max_memory_bytes / (1024 * 1024))
            } else {
                "unlimited".to_string()
            },
            config.disk_cache_enabled
        );

        Self {
            cache,
            config,
            disk_written: Arc::new(RwLock::new(HashSet::new())),
            pending_requests: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Store a piece in memory cache
    pub async fn put_piece(&self, info_hash: &str, piece_idx: i32, data: Vec<u8>) {
        self.put_piece_now(info_hash, piece_idx, data);
    }

    /// Store a verified piece without requiring an async executor hop.
    pub fn put_piece_now(&self, info_hash: &str, piece_idx: i32, data: Vec<u8>) {
        let key = (info_hash.to_lowercase(), piece_idx);
        let data_len = data.len();
        let data = Arc::new(data);

        self.cache.insert(key.clone(), data.clone());

        // Clear from pending requests since it's now cached
        self.pending_requests.write().remove(&key);

        debug!(
            "PieceCache: Stored piece {} for {} ({} bytes) - memory-first",
            piece_idx, info_hash, data_len
        );

        // Only write to disk if:
        // 1. Disk cache is enabled
        // 2. max_memory_bytes > 0 (bounded cache, so we need disk backup)
        // 3. disk_cache_path is set
        if self.config.disk_cache_enabled
            && self.config.max_memory_bytes > 0
            && let Some(ref disk_path) = self.config.disk_cache_path
        {
            let piece_path = disk_path
                .join(&key.0)
                .join(format!("piece_{:06}.bin", piece_idx));

            let disk_written = self.disk_written.clone();
            let data_clone = data.clone();
            let key_clone = key.clone();

            tokio::spawn(async move {
                if let Some(parent) = piece_path.parent()
                    && let Err(e) = tokio::fs::create_dir_all(parent).await
                {
                    warn!("Failed to create piece cache dir: {}", e);
                    return;
                }

                match tokio::fs::write(&piece_path, &*data_clone).await {
                    Ok(_) => {
                        let mut written = disk_written.write();
                        written.insert(key_clone);
                        debug!("PieceCache: Background disk write: {:?}", piece_path);
                    }
                    Err(e) => {
                        warn!("Failed to write piece to disk: {}", e);
                    }
                }
            });
        }
    }

    /// Get a piece from cache (memory first, then disk)
    pub async fn get_piece(&self, info_hash: &str, piece_idx: i32) -> Option<Arc<Vec<u8>>> {
        let key = (info_hash.to_lowercase(), piece_idx);

        // Try memory cache first (fast path)
        if let Some(data) = self.cache.get(&key) {
            return Some(data);
        }

        // Fall back to disk cache
        if self.config.disk_cache_enabled
            && let Some(ref disk_path) = self.config.disk_cache_path
        {
            let piece_path = disk_path
                .join(&key.0)
                .join(format!("piece_{:06}.bin", piece_idx));

            if let Ok(data) = tokio::fs::read(&piece_path).await {
                let data = Arc::new(data);
                // Re-populate memory cache
                self.cache.insert(key, data.clone());
                debug!(
                    "PieceCache: Loaded piece {} from disk for {}",
                    piece_idx, info_hash
                );
                return Some(data);
            }
        }

        None
    }

    /// Nonblocking hot-cache lookup for `AsyncRead::poll_read`.
    pub fn get_cached_piece(&self, info_hash: &str, piece_idx: i32) -> Option<Arc<Vec<u8>>> {
        self.cache.get(&(info_hash.to_lowercase(), piece_idx))
    }

    /// Check if piece is available (in memory or disk)
    pub async fn has_piece(&self, info_hash: &str, piece_idx: i32) -> bool {
        let key = (info_hash.to_lowercase(), piece_idx);

        if self.cache.contains_key(&key) {
            return true;
        }

        // Check disk written set (fast check without I/O)
        let disk_written = self.disk_written.read();
        disk_written.contains(&key)
    }

    /// Mark a piece as pending (returns false if already pending)
    /// Used for request coalescing - prevents duplicate read_piece() calls
    pub async fn mark_pending(&self, info_hash: &str, piece_idx: i32) -> bool {
        let key = (info_hash.to_lowercase(), piece_idx);
        let mut pending = self.pending_requests.write();
        pending.insert(key)
    }

    /// Check if a piece request is already pending
    pub async fn is_pending(&self, info_hash: &str, piece_idx: i32) -> bool {
        let key = (info_hash.to_lowercase(), piece_idx);
        let pending = self.pending_requests.read();
        pending.contains(&key)
    }

    /// Get cache statistics
    pub fn stats(&self) -> (u64, u64) {
        (self.cache.entry_count(), self.cache.weighted_size())
    }

    /// Clear all cached pieces for a specific torrent
    pub async fn clear_torrent(&self, info_hash: &str) {
        let info_hash_lower = info_hash.to_lowercase();

        if let Err(error) = self.cache.invalidate_entries_if({
            let info_hash_lower = info_hash_lower.clone();
            move |(hash, _), _| hash == &info_hash_lower
        }) {
            warn!(
                "Failed to invalidate cached pieces for {}: {}",
                info_hash, error
            );
        }
        self.cache.run_pending_tasks();

        // Clear from disk tracking
        let mut disk_written = self.disk_written.write();
        disk_written.retain(|(ih, _)| ih != &info_hash_lower);
        let mut pending = self.pending_requests.write();
        pending.retain(|(ih, _)| ih != &info_hash_lower);

        // Optionally clean disk files in background
        if self.config.disk_cache_enabled
            && let Some(ref disk_path) = self.config.disk_cache_path
        {
            let torrent_cache_dir = disk_path.join(&info_hash_lower);
            tokio::spawn(async move {
                let _ = tokio::fs::remove_dir_all(torrent_cache_dir).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn clearing_one_torrent_preserves_other_cached_pieces() {
        let cache = PieceCacheManager::new(PieceCacheConfig::default());
        cache.put_piece_now("torrent-a", 0, vec![1, 2, 3]);
        cache.put_piece_now("torrent-b", 0, vec![4, 5, 6]);

        cache.clear_torrent("torrent-a").await;

        assert!(cache.get_cached_piece("torrent-a", 0).is_none());
        assert_eq!(
            cache
                .get_cached_piece("torrent-b", 0)
                .as_deref()
                .map(Vec::as_slice),
            Some([4, 5, 6].as_slice())
        );
    }
}
