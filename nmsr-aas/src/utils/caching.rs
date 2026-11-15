use std::{
    borrow::Cow,
    marker::PhantomData,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use derive_more::with_trait::Debug;
use tokio::fs;
use tokio_stream::{wrappers::ReadDirStream, StreamExt};
use tracing::{instrument, trace, trace_span, Instrument, Span};

use crate::error::{ExplainableExt, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Freshness {
    Fresh,
    /// Past its freshness lifetime but still retained, so it can stand in when the source is unavailable.
    Stale,
}

#[derive(Debug)]
pub struct Cached<V> {
    pub value: V,
    pub freshness: Freshness,
}

impl<V> Cached<V> {
    pub const fn fresh(value: V) -> Self {
        Self {
            value,
            freshness: Freshness::Fresh,
        }
    }

    pub const fn stale(value: V) -> Self {
        Self {
            value,
            freshness: Freshness::Stale,
        }
    }

    pub fn is_fresh(&self) -> bool {
        self.freshness == Freshness::Fresh
    }
}

/// Restarts an entry's lifetime, so content still referenced by other entries is not cleaned up.
fn renew(marker_path: &Path) -> std::io::Result<()> {
    std::fs::File::options()
        .write(true)
        .open(marker_path)?
        .set_modified(SystemTime::now())
}

/// Classifies an entry of the given age; `None` means it is past retention and should be deleted.
/// A `fresh_for` of [`Duration::MAX`] never expires.
fn freshness_at(
    age: Duration,
    fresh_for: Duration,
    retain_stale_for: Duration,
) -> Option<Freshness> {
    if age < fresh_for {
        Some(Freshness::Fresh)
    } else if age < fresh_for.saturating_add(retain_stale_for) {
        Some(Freshness::Stale)
    } else {
        None
    }
}

pub struct CacheSystem<Key, ResultEntry, Config, Marker, Handler>
where
    Key: Debug + ToOwned + ?Sized,
    Handler: CacheHandler<Key, ResultEntry, Config, Marker> + Sync,
{
    base_path: PathBuf,
    config: Config,
    handler: Handler,
    _phantom: PhantomData<(ResultEntry, Marker, Key)>,
}

#[async_trait]
#[allow(unused_variables)]
pub trait CacheHandler<Key, Value, Config, Marker>
where
    Key: ToOwned + ?Sized,
{
    /// Given a path, returns the cache key for the entry.
    /// This is used to determine which entries are expired when cleaning the cache.
    async fn read_key_from_path<'a>(
        &'a self,
        config: &Config,
        base_path: &'a Path,
    ) -> Result<Option<Cow<'a, Key>>>;

    /// Gets the cache key for the given entry.
    ///
    /// If the entry is not cached, this should return `None`.
    async fn get_cache_key(&self, entry: &Key, config: &Config) -> Result<Option<String>>;

    /// How long after being written the entry is fresh. [`Duration::MAX`] means forever.
    fn fresh_for(&self, entry: &Key, config: &Config) -> Duration;

    /// How long a no-longer-fresh entry is kept and returned as [`Freshness::Stale`] before deletion.
    fn retain_stale_for(&self, config: &Config) -> Duration;

    /// Writes the given entry to the cache.
    async fn write_cache(
        &self,
        entry: &Key,
        value: &Value,
        config: &Config,
        file: &Path,
    ) -> Result<()>;

    /// Reads the given entry from the cache.
    async fn read_cache(
        &self,
        entry: &Key,
        config: &Config,
        file: &Path,
        marker: &Marker,
    ) -> Result<Option<Value>>;

    /// Returns the path to the marker file.
    async fn get_marker_path(&self, entry: &Key, config: &Config) -> Result<String> {
        Ok("marker".into())
    }

    /// Read the marker file for the given entry.
    ///
    /// The marker file is used to denote when the entry was cached.
    /// It can be empty, but it must exist.
    async fn read_marker(&self, entry: &Key, config: &Config, marker: &Path) -> Result<Marker>;

    /// Writes the marker file for the given entry.
    ///
    /// The marker file is used to denote when the entry was cached.
    /// It can be empty, but it must exist.
    async fn write_marker(
        &self,
        entry: &Key,
        value: &Value,
        config: &Config,
        marker: &Path,
    ) -> Result<()>;

    /// Whether the key identifies the value's content, so an existing entry is renewed instead of replaced.
    fn is_content_addressed(&self) -> bool {
        false
    }
}

impl<Key, ResultEntry, Config, Marker, Handler>
    CacheSystem<Key, ResultEntry, Config, Marker, Handler>
where
    Key: Debug + ToOwned + ?Sized,
    Handler: CacheHandler<Key, ResultEntry, Config, Marker> + Sync,
{
    pub async fn new(base_path: PathBuf, config: Config, handler: Handler) -> Result<Self> {
        fs::create_dir_all(&base_path)
            .await
            .explain(format!("Unable to create cache directory {:?}", &base_path))?;

        Ok(Self {
            base_path,
            config,
            handler,
            _phantom: PhantomData,
        })
    }

    pub async fn get_cache_entry_path(&self, entry: &Key) -> Result<Option<PathBuf>> {
        let key = self.handler.get_cache_key(entry, &self.config).await?;

        Ok(key.map(|k| self.base_path.join(k)))
    }

    pub async fn get_cached_entry(&self, entry: &Key) -> Result<Option<Cached<ResultEntry>>> {
        let Some(path) = self.get_cache_entry_path(entry).await? else {
            return Ok(None);
        };

        let Some((marker, freshness)) = self.inspect_entry(entry, &path).await? else {
            trace!("Haven't found marker or entry is past retention for key {entry:?}");
            return Ok(None);
        };

        let result = self
            .handler
            .read_cache(entry, &self.config, &path, &marker)
            .await?;

        if result.is_some() {
            trace!("Cache entry found ({freshness:?}).");
        } else {
            trace!("Cache entry missing at path {}.", path.display());
        }

        Ok(result.map(|value| Cached { value, freshness }))
    }

    /// Reads the entry's marker and classifies its freshness, deleting it once past retention.
    #[instrument(name = "check_entry", skip(self, path))]
    async fn inspect_entry(&self, entry: &Key, path: &Path) -> Result<Option<(Marker, Freshness)>> {
        if !path.exists() {
            trace!("Cache entry path doesn't exist.");
            return Ok(None);
        }

        let marker_path = self.marker_path(entry, path).await?;

        if !marker_path.exists() {
            trace!("Cache entry path {} doesn't exist.", marker_path.display());
            return Ok(None);
        }
        let marker = self
            .handler
            .read_marker(entry, &self.config, &marker_path)
            .await?;
        let written_at = marker_path
            .metadata()
            .and_then(|metadata| metadata.modified())
            .explain(format!(
                "Unable to read marker for entry {:?} ({})",
                entry,
                marker_path.display()
            ))?;

        let age = SystemTime::now()
            .duration_since(written_at)
            .unwrap_or_default();

        let freshness = freshness_at(
            age,
            self.handler.fresh_for(entry, &self.config),
            self.handler.retain_stale_for(&self.config),
        );

        if freshness.is_none() {
            trace!("Entry is past retention, discarding.");
            Self::invalidate_self(entry, path).await?;
        }

        Ok(freshness.map(|freshness| (marker, freshness)))
    }

    pub async fn remove_cache_entry(&self, entry: &Key) -> Result<()> {
        match self.get_cache_entry_path(entry).await? {
            Some(path) if path.exists() => Self::invalidate_self(entry, &path).await,
            _ => Ok(()),
        }
    }

    async fn marker_path(&self, entry: &Key, path: &Path) -> Result<PathBuf> {
        let marker_path = self.handler.get_marker_path(entry, &self.config).await?;

        Ok(if marker_path.is_empty() {
            path.to_owned()
        } else {
            path.join(marker_path)
        })
    }

    #[inline]
    pub(crate) async fn invalidate_self<'a>(entry: &'a Key, path: &'a Path) -> Result<()> {
        trace!(
            "Invalidating entry {entry:?} for path {p}",
            p = path.display()
        );

        let removed = if path.is_dir() {
            fs::remove_dir_all(path).await
        } else {
            fs::remove_file(path).await
        };

        match removed {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            removed => removed.explain(format!("Unable to invalidate cache entry {entry:?}")),
        }
    }

    #[instrument(name = "set_cache_entry", skip(self, value))]
    pub async fn set_cache_entry(
        &self,
        entry: &Key,
        value: &ResultEntry,
    ) -> Result<Option<PathBuf>> {
        let path = self.get_cache_entry_path(entry).await?;

        if let Some(path) = &path {
            let marker_path = self.marker_path(entry, path).await?;

            if path.exists() {
                if self.handler.is_content_addressed() {
                    renew(&marker_path)
                        .explain(format!("Unable to renew cache entry {entry:?}"))?;
                    return Ok(Some(path.clone()));
                }

                if let Some((_, Freshness::Fresh)) = self.inspect_entry(entry, path).await? {
                    return Ok(Some(path.clone()));
                }

                Self::invalidate_self(entry, path).await?;
            }

            self.handler
                .write_cache(entry, value, &self.config, path)
                .instrument(trace_span!("write_cache"))
                .await?;

            self.handler
                .write_marker(entry, value, &self.config, &marker_path)
                .await?;
        }

        Ok(path)
    }

    pub async fn perform_cache_cleanup(&self) -> Result<()> {
        let entries = fs::read_dir(&self.base_path).await.explain(format!(
            "Unable to read cache directory {}",
            &self.base_path.display()
        ))?;

        let mut stream = ReadDirStream::new(entries);

        while let Some(file) = stream.next().await {
            let file = file.explain(format!(
                "Unable to read cache entry while cleaning {}",
                &self.base_path.display()
            ))?;

            let path = file.path();

            if let Some(key) = self.handler.read_key_from_path(&self.config, &path).await? {
                let _ = self
                    .inspect_entry(&key, &path)
                    .instrument(Span::none())
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    #[test]
    fn entries_are_fresh_then_stale_then_gone() {
        assert_eq!(
            freshness_at(Duration::ZERO, 15 * MINUTE, 60 * MINUTE),
            Some(Freshness::Fresh)
        );
        assert_eq!(
            freshness_at(15 * MINUTE, 15 * MINUTE, 60 * MINUTE),
            Some(Freshness::Stale)
        );
        assert_eq!(
            freshness_at(74 * MINUTE, 15 * MINUTE, 60 * MINUTE),
            Some(Freshness::Stale)
        );
        assert_eq!(freshness_at(75 * MINUTE, 15 * MINUTE, 60 * MINUTE), None);
    }

    #[test]
    fn without_stale_retention_entries_expire_at_their_lifetime() {
        assert_eq!(freshness_at(15 * MINUTE, 15 * MINUTE, Duration::ZERO), None);
    }

    #[test]
    fn indefinitely_cached_entries_stay_fresh() {
        assert_eq!(
            freshness_at(
                Duration::from_secs(u64::MAX / 2),
                Duration::MAX,
                60 * MINUTE
            ),
            Some(Freshness::Fresh)
        );
    }
}
