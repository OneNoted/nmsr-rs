use std::{path::PathBuf, sync::Arc, time::Duration};

use super::entry::RenderRequestEntry;
use crate::{
    error::{ModelCacheError, ModelCacheResult, Result},
    model::request::cache::{
        entries::ResolvedModelTexturesCacheHandler, names::MojangNamesCacheHandler,
        textures::MojangTextureCacheHandler,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::serde_as;
use uuid::Uuid;

pub(crate) mod entries;
pub(crate) mod names;
pub(crate) mod textures;

use crate::{
    caching::{renew, CacheSystem, Cached},
    config::ModelCacheConfiguration,
    error::ExplainableExt,
    model::resolver::{MojangTexture, ResolvedRenderEntryTextures},
};

#[serde_as]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize, strum::IntoStaticStr)]
pub enum CacheBias {
    KeepCachedFor(#[serde(with = "humantime_serde")] Duration),
    CacheIndefinitely,
}

impl TryFrom<String> for CacheBias {
    type Error = ModelCacheError;

    fn try_from(value: String) -> ModelCacheResult<Self> {
        if value == Into::<&'static str>::into(Self::CacheIndefinitely) {
            return Ok(Self::CacheIndefinitely);
        }

        let duration: Duration = humantime_serde::deserialize(Value::String(value.clone()))
            .map_err(|_| ModelCacheError::InvalidCacheBiasConfiguration(value.clone()))?;

        Ok(Self::KeepCachedFor(duration))
    }
}

impl TryFrom<CacheBias> for String {
    type Error = ModelCacheError;
    fn try_from(value: CacheBias) -> ModelCacheResult<Self> {
        Ok(match value {
            CacheBias::KeepCachedFor(duration) => {
                humantime_serde::re::humantime::format_duration(duration).to_string()
            }
            CacheBias::CacheIndefinitely => {
                Into::<&'static str>::into(CacheBias::CacheIndefinitely).to_string()
            }
        })
    }
}

pub struct ModelCache {
    mojang_textures: Arc<
        CacheSystem<str, MojangTexture, ModelCacheConfiguration, (), MojangTextureCacheHandler>,
    >,
    resolved_names: CacheSystem<str, Uuid, ModelCacheConfiguration, (), MojangNamesCacheHandler>,
    resolved_textures: CacheSystem<
        RenderRequestEntry,
        ResolvedRenderEntryTextures,
        ModelCacheConfiguration,
        [u8; 1],
        ResolvedModelTexturesCacheHandler,
    >,
}

impl ModelCache {
    pub async fn new(cache_path: PathBuf, cache_config: ModelCacheConfiguration) -> Result<Self> {
        let mojang = CacheSystem::new(
            cache_path.join("textures"),
            cache_config.clone(),
            MojangTextureCacheHandler,
        )
        .await?;

        let mojang = Arc::new(mojang);

        let resolved = CacheSystem::new(
            cache_path.join("resolved"),
            cache_config.clone(),
            ResolvedModelTexturesCacheHandler {
                mojang_texture_cache: mojang.clone(),
            },
        )
        .await?;

        let names = CacheSystem::new(
            cache_path.join("names"),
            cache_config.clone(),
            MojangNamesCacheHandler,
        )
        .await?;

        Ok(Self {
            mojang_textures: mojang.clone(),
            resolved_names: names,
            resolved_textures: resolved,
        })
    }

    /// Textures are content addressed, so a stale texture is still the right texture.
    pub async fn get_cached_texture(&self, texture_id: &str) -> Result<Option<MojangTexture>> {
        Ok(self
            .mojang_textures
            .get_cached_entry(texture_id)
            .await?
            .map(|cached| cached.value))
    }

    pub async fn cache_texture(&self, texture: &MojangTexture) -> Result<()> {
        if let Some(hash) = texture.hash() {
            self.mojang_textures
                .set_cache_entry(hash, texture)
                .await
                .map(|_| ())
        } else {
            Ok(())
        }
    }

    pub async fn get_cached_resolved_texture(
        &self,
        entry: &RenderRequestEntry,
    ) -> Result<Option<Cached<ResolvedRenderEntryTextures>>> {
        self.resolved_textures.get_cached_entry(entry).await
    }

    pub async fn cache_resolved_texture(
        &self,
        entry: &RenderRequestEntry,
        textures: &ResolvedRenderEntryTextures,
    ) -> Result<()> {
        self.resolved_textures
            .set_cache_entry(entry, textures)
            .await
            .map(|_| ())
    }

    pub async fn forget_resolved_texture(&self, entry: &RenderRequestEntry) -> Result<()> {
        self.resolved_textures.remove_cache_entry(entry).await
    }

    pub async fn forget_resolved_name(&self, name: &str) -> Result<()> {
        self.resolved_names.remove_cache_entry(name).await
    }

    pub async fn cache_resolved_name(&self, name: &str, uuid: Uuid) -> Result<()> {
        self.resolved_names
            .set_cache_entry(name, &uuid)
            .await
            .map(|_| ())
    }

    pub async fn get_cached_resolved_name(&self, name: &str) -> Result<Option<Cached<Uuid>>> {
        self.resolved_names.get_cached_entry(name).await
    }

    pub(crate) async fn do_cache_clean_up(&self) -> Result<()> {
        self.resolved_textures.perform_cache_cleanup().await?;
        self.renew_linked_textures().await?;
        self.mojang_textures.perform_cache_cleanup().await?;

        Ok(())
    }

    /// Resolved entries link to their textures, and may outlive the texture lifetime (cache biases, default
    /// skins). Renewing every linked texture before cleanup keeps it while any entry still uses it.
    async fn renew_linked_textures(&self) -> Result<()> {
        let resolved = self.resolved_textures.base_path().to_owned();

        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            for entry in std::fs::read_dir(&resolved)? {
                let entry = entry?.path();
                if !entry.is_dir() {
                    continue;
                }
                let Some(files) = unless_vanished(std::fs::read_dir(&entry))? else {
                    continue;
                };

                for file in files {
                    let file = file?.path();
                    let linked = unless_vanished(file.symlink_metadata())?
                        .is_some_and(|metadata| metadata.is_symlink());

                    if linked {
                        unless_vanished(renew(&file))?;
                    }
                }
            }

            Ok(())
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|renewed| renewed)
        .explain("Unable to renew textures linked from resolved entries".to_owned())
    }
}

/// Entries are replaced concurrently by refreshes, so anything may vanish mid-scan.
fn unless_vanished<T>(result: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match result {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        result => result.map(Some),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::resolver::ResolvedRenderEntryTextureType;
    use std::{collections::BTreeMap, path::Path, time::SystemTime};

    const DAY: Duration = Duration::from_hours(24);
    const ACTIVE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const IDLE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn texture(hash: &str) -> MojangTexture {
        MojangTexture::new_named(hash.to_owned(), b"\x89PNG\r\n\x1a\nfake".to_vec())
    }

    fn backdate(path: &Path, age: Duration) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
    }

    #[tokio::test]
    async fn default_skins_stay_cached_through_cleanup() {
        use crate::model::resolver::default_skins::{DefaultSkin, DefaultSkinResolver};

        let dir = std::env::temp_dir().join(format!("nmsr-cache-test-{}", Uuid::new_v4()));
        let hash = DefaultSkinResolver::resolve_default_skin(DefaultSkin::Steve, false);
        let default_skin = RenderRequestEntry::DefaultSkinTextureHash(hash.to_owned());
        let mut config = ModelCacheConfiguration::default();
        config
            .cache_biases
            .insert(default_skin.clone(), CacheBias::CacheIndefinitely);
        let cache = ModelCache::new(dir.clone(), config).await.unwrap();
        let textures = BTreeMap::from([(ResolvedRenderEntryTextureType::Skin, texture(hash))]);

        cache
            .cache_resolved_texture(
                &default_skin,
                &ResolvedRenderEntryTextures::new(textures, None),
            )
            .await
            .unwrap();
        backdate(&dir.join("resolved").join(hash).join("marker"), 5 * DAY);
        backdate(&dir.join("textures").join(hash), 5 * DAY);

        cache.do_cache_clean_up().await.unwrap();

        assert!(cache
            .get_cached_resolved_texture(&default_skin)
            .await
            .unwrap()
            .is_some());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn textures_linked_from_long_lived_entries_survive_cleanup() {
        let dir = std::env::temp_dir().join(format!("nmsr-cache-test-{}", Uuid::new_v4()));
        let player = RenderRequestEntry::MojangPlayerUuid(Uuid::new_v4());
        let mut config = ModelCacheConfiguration::default();
        config
            .cache_biases
            .insert(player.clone(), CacheBias::CacheIndefinitely);
        let cache = ModelCache::new(dir.clone(), config).await.unwrap();
        let textures = BTreeMap::from([(ResolvedRenderEntryTextureType::Skin, texture(ACTIVE))]);

        cache
            .cache_resolved_texture(&player, &ResolvedRenderEntryTextures::new(textures, None))
            .await
            .unwrap();
        cache.cache_texture(&texture(IDLE)).await.unwrap();
        backdate(&dir.join("textures").join(ACTIVE), 5 * DAY);
        backdate(&dir.join("textures").join(IDLE), 5 * DAY);

        cache.do_cache_clean_up().await.unwrap();

        let player_skin = cache.get_cached_resolved_texture(&player).await.unwrap();
        assert!(player_skin.is_some_and(|cached| cached
            .value
            .textures
            .contains_key(&ResolvedRenderEntryTextureType::Skin)));
        assert!(cache.get_cached_texture(IDLE).await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn textures_still_referenced_by_refreshed_players_survive_cleanup() {
        let dir = std::env::temp_dir().join(format!("nmsr-cache-test-{}", Uuid::new_v4()));
        let config = ModelCacheConfiguration {
            resolve_cache_duration: Duration::ZERO,
            ..Default::default()
        };
        let cache = ModelCache::new(dir.clone(), config).await.unwrap();
        let player = RenderRequestEntry::MojangPlayerUuid(Uuid::new_v4());
        let resolved = || {
            let textures =
                BTreeMap::from([(ResolvedRenderEntryTextureType::Skin, texture(ACTIVE))]);
            ResolvedRenderEntryTextures::new(textures, None)
        };

        cache
            .cache_resolved_texture(&player, &resolved())
            .await
            .unwrap();
        cache.cache_texture(&texture(IDLE)).await.unwrap();
        backdate(&dir.join("textures").join(ACTIVE), 5 * DAY);
        backdate(&dir.join("textures").join(IDLE), 5 * DAY);

        cache
            .cache_resolved_texture(&player, &resolved())
            .await
            .unwrap();
        cache.do_cache_clean_up().await.unwrap();

        let player_skin = cache.get_cached_resolved_texture(&player).await.unwrap();
        assert!(player_skin.is_some_and(|cached| cached
            .value
            .textures
            .contains_key(&ResolvedRenderEntryTextureType::Skin)));
        assert!(cache.get_cached_texture(IDLE).await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(dir);
    }
}
