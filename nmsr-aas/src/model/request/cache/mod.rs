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
    caching::{CacheSystem, Cached},
    config::ModelCacheConfiguration,
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
        self.mojang_textures.perform_cache_cleanup().await?;

        Ok(())
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
