use self::{
    geyser::resolve_geyser_uuid_to_texture_and_model,
    mojang::{client::MojangClient, model::GameProfileTexture},
};
use super::request::{
    cache::ModelCache,
    entry::{RenderRequestEntry, RenderRequestEntryModel},
    RenderRequest,
};
#[cfg(feature = "ears")]
use crate::model::request::RenderRequestFeatures;
use crate::{
    caching::{Cached, Freshness},
    error::{FailureKind, MojangRequestError, NMSRaaSError, RenderRequestError, Result},
    model::resolver::{
        default_skins::DefaultSkinResolver, mojang::client::MojangTextureRequestType,
    },
};
use derive_more::Debug;
#[cfg(feature = "ears")]
use ears_rs::{alfalfa::AlfalfaDataKey, features::EarsFeatures, parser::EarsParser};
#[cfg(feature = "ears")]
use nmsr_rendering::high_level::parts::provider::ears::PlayerPartEarsTextureType;
use nmsr_rendering::high_level::types::PlayerPartTextureType;
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    sync::{Arc, PoisonError, Weak},
};
use strum::EnumCount;
use tracing::{instrument, trace_span, warn, Instrument, Span};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

pub mod default_skins;
pub mod geyser;
pub mod mojang;

#[cfg(test)]
mod tests;

pub struct RenderRequestResolver {
    model_cache: ModelCache,
    mojang_requests_client: Arc<MojangClient>,
    in_flight: KeyedLocks<RenderRequestEntry>,
}

/// Serializes work per key, so identical concurrent requests share one upstream fetch
/// and never observe each other's cache rewrite.
struct KeyedLocks<K>(std::sync::Mutex<HashMap<K, Weak<tokio::sync::Mutex<()>>>>);

impl<K: Eq + Hash + Clone> KeyedLocks<K> {
    fn new() -> Self {
        Self(std::sync::Mutex::new(HashMap::new()))
    }

    async fn lock(&self, key: &K) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);

            locks.get(key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(key.clone(), Arc::downgrade(&lock));
                lock
            })
        };

        lock.lock_owned().await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResolvedRenderEntryTextureType {
    Cape,
    Skin,
    #[cfg(feature = "ears")]
    Ears(ResolvedRenderEntryEarsTextureType),
}

impl From<ResolvedRenderEntryTextureType> for &'static str {
    fn from(value: ResolvedRenderEntryTextureType) -> Self {
        match value {
            ResolvedRenderEntryTextureType::Cape => "Cape",
            ResolvedRenderEntryTextureType::Skin => "Skin",
            #[cfg(feature = "ears")]
            ResolvedRenderEntryTextureType::Ears(ears) => ears.key(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResolvedRenderEntryEarsTextureType {
    Cape,
    Wings,
    DisplacedSkin,
    EmissiveDisplacedSkin,
    /// The non-emissive remaining part of the skin texture.
    EmissiveProcessedSkin,
    /// The non-emissive remaining part of the wings texture.
    EmissiveProcessedWings,
    /// The emissive skin texture type.
    EmissiveSkin,
    /// The emissive wings texture type.
    EmissiveWings,
}

#[cfg(feature = "ears")]
impl From<ResolvedRenderEntryEarsTextureType> for PlayerPartEarsTextureType {
    fn from(value: ResolvedRenderEntryEarsTextureType) -> Self {
        match value {
            ResolvedRenderEntryEarsTextureType::Cape => Self::Cape,
            ResolvedRenderEntryEarsTextureType::Wings => Self::Wings,
            ResolvedRenderEntryEarsTextureType::EmissiveSkin => Self::EmissiveSkin,
            ResolvedRenderEntryEarsTextureType::DisplacedSkin => Self::DisplacedSkin,
            ResolvedRenderEntryEarsTextureType::EmissiveDisplacedSkin => {
                Self::EmissiveDisplacedSkin
            }
            ResolvedRenderEntryEarsTextureType::EmissiveProcessedSkin => {
                Self::EmissiveProcessedSkin
            }
            ResolvedRenderEntryEarsTextureType::EmissiveProcessedWings => {
                Self::EmissiveProcessedWings
            }
            ResolvedRenderEntryEarsTextureType::EmissiveWings => Self::EmissiveWings,
        }
    }
}

#[cfg(feature = "ears")]
impl ResolvedRenderEntryEarsTextureType {
    const fn alfalfa_key(self) -> Option<AlfalfaDataKey> {
        match self {
            Self::Cape => Some(AlfalfaDataKey::Cape),
            Self::Wings => Some(AlfalfaDataKey::Wings),
            Self::DisplacedSkin
            | Self::EmissiveDisplacedSkin
            | Self::EmissiveSkin
            | Self::EmissiveProcessedSkin
            | Self::EmissiveProcessedWings
            | Self::EmissiveWings => None,
        }
    }

    fn key(self) -> &'static str {
        PlayerPartEarsTextureType::from(self).key()
    }
}

impl From<ResolvedRenderEntryTextureType> for PlayerPartTextureType {
    fn from(value: ResolvedRenderEntryTextureType) -> Self {
        match value {
            ResolvedRenderEntryTextureType::Skin => Self::Skin,
            ResolvedRenderEntryTextureType::Cape => Self::Cape,
            #[cfg(feature = "ears")]
            ResolvedRenderEntryTextureType::Ears(ears) => {
                PlayerPartEarsTextureType::from(ears).into()
            }
        }
    }
}

pub struct MojangTexture {
    hash: Option<String>,
    data: Vec<u8>,
}

impl MojangTexture {
    pub(crate) fn new_named(hash: String, data: Vec<u8>) -> Self {
        Self {
            hash: Some(hash),
            data,
        }
    }

    pub(crate) fn new_unnamed(data: Vec<u8>) -> Self {
        Self { hash: None, data }
    }

    pub(crate) fn new_unnamed_hashed(data: Vec<u8>) -> Self {
        let hash = format!("{:x}", xxh3_128(&data));

        Self {
            hash: Some(hash),
            data,
        }
    }

    #[must_use]
    pub const fn hash(&self) -> Option<&String> {
        self.hash.as_ref()
    }

    #[must_use]
    pub fn data(&self) -> &[u8] {
        self.data.as_ref()
    }
}

pub struct ResolvedRenderEntryTextures {
    pub model: Option<RenderRequestEntryModel>,
    pub textures: BTreeMap<ResolvedRenderEntryTextureType, MojangTexture>,
}

pub struct ResolvedRenderEntryTexturesMarker {
    pub model: u8,
}

impl From<ResolvedRenderEntryTextures> for ResolvedRenderEntryTexturesMarker {
    fn from(value: ResolvedRenderEntryTextures) -> Self {
        let model = value
            .model
            .map_or(RenderRequestEntryModel::COUNT as u8, |value| value as u8);

        Self { model }
    }
}

impl ResolvedRenderEntryTextures {
    #[must_use]
    pub const fn new(
        textures: BTreeMap<ResolvedRenderEntryTextureType, MojangTexture>,
        model: Option<RenderRequestEntryModel>,
    ) -> Self {
        Self { model, textures }
    }

    #[must_use]
    pub fn new_from_marker_slice(
        textures: BTreeMap<ResolvedRenderEntryTextureType, MojangTexture>,
        marker: &[u8],
    ) -> Self {
        let model = RenderRequestEntryModel::from_repr(marker[0] as usize);

        Self { model, textures }
    }

    #[must_use]
    pub fn to_marker_slice(&self) -> [u8; 1] {
        let model = self
            .model
            .map_or(RenderRequestEntryModel::COUNT as u8, |m| m as u8);

        [model]
    }
}

impl RenderRequestResolver {
    pub fn new(model_cache: ModelCache, client: Arc<MojangClient>) -> Self {
        Self {
            model_cache,
            mojang_requests_client: client,
            in_flight: KeyedLocks::new(),
        }
    }

    async fn fetch_game_profile_texture(
        &self,
        texture: Option<&GameProfileTexture>,
        req_type: MojangTextureRequestType,
    ) -> Result<Option<MojangTexture>> {
        if let Some(texture) = texture {
            let texture_id = texture.hash()?;
            let texture_url = texture.url();

            let texture = self
                .fetch_texture_from_mojang(texture_id, Some(texture_url), req_type)
                .await?;

            Ok(Some(texture))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self), parent = &Span::current())]
    async fn fetch_texture_from_mojang(
        &self,
        texture_id: &str,
        texture_url: Option<&str>,
        req_type: MojangTextureRequestType,
    ) -> Result<MojangTexture> {
        if let Some(result) = self.model_cache.get_cached_texture(texture_id).await? {
            return Ok(result);
        }

        let bytes = self
            .mojang_requests_client
            .fetch_texture_from_mojang(texture_id, texture_url, req_type)
            .await?;

        let texture = MojangTexture::new_named(texture_id.to_owned(), bytes);

        self.model_cache.cache_texture(&texture).await?;

        Ok(texture)
    }

    #[instrument(skip(self))]
    async fn resolve_entry_textures(
        &self,
        entry: &RenderRequestEntry,
    ) -> Result<Cached<ResolvedRenderEntryTextures>> {
        if let RenderRequestEntry::MojangPlayerName(name) = entry {
            let id = self.resolve_name(name).await?;
            let mut resolved = Box::pin(
                self.resolve_entry_textures(&RenderRequestEntry::MojangPlayerUuid(id.value)),
            )
            .await?;
            resolved.freshness = resolved.freshness.max(id.freshness);

            return Ok(resolved);
        }

        let _in_flight = match entry {
            RenderRequestEntry::PlayerSkin(_, _) => None,
            entry => Some(self.in_flight.lock(entry).await),
        };

        let stale = match self
            .model_cache
            .get_cached_resolved_texture(entry)
            .await?
            .map(Self::with_derived_textures)
        {
            Some(cached) if cached.is_fresh() => return Ok(cached),
            cached => cached,
        };

        let fetched = self.fetch_entry_textures(entry).await;

        let cached = match &fetched {
            Ok(textures) => {
                self.model_cache
                    .cache_resolved_texture(entry, textures)
                    .await
            }
            Err(error) if stale.is_some() && error.failure_kind().is_definitive_absence() => {
                self.model_cache.forget_resolved_texture(entry).await
            }
            Err(_) => Ok(()),
        };
        if let Err(error) = cached {
            warn!(%error, "Unable to update cached textures for {entry:?}");
        }

        stale_if_error(fetched, stale)
    }

    async fn resolve_name(&self, name: &str) -> Result<Cached<Uuid>> {
        let _in_flight = self
            .in_flight
            .lock(&RenderRequestEntry::MojangPlayerName(name.to_owned()))
            .await;

        let stale = match self.model_cache.get_cached_resolved_name(name).await? {
            Some(cached) if cached.is_fresh() => return Ok(cached),
            cached => cached,
        };

        let fetched = self
            .mojang_requests_client
            .resolve_name_to_uuid(name)
            .await
            .map_err(NMSRaaSError::from);

        let cached = match &fetched {
            Ok(id) => self.model_cache.cache_resolved_name(name, *id).await,
            Err(error) if stale.is_some() && error.failure_kind().is_definitive_absence() => {
                self.model_cache.forget_resolved_name(name).await
            }
            Err(_) => Ok(()),
        };
        if let Err(error) = cached {
            warn!(%error, "Unable to update cached name {name:?}");
        }

        stale_if_error(fetched, stale)
    }

    #[cfg_attr(not(feature = "ears"), allow(unused_mut))]
    fn with_derived_textures(
        mut cached: Cached<ResolvedRenderEntryTextures>,
    ) -> Cached<ResolvedRenderEntryTextures> {
        #[cfg(feature = "ears")]
        if let Some(skin) = cached
            .value
            .textures
            .remove(&ResolvedRenderEntryTextureType::Skin)
        {
            Self::resolve_ears_textures(&skin, &mut cached.value.textures);
            cached
                .value
                .textures
                .insert(ResolvedRenderEntryTextureType::Skin, skin);
        }

        cached
    }

    async fn fetch_entry_textures(
        &self,
        entry: &RenderRequestEntry,
    ) -> Result<ResolvedRenderEntryTextures> {
        let model: Option<RenderRequestEntryModel>;
        let skin_texture: Option<MojangTexture>;
        let cape_texture: Option<MojangTexture>;

        match &entry {
            RenderRequestEntry::MojangPlayerName(_) => {
                unreachable!("player names are resolved to a UUID before fetching textures")
            }
            RenderRequestEntry::MojangPlayerUuid(id)
            | RenderRequestEntry::MojangOfflinePlayerUuid(id) => {
                if matches!(&entry, RenderRequestEntry::MojangOfflinePlayerUuid(_))
                    && !self
                        .mojang_requests_client
                        .mojank_config()
                        .allow_offline_mode_uuids
                {
                    return Err(RenderRequestError::InvalidPlayerUuidRequest(
                        id.to_string(),
                        id.get_version_num(),
                    ))?;
                }

                let result = self
                    .mojang_requests_client
                    .resolve_uuid_to_game_profile(id)
                    .instrument(trace_span!("resolve_uuid_to_game_profile", uuid = %id))
                    .await?;

                let textures = result.textures()?;

                if let Some(skin) = textures.skin() {
                    model = Some(if skin.is_slim() {
                        RenderRequestEntryModel::Alex
                    } else {
                        RenderRequestEntryModel::Steve
                    });
                    skin_texture = self
                        .fetch_game_profile_texture(Some(skin), MojangTextureRequestType::Skin)
                        .await?;
                } else {
                    // Players who never chose a skin have none in their profile; Mojang shows the default for their UUID.
                    let (default_skin, slim) =
                        DefaultSkinResolver::resolve_default_skin_for_uuid_parts(*id, None);
                    model = Some(if slim {
                        RenderRequestEntryModel::Alex
                    } else {
                        RenderRequestEntryModel::Steve
                    });
                    skin_texture = Some(
                        self.fetch_texture_from_mojang(
                            DefaultSkinResolver::resolve_default_skin(default_skin, slim),
                            None,
                            MojangTextureRequestType::DefaultSkin,
                        )
                        .await?,
                    );
                }

                cape_texture = self
                    .fetch_game_profile_texture(textures.cape(), MojangTextureRequestType::Cape)
                    .await?;
            }
            RenderRequestEntry::GeyserPlayerUuid(id) => {
                let (texture_id, player_model) =
                    resolve_geyser_uuid_to_texture_and_model(&self.mojang_requests_client, id)
                        .await?;

                skin_texture = Some(
                    self.fetch_texture_from_mojang(
                        &texture_id,
                        None,
                        MojangTextureRequestType::Skin,
                    )
                    .await?,
                );
                cape_texture = None;

                model = Some(player_model);
            }
            RenderRequestEntry::TextureHash(skin_hash) => {
                // If the skin is not cached, we'll have to fetch it from Mojang.
                skin_texture = Some(
                    self.fetch_texture_from_mojang(skin_hash, None, MojangTextureRequestType::Skin)
                        .await?,
                );
                cape_texture = None;
                model = None;
            }
            RenderRequestEntry::DefaultSkinTextureHash(skin_hash) => {
                // Handle default skin textures. These have to go straight to Mojang, whether or not the user changed the config.
                skin_texture = Some(
                    self.fetch_texture_from_mojang(
                        skin_hash,
                        None,
                        MojangTextureRequestType::DefaultSkin,
                    )
                    .await?,
                );
                cape_texture = None;
                model = None;
            }
            RenderRequestEntry::PlayerSkin(skin_bytes, cape_bytes) => {
                skin_texture = Some(MojangTexture::new_unnamed(skin_bytes.clone()));
                cape_texture = cape_bytes.to_owned().map(|b| MojangTexture::new_unnamed(b));
                model = None;
            }
        }

        let mut textures = BTreeMap::new();

        if let Some(cape_texture) = cape_texture {
            textures.insert(ResolvedRenderEntryTextureType::Cape, cape_texture);
        }

        if let Some(skin_texture) = skin_texture {
            #[cfg(feature = "ears")]
            Self::resolve_ears_textures(&skin_texture, &mut textures);

            textures
                .entry(ResolvedRenderEntryTextureType::Skin)
                .or_insert(skin_texture);
        }

        Ok(ResolvedRenderEntryTextures::new(textures, model))
    }

    #[cfg(feature = "ears")]
    fn resolve_ears_textures(
        skin_texture: &MojangTexture,
        textures: &mut BTreeMap<ResolvedRenderEntryTextureType, MojangTexture>,
    ) -> Option<EarsFeatures> {
        use crate::utils::png::create_png_from_bytes;
        use ears_rs::utils::EarsEmissivePalette;
        use image::DynamicImage;
        use std::borrow::Cow;
        use xxhash_rust::xxh3::xxh3_128;

        let skin_image = image::load_from_memory(skin_texture.data())
            .ok()?
            .into_rgba8();
        let features = EarsParser::parse(&skin_image).ok().flatten()?;
        let alfalfa = ears_rs::alfalfa::read_alfalfa(&skin_image).ok().flatten();

        if let Some(alfalfa) = alfalfa {
            for texture_type in &[
                ResolvedRenderEntryEarsTextureType::Cape,
                ResolvedRenderEntryEarsTextureType::Wings,
            ] {
                if let Some(alfalfa_key) = texture_type.alfalfa_key() {
                    if let Some(data) = alfalfa.get_data(alfalfa_key) {
                        let hash = format!("{:x}", xxh3_128(data));

                        let data = if alfalfa_key == AlfalfaDataKey::Cape {
                            let image = image::load_from_memory(data)
                                .map(DynamicImage::into_rgba8)
                                .map(ears_rs::utils::convert_ears_cape_to_mojang_cape)
                                .ok()
                                .and_then(|i| {
                                    create_png_from_bytes((i.width(), i.height()), &i).ok()
                                });

                            image.map_or(Cow::Borrowed(data), Cow::Owned)
                        } else {
                            Cow::Borrowed(data)
                        };

                        textures.insert(
                            ResolvedRenderEntryTextureType::Ears(*texture_type),
                            MojangTexture::new_named(hash, data.into_owned()),
                        );
                    }
                }
            }
        }

        if let Some(displaced_skin) = ears_rs::utils::extract_displaced_skin(&skin_image, &features)
        {
            textures.insert(
                ResolvedRenderEntryTextureType::Ears(
                    ResolvedRenderEntryEarsTextureType::DisplacedSkin,
                ),
                MojangTexture::new_unnamed_hashed(
                    create_png_from_bytes(
                        (displaced_skin.width(), displaced_skin.height()),
                        &displaced_skin,
                    )
                    .ok()?,
                ),
            );
        }

        if features.emissive {
            let emissive_map = [
                (
                    ResolvedRenderEntryTextureType::Skin,
                    Some(ResolvedRenderEntryTextureType::Ears(
                        ResolvedRenderEntryEarsTextureType::EmissiveProcessedSkin,
                    )),
                    ResolvedRenderEntryTextureType::Ears(
                        ResolvedRenderEntryEarsTextureType::EmissiveSkin,
                    ),
                ),
                (
                    ResolvedRenderEntryTextureType::Ears(
                        ResolvedRenderEntryEarsTextureType::DisplacedSkin,
                    ),
                    None,
                    ResolvedRenderEntryTextureType::Ears(
                        ResolvedRenderEntryEarsTextureType::EmissiveDisplacedSkin,
                    ),
                ),
                (
                    ResolvedRenderEntryTextureType::Ears(ResolvedRenderEntryEarsTextureType::Wings),
                    Some(ResolvedRenderEntryTextureType::Ears(
                        ResolvedRenderEntryEarsTextureType::EmissiveProcessedWings,
                    )),
                    ResolvedRenderEntryTextureType::Ears(
                        ResolvedRenderEntryEarsTextureType::EmissiveWings,
                    ),
                ),
            ];

            fn apply_emissive(
                textures: &mut BTreeMap<ResolvedRenderEntryTextureType, MojangTexture>,
                source_texture: Option<&MojangTexture>,
                source: ResolvedRenderEntryTextureType,
                processed_target: Option<ResolvedRenderEntryTextureType>,
                target: ResolvedRenderEntryTextureType,
                palette: &EarsEmissivePalette,
            ) -> Option<()> {
                let texture = source_texture.or_else(|| textures.get(&source))?;

                let mut original_img = image::load_from_memory(texture.data()).ok()?.into_rgba8();

                let emissive_texture =
                    ears_rs::utils::apply_emissive_palette(&mut original_img, palette).ok()?;

                textures.insert(
                    target,
                    MojangTexture::new_unnamed_hashed(
                        create_png_from_bytes(
                            (emissive_texture.width(), emissive_texture.height()),
                            &emissive_texture,
                        )
                        .ok()?,
                    ),
                );

                if let Some(processed_target) = processed_target {
                    textures.insert(
                        processed_target,
                        MojangTexture::new_unnamed_hashed(
                            create_png_from_bytes(
                                (original_img.width(), original_img.height()),
                                &original_img,
                            )
                            .ok()?,
                        ),
                    );
                }

                Some(())
            }

            if let Ok(Some(palette)) = ears_rs::utils::extract_emissive_palette(&skin_image) {
                for (source, processed_target, target) in &emissive_map {
                    apply_emissive(
                        textures,
                        Some(skin_texture)
                            .filter(|_| *source == ResolvedRenderEntryTextureType::Skin),
                        *source,
                        *processed_target,
                        *target,
                        &palette,
                    );
                }
            }
        }

        Some(features)
    }

    pub async fn resolve(&self, request: &RenderRequest) -> Result<ResolvedRenderRequest> {
        let error = match self.resolve_raw(request).await {
            Ok(resolved) => return Ok(resolved),
            Err(error) => error,
        };

        let fallback_uuid = match &request.entry {
            RenderRequestEntry::GeyserPlayerUuid(u)
            | RenderRequestEntry::MojangOfflinePlayerUuid(u)
            | RenderRequestEntry::MojangPlayerUuid(u) => Some(*u),
            RenderRequestEntry::MojangPlayerName(name) => Some(Uuid::from_u128(xxh3_128(
                name.to_ascii_lowercase().as_bytes(),
            ))),
            RenderRequestEntry::TextureHash(_)
            | RenderRequestEntry::DefaultSkinTextureHash(_)
            | RenderRequestEntry::PlayerSkin(_, _) => None,
        };

        let Some(uuid) = fallback_uuid.filter(|_| {
            self.mojang_requests_client
                .mojank_config()
                .use_default_skins_when_missing
        }) else {
            return Err(error);
        };

        let reason = FallbackReason::from(error.failure_kind());
        warn!(?reason, %error, "Rendering a default skin for {:?}", request.entry);

        let optional_slim_model = request.model.map(|m| m == RenderRequestEntryModel::Alex);
        let (default_skin, is_default_slim) =
            DefaultSkinResolver::resolve_default_skin_for_uuid_parts(uuid, optional_slim_model);

        let mut new_request = request.clone();
        new_request.entry = RenderRequestEntry::default_skin_hash(default_skin, is_default_slim);

        let mut result = self.resolve_raw(&new_request).await?;
        result.model = if is_default_slim {
            RenderRequestEntryModel::Alex
        } else {
            RenderRequestEntryModel::Steve
        };
        result.source = TextureSource::Fallback(reason);

        Ok(result)
    }

    async fn resolve_raw(&self, request: &RenderRequest) -> Result<ResolvedRenderRequest> {
        let resolved = self
            .resolve_entry_textures(&request.entry)
            .await
            .map_err(|e| {
                MojangRequestError::UnableToResolveRenderRequestEntity(
                    Box::new(e),
                    request.entry.clone(),
                )
            })?;

        let source = match resolved.freshness {
            Freshness::Fresh => TextureSource::Current,
            Freshness::Stale => TextureSource::LastKnownGood,
        };
        let resolved_textures = resolved.value;

        let final_model = request
            .model
            .or(resolved_textures.model)
            .unwrap_or_default();

        // Load the textures into memory.
        let mut textures = BTreeMap::new();
        for (texture_type, texture) in resolved_textures.textures {
            #[cfg(feature = "ears")]
            {
                if let ResolvedRenderEntryTextureType::Ears(_) = texture_type {
                    if !request.features.contains(RenderRequestFeatures::Ears) {
                        continue;
                    }
                }
            }

            textures.insert(texture_type, texture.data);
        }

        Ok(ResolvedRenderRequest {
            model: final_model,
            textures,
            source,
        })
    }

    #[inline]
    pub(crate) async fn do_cache_clean_up(&self) -> Result<()> {
        self.model_cache.do_cache_clean_up().await
    }
}

/// Uses `stale` in place of a failed refresh, unless upstream definitively said the subject is gone.
fn stale_if_error<T>(fetched: Result<T>, stale: Option<Cached<T>>) -> Result<Cached<T>> {
    match (fetched, stale) {
        (Ok(value), _) => Ok(Cached::fresh(value)),
        (Err(error), Some(stale)) if !error.failure_kind().is_definitive_absence() => {
            warn!(%error, "Serving last known good result while upstream is unavailable");
            Ok(Cached::stale(stale.value))
        }
        (Err(error), _) => Err(error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackReason {
    /// Mojang answered that the player does not exist.
    PlayerNotFound,
    /// Mojang could not be asked or did not answer; the real skin may resolve on retry.
    UpstreamUnavailable,
}

impl From<FailureKind> for FallbackReason {
    fn from(kind: FailureKind) -> Self {
        if kind.is_definitive_absence() {
            Self::PlayerNotFound
        } else {
            Self::UpstreamUnavailable
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextureSource {
    /// Resolved from Mojang within its cache lifetime.
    Current,
    /// An expired resolution, served because Mojang could not answer the refresh.
    LastKnownGood,
    /// A default skin substituted for the requested player.
    Fallback(FallbackReason),
}

#[derive(Debug, Clone)]
pub struct ResolvedRenderRequest {
    pub model: RenderRequestEntryModel,
    #[debug(skip)]
    pub textures: BTreeMap<ResolvedRenderEntryTextureType, Vec<u8>>,
    pub source: TextureSource,
}
