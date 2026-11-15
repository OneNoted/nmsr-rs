use super::*;
use crate::{
    config::{ModelCacheConfiguration, MojankConfiguration},
    model::request::RenderRequestMode,
    utils::png::create_png_from_bytes,
};
use axum::{
    extract::{Path, State},
    http::{header::RETRY_AFTER, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use enumset::EnumSet;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

const NAME: &str = "Alice";
const ID: &str = "5b2a1f6e0d3c4e8a9f7b1a2b3c4d5e6f";
const SKIN_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone, Copy)]
enum Upstream {
    Healthy,
    RateLimited { retry_after_secs: u64 },
    Missing,
    Skinless,
}

struct Fake {
    mode: Mutex<Upstream>,
    base: Mutex<String>,
    name_lookups: AtomicUsize,
}

impl Fake {
    fn mode(&self) -> Upstream {
        *self.mode.lock().unwrap()
    }

    fn set(&self, mode: Upstream) {
        *self.mode.lock().unwrap() = mode;
    }
}

fn rate_limited(retry_after_secs: u64) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(RETRY_AFTER, retry_after_secs.to_string())],
    )
        .into_response()
}

async fn name_lookup(State(fake): State<Arc<Fake>>, Path(_): Path<String>) -> Response {
    fake.name_lookups.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(20)).await;
    match fake.mode() {
        Upstream::Healthy | Upstream::Skinless => {
            Json(json!({ "id": ID, "name": NAME })).into_response()
        }
        Upstream::RateLimited { retry_after_secs } => rate_limited(retry_after_secs),
        Upstream::Missing => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn session_profile(State(fake): State<Arc<Fake>>, Path(_): Path<String>) -> Response {
    tokio::time::sleep(Duration::from_millis(20)).await;
    let skins = match fake.mode() {
        Upstream::Healthy => {
            let base = fake.base.lock().unwrap().clone();
            json!({ "SKIN": { "url": format!("{base}/texture/{SKIN_HASH}") } })
        }
        Upstream::Skinless => json!({}),
        Upstream::RateLimited { retry_after_secs } => return rate_limited(retry_after_secs),
        Upstream::Missing => return StatusCode::NO_CONTENT.into_response(),
    };
    let value = STANDARD.encode(json!({ "textures": skins }).to_string());

    Json(json!({
        "id": ID,
        "name": NAME,
        "properties": [{ "name": "textures", "value": value }],
    }))
    .into_response()
}

async fn texture(Path(hash): Path<String>) -> Response {
    let shade = if hash == SKIN_HASH { 200 } else { 50 };
    create_png_from_bytes((64, 64), &[shade; 64 * 64 * 4])
        .unwrap()
        .into_response()
}

struct Harness {
    fake: Arc<Fake>,
    resolver: RenderRequestResolver,
    cache_dir: std::path::PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.cache_dir);
    }
}

/// Resolver against a local fake Mojang whose cache entries are stale as soon as they are written,
/// so every request attempts a refresh and exercises stale-if-error.
async fn harness() -> Harness {
    harness_fresh_for(Duration::ZERO).await
}

async fn harness_fresh_for(fresh_for: Duration) -> Harness {
    let fake = Arc::new(Fake {
        mode: Mutex::new(Upstream::Healthy),
        base: Mutex::new(String::new()),
        name_lookups: AtomicUsize::new(0),
    });

    let app = Router::new()
        .route("/users/profiles/minecraft/{name}", get(name_lookup))
        .route("/session/minecraft/profile/{id}", get(session_profile))
        .route("/texture/{hash}", get(texture))
        .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    *fake.base.lock().unwrap() = base.clone();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mojank = MojankConfiguration {
        session_server: base.clone(),
        mojang_api_server: base.clone(),
        textures_server: base,
        default_skins_use_official_textures_server: false,
        session_server_timeout: 5,
        session_server_retries: 0,
        ..Default::default()
    };
    let caching = ModelCacheConfiguration {
        resolve_cache_duration: fresh_for,
        username_cache_duration: fresh_for,
        ..Default::default()
    };

    let cache_dir = std::env::temp_dir().join(format!("nmsr-resolver-test-{}", Uuid::new_v4()));
    let cache = ModelCache::new(cache_dir.clone(), caching).await.unwrap();
    let client = MojangClient::new(Arc::new(mojank)).unwrap();

    Harness {
        fake,
        resolver: RenderRequestResolver::new(cache, Arc::new(client)),
        cache_dir,
    }
}

fn by_name() -> RenderRequest {
    request(RenderRequestEntry::MojangPlayerName(NAME.into()))
}

fn by_uuid() -> RenderRequest {
    request(RenderRequestEntry::MojangPlayerUuid(
        Uuid::parse_str(ID).unwrap(),
    ))
}

fn request(entry: RenderRequestEntry) -> RenderRequest {
    RenderRequest::new_from_excluded_features(
        RenderRequestMode::Skin,
        entry,
        None,
        EnumSet::empty(),
        None,
    )
}

async fn resolve(h: &Harness, request: &RenderRequest) -> (TextureSource, u8) {
    let resolved = h.resolver.resolve(request).await.unwrap();
    let skin = &resolved.textures[&ResolvedRenderEntryTextureType::Skin];
    let pixels = image::load_from_memory(skin).unwrap().into_rgba8();

    (resolved.source, pixels.get_pixel(0, 0).0[0])
}

const CUSTOM_SKIN: u8 = 200;
const DEFAULT_SKIN: u8 = 50;

#[tokio::test]
async fn rate_limited_cold_player_gets_a_transient_default() {
    let h = harness().await;
    h.fake.set(Upstream::RateLimited {
        retry_after_secs: 0,
    });

    let unavailable = TextureSource::Fallback(FallbackReason::UpstreamUnavailable);
    assert_eq!(resolve(&h, &by_name()).await, (unavailable, DEFAULT_SKIN));
    assert_eq!(resolve(&h, &by_uuid()).await, (unavailable, DEFAULT_SKIN));
}

#[tokio::test]
async fn nonexistent_player_gets_a_not_found_default() {
    let h = harness().await;
    h.fake.set(Upstream::Missing);

    let missing = TextureSource::Fallback(FallbackReason::PlayerNotFound);
    assert_eq!(resolve(&h, &by_name()).await, (missing, DEFAULT_SKIN));
    assert_eq!(resolve(&h, &by_uuid()).await, (missing, DEFAULT_SKIN));
}

#[tokio::test]
async fn known_player_keeps_their_skin_while_rate_limited_and_recovers() {
    let h = harness().await;
    assert_eq!(
        resolve(&h, &by_name()).await,
        (TextureSource::Current, CUSTOM_SKIN)
    );

    h.fake.set(Upstream::RateLimited {
        retry_after_secs: 0,
    });
    let stale = (TextureSource::LastKnownGood, CUSTOM_SKIN);
    assert_eq!(resolve(&h, &by_name()).await, stale);
    assert_eq!(resolve(&h, &by_uuid()).await, stale);

    h.fake.set(Upstream::Healthy);
    assert_eq!(
        resolve(&h, &by_name()).await,
        (TextureSource::Current, CUSTOM_SKIN)
    );
}

#[tokio::test]
async fn player_without_a_skin_gets_their_own_default_as_a_current_render() {
    let h = harness().await;
    h.fake.set(Upstream::Skinless);

    let by_name = h.resolver.resolve(&by_name()).await.unwrap();
    let by_uuid = h.resolver.resolve(&by_uuid()).await.unwrap();

    assert_eq!(by_name.source, TextureSource::Current);
    assert_eq!(by_name.model, by_uuid.model);
    assert_eq!(
        by_name.textures[&ResolvedRenderEntryTextureType::Skin],
        by_uuid.textures[&ResolvedRenderEntryTextureType::Skin]
    );
}

#[tokio::test]
async fn definitive_absence_replaces_the_last_known_good_skin_for_good() {
    let h = harness().await;
    resolve(&h, &by_name()).await;

    h.fake.set(Upstream::Missing);
    let missing = (
        TextureSource::Fallback(FallbackReason::PlayerNotFound),
        DEFAULT_SKIN,
    );
    assert_eq!(resolve(&h, &by_name()).await, missing);
    assert_eq!(resolve(&h, &by_uuid()).await, missing);

    h.fake.set(Upstream::RateLimited {
        retry_after_secs: 0,
    });
    let unavailable = (
        TextureSource::Fallback(FallbackReason::UpstreamUnavailable),
        DEFAULT_SKIN,
    );
    assert_eq!(resolve(&h, &by_name()).await, unavailable);
    assert_eq!(resolve(&h, &by_uuid()).await, unavailable);
}

#[tokio::test]
async fn rate_limited_host_is_not_asked_again_before_retry_after() {
    let h = harness().await;
    h.fake.set(Upstream::RateLimited {
        retry_after_secs: 60,
    });

    for _ in 0..3 {
        resolve(&h, &by_name()).await;
    }

    assert_eq!(h.fake.name_lookups.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_identical_refreshes_all_get_the_custom_skin() {
    let h = Arc::new(harness().await);
    resolve(&h, &by_name()).await;

    let mut refreshes = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let h = h.clone();
        refreshes.spawn(async move { resolve(&h, &by_uuid()).await });
    }

    for refreshed in refreshes.join_all().await {
        assert_eq!(refreshed, (TextureSource::Current, CUSTOM_SKIN));
    }
}

#[tokio::test]
async fn concurrent_identical_cold_requests_share_one_upstream_lookup() {
    let h = Arc::new(harness_fresh_for(Duration::from_secs(60)).await);

    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let h = h.clone();
        requests.spawn(async move { resolve(&h, &by_name()).await });
    }

    for resolved in requests.join_all().await {
        assert_eq!(resolved, (TextureSource::Current, CUSTOM_SKIN));
    }
    assert_eq!(h.fake.name_lookups.load(Ordering::SeqCst), 1);
}
