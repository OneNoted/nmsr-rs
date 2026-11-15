use super::{bbmodel_export::internal_bbmodel_export, NMSRState};
use crate::{
    config::ModelCacheConfiguration,
    error::{RenderRequestError, Result},
    model::{
        request::{RenderRequest, RenderRequestMode},
        resolver::{FallbackReason, TextureSource},
    },
    routes::render_model::internal_render_model,
    routes::render_skin::internal_render_skin_or_cape,
};
use axum::{
    extract::State,
    http::HeaderValue,
    response::{IntoResponse, Response},
};
use http::{HeaderMap, HeaderName};
use hyper::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG};
use std::{borrow::Cow, time::Duration};
use tracing::instrument;
use xxhash_rust::xxh3::xxh3_128;

const IMAGE_PNG_MIME: &str = "image/png";

#[axum::debug_handler]
pub async fn render_post_warning() -> Result<Response> {
    return Err(RenderRequestError::WrongHttpMethodError("POST", "GET").into());
}

#[axum::debug_handler]
pub async fn render_get_warning() -> Result<Response> {
    return Err(RenderRequestError::WrongHttpMethodError("GET", "POST").into());
}

const NMSR_FALLBACK_HEADER: HeaderName = HeaderName::from_static("x-nmsr-fallback");
const NMSR_FALLBACK_REASON_HEADER: HeaderName = HeaderName::from_static("x-nmsr-fallback-reason");
const NMSR_STALE_HEADER: HeaderName = HeaderName::from_static("x-nmsr-stale");
const TRUE_VALUE: HeaderValue = HeaderValue::from_static("True");

const ONE_YEAR: Duration = Duration::from_hours(24 * 365);
/// A default skin for a nonexistent player may become wrong as soon as the name is registered.
const PLAYER_NOT_FOUND_MAX_AGE: Duration = Duration::from_mins(5);
/// A last known good skin is shown while Mojang is unavailable and should be replaced soon after it recovers.
const LAST_KNOWN_GOOD_MAX_AGE: Duration = Duration::from_mins(1);

#[axum::debug_handler]
#[instrument(skip(state))]
pub async fn render(
    state: State<NMSRState<'static>>,
    mut request: RenderRequest,
) -> Result<Response> {
    let resolved = state.resolver.resolve(&request).await?;
    let source = resolved.source;
    let cache_control = cache_control(&state.cache_config, &request, source);

    if request.mode.is_blockbench_export() {
        let mut response = internal_bbmodel_export(&state, request, resolved).await?;
        apply_source_headers(response.headers_mut(), &cache_control, source);

        return Ok(response);
    }

    let image = match request.mode {
        RenderRequestMode::Skin | RenderRequestMode::Cape => {
            internal_render_skin_or_cape(&request, resolved).await
        }
        _ => internal_render_model(&mut request, &state, &resolved).await,
    }?;

    Ok(image_response(image, &cache_control, source))
}

fn image_response(image: Vec<u8>, cache_control: &str, source: TextureSource) -> Response {
    let etag = format!("\"{:032x}\"", xxh3_128(&image));
    let mut response = image.into_response();
    let headers = response.headers_mut();

    headers.insert(CONTENT_TYPE, HeaderValue::from_static(IMAGE_PNG_MIME));

    if let Ok(etag) = HeaderValue::from_str(&etag) {
        headers.insert(ETAG, etag);
    }

    apply_source_headers(headers, cache_control, source);

    response
}

fn apply_source_headers(headers: &mut HeaderMap, cache_control: &str, source: TextureSource) {
    if let Ok(cache_control) = HeaderValue::from_str(cache_control) {
        headers.insert(CACHE_CONTROL, cache_control);
    }

    match source {
        TextureSource::Current => {}
        TextureSource::LastKnownGood => {
            headers.insert(NMSR_STALE_HEADER, TRUE_VALUE);
        }
        TextureSource::Fallback(reason) => {
            headers.insert(NMSR_FALLBACK_HEADER, TRUE_VALUE);
            headers.insert(
                NMSR_FALLBACK_REASON_HEADER,
                HeaderValue::from_static(match reason {
                    FallbackReason::PlayerNotFound => "player-not-found",
                    FallbackReason::UpstreamUnavailable => "upstream-unavailable",
                }),
            );
        }
    }
}

fn cache_control(
    config: &ModelCacheConfiguration,
    request: &RenderRequest,
    source: TextureSource,
) -> Cow<'static, str> {
    if request.mode.is_custom() {
        return "public, no-store".into();
    }

    let entry_duration = *config.get_cache_duration(&request.entry);
    let max_age = |duration: Duration| format!("public, max-age={}", duration.as_secs()).into();

    match source {
        TextureSource::Fallback(FallbackReason::UpstreamUnavailable) => "no-store".into(),
        TextureSource::Fallback(FallbackReason::PlayerNotFound) => {
            max_age(entry_duration.min(PLAYER_NOT_FOUND_MAX_AGE))
        }
        TextureSource::LastKnownGood => max_age(entry_duration.min(LAST_KNOWN_GOOD_MAX_AGE)),
        TextureSource::Current if entry_duration == Duration::MAX => {
            format!("public, max-age={}, immutable", ONE_YEAR.as_secs()).into()
        }
        TextureSource::Current => max_age(entry_duration.min(ONE_YEAR)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::request::{cache::CacheBias, entry::RenderRequestEntry};
    use enumset::EnumSet;
    use http::HeaderMap;

    fn request(mode: RenderRequestMode, entry: RenderRequestEntry) -> RenderRequest {
        RenderRequest::new_from_excluded_features(mode, entry, None, EnumSet::empty(), None)
    }

    fn player() -> RenderRequest {
        request(
            RenderRequestMode::HeadIso,
            RenderRequestEntry::MojangPlayerName("Alice".into()),
        )
    }

    fn config() -> ModelCacheConfiguration {
        ModelCacheConfiguration {
            resolve_cache_duration: Duration::from_mins(15),
            ..Default::default()
        }
    }

    #[test]
    fn transient_fallbacks_are_never_stored() {
        let source = TextureSource::Fallback(FallbackReason::UpstreamUnavailable);

        assert_eq!(cache_control(&config(), &player(), source), "no-store");
    }

    #[test]
    fn nonexistent_player_fallbacks_are_cached_briefly() {
        let source = TextureSource::Fallback(FallbackReason::PlayerNotFound);

        assert_eq!(
            cache_control(&config(), &player(), source),
            "public, max-age=300"
        );
    }

    #[test]
    fn last_known_good_is_cached_for_at_most_a_minute() {
        assert_eq!(
            cache_control(&config(), &player(), TextureSource::LastKnownGood),
            "public, max-age=60"
        );
    }

    #[test]
    fn current_skins_follow_the_resolve_duration_and_cache_biases() {
        assert_eq!(
            cache_control(&config(), &player(), TextureSource::Current),
            "public, max-age=900"
        );

        let entry = RenderRequestEntry::TextureHash("ab".repeat(32));
        let mut biased = config();
        biased
            .cache_biases
            .insert(entry.clone(), CacheBias::CacheIndefinitely);

        assert_eq!(
            cache_control(
                &biased,
                &request(RenderRequestMode::HeadIso, entry),
                TextureSource::Current
            ),
            "public, max-age=31536000, immutable"
        );
    }

    fn headers(image: &[u8], source: TextureSource) -> HeaderMap {
        image_response(image.to_vec(), "no-store", source)
            .headers()
            .clone()
    }

    #[test]
    fn etag_is_a_strong_validator_of_the_image_bytes() {
        let custom = headers(b"custom skin render", TextureSource::Current);
        let again = headers(b"custom skin render", TextureSource::Current);
        let default = headers(
            b"default skin render",
            TextureSource::Fallback(FallbackReason::UpstreamUnavailable),
        );

        let etag = custom[ETAG].to_str().unwrap();
        assert!(etag.starts_with('"') && etag.ends_with('"'));
        assert_eq!(custom[ETAG], again[ETAG]);
        assert_ne!(custom[ETAG], default[ETAG]);
    }

    #[test]
    fn responses_say_where_their_textures_came_from() {
        let current = headers(b"png", TextureSource::Current);
        assert!(!current.contains_key(NMSR_FALLBACK_HEADER));
        assert!(!current.contains_key(NMSR_STALE_HEADER));

        let stale = headers(b"png", TextureSource::LastKnownGood);
        assert_eq!(stale[NMSR_STALE_HEADER], "True");
        assert!(!stale.contains_key(NMSR_FALLBACK_HEADER));

        let missing = headers(
            b"png",
            TextureSource::Fallback(FallbackReason::PlayerNotFound),
        );
        assert_eq!(missing[NMSR_FALLBACK_HEADER], "True");
        assert_eq!(missing[NMSR_FALLBACK_REASON_HEADER], "player-not-found");

        let unavailable = headers(
            b"png",
            TextureSource::Fallback(FallbackReason::UpstreamUnavailable),
        );
        assert_eq!(
            unavailable[NMSR_FALLBACK_REASON_HEADER],
            "upstream-unavailable"
        );
    }
}
