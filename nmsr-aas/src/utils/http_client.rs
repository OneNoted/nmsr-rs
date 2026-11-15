use axum::http::{HeaderName, HeaderValue};
use http::{header::RETRY_AFTER, HeaderMap, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::{body::Bytes, Method, Request};
use hyper_tls::{native_tls::TlsConnector, HttpsConnector};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{
    collections::HashMap,
    future::{ready, Ready},
    net::IpAddr,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant, SystemTime},
};
use tower::{
    balance::p2c::Balance,
    buffer::Buffer,
    discover::ServiceList,
    limit::RateLimit,
    load::{CompleteOnResponse, PeakEwmaDiscover},
    retry::{Policy, Retry},
    timeout::{Timeout, TimeoutLayer},
    Service, ServiceBuilder, ServiceExt,
};
use tower_http::{
    classify::{ServerErrorsAsFailures, SharedClassifier},
    set_header::{SetRequestHeader, SetRequestHeaderLayer},
    trace::{
        DefaultMakeSpan, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, Trace, TraceLayer,
    },
};
use tracing::{instrument, trace, Span};

use crate::error::{MojangRequestError, MojangRequestResult};

pub const USER_AGENT: &str = concat!(
    "NMSR-as-a-Service/",
    env!("VERGEN_IS_LITERALLY_TRASH__IT_DOES_NOT_WORK_AND_IT_ACTUALLY_BREAKS_EVERY_TIME_I_UPDATE_IT__LIKE_SERIOUSLY_HOW_IS_THAT_POSSIBLE___STOP_CHANGING_THE_DAMN_IMPLEMENTATION___I_JUST_WANT_A_STUPID_GIT_HASH"),
);

pub(crate) type SyncBody =
    http_body_util::combinators::BoxBody<Bytes, hyper_util::client::legacy::Error>;

pub(crate) type SyncBodyClient = Client<HttpsConnector<HttpConnector>, SyncBody>;

pub(crate) type NmsrTraceLayer = Trace<
    SetRequestHeader<LogRequestIpService<SyncBodyClient>, HeaderValue>,
    SharedClassifier<ServerErrorsAsFailures>,
    DefaultMakeSpan,
    DefaultOnRequest,
    DefaultOnResponse,
    (),
    (),
    DefaultOnFailure,
>;

pub(crate) type HttpClientInnerService =
    Buffer<
        Request<SyncBody>,
        <RateLimit<Retry<MojankRetryPolicy, Timeout<NmsrTraceLayer>>> as Service<
            Request<SyncBody>,
        >>::Future,
    >;

pub enum HttpTransport {
    SingleIp {
        inner: HttpClientInnerService,
    },
    LoadBalanced {
        inner:
            Buffer<
                Request<SyncBody>,
                <Balance<
                    PeakEwmaDiscover<ServiceList<Vec<HttpClientInnerService>>>,
                    Request<SyncBody>,
                > as Service<Request<SyncBody>>>::Future,
            >,
    },
}

pub struct NmsrHttpClient {
    transport: HttpTransport,
    backoff: UpstreamBackoff,
}

/// Wait used when an upstream answers 429 without a usable `Retry-After`.
const DEFAULT_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(30);
/// Upper bound so a hostile or buggy `Retry-After` cannot disable a host for long.
const MAX_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(5 * 60);

/// Per-host deadlines before which requests are refused locally instead of sent,
/// so a rate-limited upstream is not asked again until it said it would answer.
#[derive(Default)]
struct UpstreamBackoff(Mutex<HashMap<String, Instant>>);

impl UpstreamBackoff {
    fn remaining(&self, host: &str) -> Option<Duration> {
        let mut deadlines = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let remaining = deadlines
            .get(host)
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));

        match remaining {
            Some(remaining) if !remaining.is_zero() => Some(remaining),
            Some(_) => {
                deadlines.remove(host);
                None
            }
            None => None,
        }
    }

    fn defer(&self, host: &str, wait: Duration) {
        let deadline = Instant::now() + wait;
        let mut deadlines = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = deadlines.entry(host.to_owned()).or_insert(deadline);
        *entry = (*entry).max(deadline);
    }
}

/// Reads `Retry-After` as delta-seconds or an HTTP-date, bounded by [`MAX_RATE_LIMIT_BACKOFF`].
fn retry_after(headers: &HeaderMap, now: SystemTime) -> Duration {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .and_then(|value| {
            value.parse().map(Duration::from_secs).ok().or_else(|| {
                httpdate::parse_http_date(value)
                    .ok()
                    .map(|at| at.duration_since(now).unwrap_or_default())
            })
        })
        .unwrap_or(DEFAULT_RATE_LIMIT_BACKOFF)
        .min(MAX_RATE_LIMIT_BACKOFF)
}

impl NmsrHttpClient {
    pub fn new(
        rate_limit_per_second: u64,
        request_timeout_seconds: u64,
        request_retries_count: usize,
        client_ips: &[IpAddr],
        user_agent: Option<&str>,
    ) -> Self {
        Self {
            transport: create_http_client(
                rate_limit_per_second,
                request_timeout_seconds,
                request_retries_count,
                client_ips,
                user_agent,
            ),
            backoff: UpstreamBackoff::default(),
        }
    }

    /// Fetches `url`, mapping 204 and 404 to `not_found()` and every other non-200 answer
    /// to an upstream error that callers treat as transient.
    #[instrument(skip(self, parent_span, not_found), parent = parent_span, err)]
    pub(crate) async fn do_request(
        &self,
        url: &str,
        method: Method,
        parent_span: &Span,
        not_found: impl FnOnce() -> MojangRequestError,
    ) -> MojangRequestResult<Bytes> {
        let request = Request::builder()
            .method(method)
            .uri(url)
            .body(SyncBody::new(Empty::new().map_err(|e| {
                unreachable!("Empty body should not error: {}", e)
            })))?;

        let host = request
            .uri()
            .authority()
            .map_or_else(String::new, ToString::to_string);

        if let Some(retry_after) = self.backoff.remaining(&host) {
            return Err(MojangRequestError::UpstreamRateLimited { host, retry_after });
        }

        let response = match &self.transport {
            HttpTransport::SingleIp { inner } => {
                inner
                    .clone()
                    .ready()
                    .await
                    .map_err(MojangRequestError::BoxedRequestError)?
                    .call(request)
                    .await
            }
            HttpTransport::LoadBalanced { inner } => {
                inner
                    .clone()
                    .ready()
                    .await
                    .map_err(MojangRequestError::BoxedRequestError)?
                    .call(request)
                    .await
            }
        }
        .map_err(MojangRequestError::BoxedRequestError)?;

        match response.status() {
            StatusCode::OK => {}
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => return Err(not_found()),
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = retry_after(response.headers(), SystemTime::now());
                self.backoff.defer(&host, retry_after);
                return Err(MojangRequestError::UpstreamRateLimited { host, retry_after });
            }
            status => return Err(MojangRequestError::UpstreamStatus { host, status }),
        }

        response
            .into_body()
            .collect()
            .await
            .map(|b| b.to_bytes())
            .map_err(|e| MojangRequestError::BoxedRequestError(Box::new(e)))
    }
}

fn create_http_client(
    rate_limit_per_second: u64,
    request_timeout_seconds: u64,
    request_retries_count: usize,
    client_ips: &[IpAddr],
    user_agent: Option<&str>,
) -> HttpTransport {
    if client_ips.is_empty() {
        create_http_client_internal(
            rate_limit_per_second,
            request_timeout_seconds,
            request_retries_count,
            None,
            user_agent,
        )
    } else if client_ips.len() == 1 {
        create_http_client_internal(
            rate_limit_per_second,
            request_timeout_seconds,
            request_retries_count,
            Some(client_ips[0]),
            user_agent,
        )
    } else {
        let clients = client_ips
            .into_iter()
            .map(|ip| {
                create_http_client_internal(
                    rate_limit_per_second,
                    request_timeout_seconds,
                    request_retries_count,
                    Some(*ip),
                    user_agent,
                )
            })
            .flat_map(|svc| {
                if let HttpTransport::SingleIp { inner } = svc {
                    Some(inner)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let discover = ServiceList::new(clients);
        let load = PeakEwmaDiscover::new(
            discover,
            Duration::from_secs(1),
            Duration::from_millis(50),
            CompleteOnResponse::default(),
        );
        let balanced = Balance::new(load);

        let balanced = ServiceBuilder::new()
            .buffer(rate_limit_per_second.saturating_mul(2) as usize)
            .check_clone()
            .service(balanced);

        HttpTransport::LoadBalanced { inner: balanced }
    }
}

fn create_http_client_internal(
    rate_limit_per_second: u64,
    request_timeout_seconds: u64,
    request_retries_count: usize,
    client_ip: Option<IpAddr>,
    user_agent: Option<&str>,
) -> HttpTransport {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    http.set_local_address(client_ip);

    let tls = TlsConnector::new().expect("Expected TLS connector to be valid");

    let https = HttpsConnector::from((http, tls.into()));

    // A new higher level client from hyper is in the works, so we gotta use the legacy one
    let client = Client::builder(TokioExecutor::new())
        .http2_keep_alive_while_idle(true)
        .build(https);

    let tracing = TraceLayer::new_for_http().on_body_chunk(()).on_eos(());
    let ip_layer = LogRequestIpLayer::new(client_ip);

    let service = ServiceBuilder::new()
        .buffer(rate_limit_per_second.saturating_mul(2) as usize)
        .rate_limit(rate_limit_per_second, Duration::from_secs(1))
        .layer(CloneRetryLayer::new(MojankRetryPolicy::new(
            request_retries_count, /* Retry attempts */
        )))
        .layer(TimeoutLayer::new(Duration::from_secs(
            request_timeout_seconds,
        )))
        .layer(tracing)
        .layer(SetRequestHeaderLayer::overriding(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_str(user_agent.unwrap_or(USER_AGENT))
                .expect("Expected user-agent to be valid"),
        ))
        .layer(ip_layer)
        .check_clone()
        .service(client);

    HttpTransport::SingleIp { inner: service }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct MojankRetryPolicy {
    attempts: usize,
}

impl MojankRetryPolicy {
    pub fn new(attempts: usize) -> Self {
        Self { attempts }
    }
}

impl<P, Res> Policy<Request<SyncBody>, Res, P> for MojankRetryPolicy {
    type Future = Ready<()>;

    fn retry(
        &mut self,
        _req: &mut Request<SyncBody>,
        result: &mut Result<Res, P>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(_) => {
                if self.attempts > 0 {
                    self.attempts -= 1;
                    Some(ready(()))
                } else {
                    None
                }
            }
        }
    }

    fn clone_request(&mut self, req: &Request<SyncBody>) -> Option<Request<SyncBody>> {
        let method = req.method().clone();
        let uri = req.uri().clone();

        let mut builder = Request::builder().method(method).uri(uri);

        for (key, value) in req.headers() {
            builder = builder.header(key, value);
        }

        builder
            .body(SyncBody::new(Empty::new().map_err(|e| {
                unreachable!("Empty body should not error: {}", e)
            })))
            .ok()
    }
}

/// Clone retry layer
#[derive(Clone, Debug)]
pub struct CloneRetryLayer<P> {
    policy: P,
}

impl<P> CloneRetryLayer<P> {
    /// Create a new [`CloneRetryLayer`] from a retry policy
    pub fn new(policy: P) -> Self {
        CloneRetryLayer { policy }
    }
}

impl<P, S> tower::Layer<S> for CloneRetryLayer<P>
where
    P: Clone,
{
    type Service = Retry<P, S>;

    fn layer(&self, service: S) -> Self::Service {
        let policy = self.policy.clone();
        Retry::new(policy, service)
    }
}

#[derive(Clone, Debug, Copy)]
pub struct LogRequestIpLayer {
    ip: Option<IpAddr>,
}

impl LogRequestIpLayer {
    pub fn new(ip: Option<IpAddr>) -> Self {
        Self { ip }
    }
}

impl<S> tower::Layer<S> for LogRequestIpLayer {
    type Service = LogRequestIpService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LogRequestIpService {
            ip: self.ip,
            inner: service,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogRequestIpService<S> {
    ip: Option<IpAddr>,
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for LogRequestIpService<S>
where
    S: Service<Request<ReqBody>, Response = http::Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if let Some(ip) = self.ip {
            trace!("Outgoing request from IP: {}", ip);
        }
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(retry_after: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_str(retry_after).unwrap());
        headers
    }

    fn now() -> SystemTime {
        httpdate::parse_http_date("Sun, 15 Nov 2026 14:00:00 GMT").unwrap()
    }

    #[test]
    fn retry_after_uses_delta_seconds_and_caps_them() {
        assert_eq!(retry_after(&headers("7"), now()), Duration::from_secs(7));
        assert_eq!(
            retry_after(&headers("86400"), now()),
            MAX_RATE_LIMIT_BACKOFF
        );
    }

    #[test]
    fn retry_after_accepts_http_dates() {
        let at = |date| retry_after(&headers(date), now());

        assert_eq!(at("Sun, 15 Nov 2026 14:00:45 GMT"), Duration::from_secs(45));
        assert_eq!(at("Sun, 15 Nov 2026 13:59:00 GMT"), Duration::ZERO);
        assert_eq!(at("Mon, 16 Nov 2026 14:00:00 GMT"), MAX_RATE_LIMIT_BACKOFF);
    }

    #[test]
    fn retry_after_defaults_when_missing_or_unparseable() {
        assert_eq!(
            retry_after(&HeaderMap::new(), now()),
            DEFAULT_RATE_LIMIT_BACKOFF
        );
        assert_eq!(
            retry_after(&headers("soon"), now()),
            DEFAULT_RATE_LIMIT_BACKOFF
        );
    }

    #[test]
    fn backoff_blocks_only_the_deferred_host_until_its_deadline() {
        let backoff = UpstreamBackoff::default();
        backoff.defer("api.mojang.com", Duration::from_secs(30));

        assert!(backoff.remaining("api.mojang.com").is_some());
        assert!(backoff.remaining("sessionserver.mojang.com").is_none());

        backoff.defer("textures.minecraft.net", Duration::ZERO);
        assert!(backoff.remaining("textures.minecraft.net").is_none());
    }

    #[test]
    fn backoff_keeps_the_later_deadline() {
        let backoff = UpstreamBackoff::default();
        backoff.defer("api.mojang.com", Duration::from_secs(60));
        backoff.defer("api.mojang.com", Duration::from_secs(1));

        assert!(backoff.remaining("api.mojang.com").unwrap() > Duration::from_secs(30));
    }
}
