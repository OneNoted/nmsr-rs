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
    future::{ready, Future, Ready},
    net::IpAddr,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant, SystemTime},
};
use tower::{
    balance::p2c::Balance,
    buffer::Buffer,
    discover::ServiceList,
    layer::util::{Identity, Stack},
    limit::RateLimit,
    load::{CompleteOnResponse, PeakEwmaDiscover},
    retry::{Policy, Retry},
    timeout::{Timeout, TimeoutLayer},
    BoxError, Service, ServiceBuilder, ServiceExt,
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

pub(crate) type HttpClientInnerService = Buffer<
    Request<SyncBody>,
    <RateLimit<Retry<MojankRetryPolicy, BackoffGate<Timeout<NmsrTraceLayer>>>> as Service<
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
    backoff: Arc<UpstreamBackoff>,
    /// Bounds the whole exchange; the transport timeout alone ends once response headers arrive.
    timeout: Duration,
}

/// Wait used when an upstream answers 429 without a usable `Retry-After`.
const DEFAULT_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(30);
/// Upper bound so a buggy `Retry-After` cannot disable a host indefinitely.
const MAX_RATE_LIMIT_BACKOFF: Duration = Duration::from_hours(1);

/// Per-host deadlines before which requests are refused locally instead of sent,
/// so a rate-limited upstream is not asked again until it said it would answer.
#[derive(Default)]
pub struct UpstreamBackoff(Mutex<HashMap<String, Instant>>);

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
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(host.to_owned())
            .and_modify(|current| *current = (*current).max(deadline))
            .or_insert(deadline);
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
        let backoff = Arc::new(UpstreamBackoff::default());

        Self {
            transport: create_http_client(
                rate_limit_per_second,
                request_timeout_seconds,
                request_retries_count,
                client_ips,
                user_agent,
                &backoff,
            ),
            backoff,
            timeout: Duration::from_secs(request_timeout_seconds),
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

        tokio::time::timeout(self.timeout, self.exchange(request, host, not_found))
            .await
            .map_err(|elapsed| MojangRequestError::BoxedRequestError(Box::new(elapsed)))?
    }

    async fn exchange(
        &self,
        request: Request<SyncBody>,
        host: String,
        not_found: impl FnOnce() -> MojangRequestError,
    ) -> MojangRequestResult<Bytes> {
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
        .map_err(|error| match error.downcast::<MojangRequestError>() {
            Ok(error) => *error,
            Err(error) => MojangRequestError::BoxedRequestError(error),
        })?;

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
    backoff: &Arc<UpstreamBackoff>,
) -> HttpTransport {
    if client_ips.is_empty() {
        create_http_client_internal(
            rate_limit_per_second,
            request_timeout_seconds,
            request_retries_count,
            None,
            user_agent,
            backoff,
        )
    } else if client_ips.len() == 1 {
        create_http_client_internal(
            rate_limit_per_second,
            request_timeout_seconds,
            request_retries_count,
            Some(client_ips[0]),
            user_agent,
            backoff,
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
                    backoff,
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
    backoff: &Arc<UpstreamBackoff>,
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
        .layer(attempt_layers(request_retries_count, backoff).into_inner())
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

impl<Res> Policy<Request<SyncBody>, Res, BoxError> for MojankRetryPolicy {
    type Future = Ready<()>;

    fn retry(
        &mut self,
        _req: &mut Request<SyncBody>,
        result: &mut Result<Res, BoxError>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            // Refused locally by the backoff gate; retrying now would only be refused again.
            Err(error) if error.is::<MojangRequestError>() => None,
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

/// Retries wrap the backoff gate, so every attempt is checked against the backoff.
fn attempt_layers(
    retries: usize,
    backoff: &Arc<UpstreamBackoff>,
) -> ServiceBuilder<Stack<BackoffGateLayer, Stack<CloneRetryLayer<MojankRetryPolicy>, Identity>>> {
    ServiceBuilder::new()
        .layer(CloneRetryLayer::new(MojankRetryPolicy::new(retries)))
        .layer(BackoffGateLayer(backoff.clone()))
}

/// Checks the backoff on every attempt that has passed the rate limiter, retries included, so requests
/// that were already queued or in flight when an upstream answered 429 are not sent before its `Retry-After`.
#[derive(Clone)]
pub struct BackoffGateLayer(Arc<UpstreamBackoff>);

impl<S> tower::Layer<S> for BackoffGateLayer {
    type Service = BackoffGate<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BackoffGate {
            backoff: self.0.clone(),
            inner,
        }
    }
}

#[derive(Clone)]
pub struct BackoffGate<S> {
    backoff: Arc<UpstreamBackoff>,
    inner: S,
}

impl<S, ReqBody> Service<Request<ReqBody>> for BackoffGate<S>
where
    S: Service<Request<ReqBody>, Error = BoxError>,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let host = req
            .uri()
            .authority()
            .map_or_else(String::new, ToString::to_string);

        match self.backoff.remaining(&host) {
            Some(retry_after) => Box::pin(ready(Err(Box::new(
                MojangRequestError::UpstreamRateLimited { host, retry_after },
            ) as BoxError))),
            None => Box::pin(self.inner.call(req)),
        }
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
    use tower::Layer;

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
        assert_eq!(at("Sun, 15 Nov 2026 14:20:00 GMT"), Duration::from_mins(20));
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

    #[tokio::test]
    async fn gate_refuses_requests_to_a_backed_off_host_without_sending_them() {
        let backoff = Arc::new(UpstreamBackoff::default());
        backoff.defer("api.mojang.com", Duration::from_secs(60));
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = sent.clone();
        let mut gate = BackoffGateLayer(backoff).layer(tower::service_fn(move |_: Request<()>| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ready(Ok::<_, BoxError>(()))
        }));
        let request = |url| Request::get(url).body(()).unwrap();

        let refused = gate
            .ready()
            .await
            .unwrap()
            .call(request(
                "https://api.mojang.com/users/profiles/minecraft/Alice",
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            refused.downcast_ref::<MojangRequestError>(),
            Some(MojangRequestError::UpstreamRateLimited { .. })
        ));

        gate.ready()
            .await
            .unwrap()
            .call(request(
                "https://sessionserver.mojang.com/session/minecraft/profile/x",
            ))
            .await
            .unwrap();
        assert_eq!(sent.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_are_not_sent_to_a_host_that_backed_off_meanwhile() {
        let backoff = Arc::new(UpstreamBackoff::default());
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (counter, concurrent_429) = (sent.clone(), backoff.clone());
        let mut service =
            attempt_layers(3, &backoff).service(tower::service_fn(move |_: Request<SyncBody>| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                concurrent_429.defer("api.mojang.com", Duration::from_secs(60));
                ready(Err::<(), BoxError>("connection reset".into()))
            }));
        let request = Request::get("https://api.mojang.com/users/profiles/minecraft/Alice")
            .body(SyncBody::new(Empty::new().map_err(|e| match e {})))
            .unwrap();

        let error = service
            .ready()
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<MojangRequestError>(),
            Some(MojangRequestError::UpstreamRateLimited { .. })
        ));
        assert_eq!(sent.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn local_backoff_refusals_are_not_retried_but_transport_errors_are() {
        let mut policy = MojankRetryPolicy::new(3);
        let mut request = Request::get("https://api.mojang.com/")
            .body(SyncBody::new(Empty::new().map_err(|e| match e {})))
            .unwrap();
        let refused = MojangRequestError::UpstreamRateLimited {
            host: "api.mojang.com".to_owned(),
            retry_after: Duration::from_secs(60),
        };

        let mut refusal: Result<(), BoxError> = Err(Box::new(refused));
        assert!(policy.retry(&mut request, &mut refusal).is_none());

        let mut reset: Result<(), BoxError> = Err("connection reset".into());
        assert!(policy.retry(&mut request, &mut reset).is_some());
    }

    #[tokio::test]
    async fn a_response_body_that_stalls_times_out() {
        let app = axum::Router::new().route(
            "/stall",
            axum::routing::get(|| async {
                axum::body::Body::from_stream(
                    tokio_stream::pending::<Result<Bytes, std::io::Error>>(),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/stall", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = NmsrHttpClient::new(10, 1, 0, &[], None);
        let span = Span::none();
        let request = client.do_request(&url, Method::GET, &span, || unreachable!());

        let result = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("the client's own timeout should end the request");
        assert!(matches!(
            result,
            Err(MojangRequestError::BoxedRequestError(_))
        ));
    }

    #[test]
    fn backoff_keeps_the_later_deadline() {
        let backoff = UpstreamBackoff::default();
        backoff.defer("api.mojang.com", Duration::from_secs(60));
        backoff.defer("api.mojang.com", Duration::from_secs(1));

        assert!(backoff.remaining("api.mojang.com").unwrap() > Duration::from_secs(30));
    }
}
