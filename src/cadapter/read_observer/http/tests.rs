//! Real pinned AWS SDK dispatch/retry tests, not calls to counters by hand.
use super::*;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region, retry::RetryConfig},
};
use aws_smithy_runtime_api::{client::orchestrator::HttpResponse, http::StatusCode};
use http_body_util::BodyExt;
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
struct ScriptBody {
    frames: VecDeque<Result<Frame<Bytes>, aws_smithy_runtime_api::box_error::BoxError>>,
    pending: bool,
    hint: u64,
}
impl Body for ScriptBody {
    type Data = Bytes;
    type Error = aws_smithy_runtime_api::box_error::BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if let Some(frame) = self.frames.pop_front() {
            Poll::Ready(Some(frame))
        } else if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
    fn is_end_stream(&self) -> bool {
        !self.pending && self.frames.is_empty()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.hint)
    }
}
fn response(
    status: u16,
    frames: Vec<Result<Frame<Bytes>, aws_smithy_runtime_api::box_error::BoxError>>,
    pending: bool,
    length: u64,
) -> HttpResponse {
    let body = SdkBody::from_body_1_x(ScriptBody {
        frames: frames.into(),
        pending,
        hint: length,
    });
    let mut response = HttpResponse::new(StatusCode::try_from(status).unwrap(), body);
    response
        .headers_mut()
        .insert("content-length", length.to_string());
    response.headers_mut().insert(
        "content-type",
        if status >= 400 {
            "application/xml"
        } else {
            "application/octet-stream"
        },
    );
    response
}
fn complete(status: u16, bytes: &'static [u8]) -> HttpResponse {
    response(
        status,
        vec![Ok(Frame::data(Bytes::from_static(bytes)))],
        false,
        bytes.len() as u64,
    )
}
#[derive(Clone, Debug)]
struct ScriptConnector {
    responses: Arc<Mutex<VecDeque<HttpResponse>>>,
    calls: Arc<AtomicUsize>,
    pending_headers: bool,
    expected_range: Option<&'static str>,
}
impl ScriptConnector {
    fn new(responses: Vec<HttpResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into())),
            calls: Arc::new(AtomicUsize::new(0)),
            pending_headers: false,
            expected_range: Some("bytes=10-17"),
        }
    }
}
impl HttpConnector for ScriptConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        // Do not emit URI/headers; only verify a bounded Range was generated.
        assert_eq!(request.headers().get("range"), self.expected_range);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self.responses.lock().unwrap().pop_front();
        let pending = self.pending_headers;
        HttpConnectorFuture::new(async move {
            if pending {
                futures_util::future::pending().await
            } else {
                Ok(response.expect("unexpected SDK dispatch"))
            }
        })
    }
}
impl HttpClient for ScriptConnector {
    fn http_connector(
        &self,
        _: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}
fn context(origin: super::super::Origin) -> ReadContext {
    ReadContext {
        engine: super::super::Engine::Packed,
        phase: super::super::Phase::Runtime,
        class: super::super::ReadClass::PackedPayload,
        origin,
    }
}
fn client(connector: ScriptConnector) -> Client {
    Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "fixture-access",
                "fixture-secret",
                None,
                None,
                "fixture",
            ))
            .force_path_style(true)
            .endpoint_url("https://fixture.invalid")
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(2)
                    .with_initial_backoff(std::time::Duration::from_millis(1)),
            )
            .http_client(connector)
            .build(),
    )
}
async fn get(
    client: &Client,
    observer: Arc<ReadObserver>,
    context: ReadContext,
) -> Result<
    aws_sdk_s3::operation::get_object::GetObjectOutput,
    Box<aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>>,
> {
    let http =
        ObservedHttpClient::new(client.config().http_client().unwrap(), observer, context, 8);
    client
        .get_object()
        .bucket("fixture")
        .key("data-name-does-not-classify")
        .range("bytes=10-17")
        .customize()
        .config_override(aws_sdk_s3::config::Builder::new().http_client(http))
        .send()
        .await
        .map_err(Box::new)
}
fn row(observer: &ReadObserver, context: ReadContext) -> super::super::Counters {
    let snapshot = observer.snapshot();
    assert!(!snapshot.overflowed);
    assert!(
        snapshot
            .rows
            .values()
            .all(super::super::Counters::conserved)
    );
    snapshot.rows[&(Ledger::HttpAttempt, context)].clone()
}

#[tokio::test]
async fn aws_sdk_retry_error_xml_and_success_body_have_distinct_attempts() {
    const XML: &[u8] = b"<Error><Code>SlowDown</Code><Message>fixture</Message></Error>";
    let connector = ScriptConnector::new(vec![complete(503, XML), complete(206, b"12345678")]);
    let calls = connector.calls.clone();
    let sdk = client(connector);
    let observer = Arc::new(ReadObserver::default());
    let tag = context(super::super::Origin::Demand);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        get(&sdk, observer.clone(), tag),
    )
    .await
    .unwrap()
    .unwrap();
    let headers = row(&observer, tag);
    assert_eq!(
        (headers.started, headers.failed, headers.inflight),
        (2, 1, 1)
    );
    assert_eq!(headers.received_failed, XML.len() as u64);
    assert_eq!(
        output.body.collect().await.unwrap().into_bytes().as_ref(),
        b"12345678"
    );
    let done = row(&observer, tag);
    assert_eq!(
        (done.started, done.failed, done.success, done.cancelled),
        (2, 1, 1, 0)
    );
    assert_eq!(done.received, XML.len() as u64 + 8);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn aws_sdk_partial_reset_and_dropped_body_retain_actual_bytes() {
    let error: aws_smithy_runtime_api::box_error::BoxError =
        std::io::Error::from(std::io::ErrorKind::ConnectionReset).into();
    let connector = ScriptConnector::new(vec![response(
        206,
        vec![Ok(Frame::data(Bytes::from_static(b"abc"))), Err(error)],
        false,
        8,
    )]);
    let sdk = client(connector);
    let observer = Arc::new(ReadObserver::default());
    let tag = context(super::super::Origin::Demand);
    let output = get(&sdk, observer.clone(), tag).await.unwrap();
    assert!(output.body.collect().await.is_err());
    let failed = row(&observer, tag);
    assert_eq!(
        (failed.started, failed.failed, failed.received_failed),
        (1, 1, 3)
    );

    let sdk = client(ScriptConnector::new(vec![response(
        206,
        vec![Ok(Frame::data(Bytes::from_static(b"xy")))],
        true,
        8,
    )]));
    let tag = context(super::super::Origin::Prefetch);
    let mut output = get(&sdk, observer.clone(), tag).await.unwrap();
    assert_eq!(output.body.next().await.unwrap().unwrap().as_ref(), b"xy");
    drop(output);
    let cancelled = row(&observer, tag);
    assert_eq!((cancelled.cancelled, cancelled.received_cancelled), (1, 2));
}
#[tokio::test]
async fn unpolled_sdk_future_never_dispatches_and_headers_cancellation_terminates_once() {
    let mut connector = ScriptConnector::new(vec![]);
    connector.pending_headers = true;
    let calls = connector.calls.clone();
    let sdk = client(connector);
    let observer = Arc::new(ReadObserver::default());
    let tag = context(super::super::Origin::Warmup);
    let future = get(&sdk, observer.clone(), tag);
    drop(future);
    assert!(observer.snapshot().rows.is_empty());
    let mut future = Box::pin(get(&sdk, observer.clone(), tag));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    drop(future);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(row(&observer, tag).cancelled, 1);
}
#[tokio::test]
async fn parallel_origins_and_mounts_are_operation_local_and_trailers_survive() {
    let observer_a = Arc::new(ReadObserver::default());
    let observer_b = Arc::new(ReadObserver::default());
    let mut trailers = ::http::HeaderMap::new();
    trailers.insert(
        "x-fixture-trailer",
        ::http::HeaderValue::from_static("retained"),
    );
    let sdk = client(ScriptConnector::new(vec![
        response(
            206,
            vec![
                Ok(Frame::data(Bytes::from_static(b"12345678"))),
                Ok(Frame::trailers(trailers)),
            ],
            false,
            8,
        ),
        complete(206, b"abcdefgh"),
        complete(206, b"ABCDEFGH"),
    ]));
    let demand = context(super::super::Origin::Demand);
    let prefetch = context(super::super::Origin::Prefetch);
    let (a, b, c) = tokio::join!(
        get(&sdk, observer_a.clone(), demand),
        get(&sdk, observer_a.clone(), prefetch),
        get(&sdk, observer_b.clone(), demand)
    );
    let a = a.unwrap().body.collect().await.unwrap().into_bytes();
    let b = b.unwrap().body.collect().await.unwrap().into_bytes();
    let c = c.unwrap().body.collect().await.unwrap().into_bytes();
    assert_eq!(a.len() + b.len() + c.len(), 24);
    assert_eq!(row(&observer_a, demand).received_success, 8);
    assert_eq!(row(&observer_a, prefetch).received_success, 8);
    assert_eq!(row(&observer_b, demand).received_success, 8);

    let observer = Arc::new(ReadObserver::default());
    let mut trailer = ::http::HeaderMap::new();
    trailer.insert(
        "x-fixture-trailer",
        ::http::HeaderValue::from_static("retained"),
    );
    let mut body = ObservedHttpBody::new(
        SdkBody::from_body_1_x(ScriptBody {
            frames: vec![
                Ok(Frame::data(Bytes::from_static(b"12345678"))),
                Ok(Frame::trailers(trailer)),
            ]
            .into(),
            pending: false,
            hint: 8,
        }),
        observer.start(Ledger::HttpAttempt, demand, 8),
        true,
    );
    assert!(body.frame().await.unwrap().unwrap().is_data());
    let frame = body.frame().await.unwrap().unwrap();
    assert_eq!(
        frame
            .trailers_ref()
            .unwrap()
            .get("x-fixture-trailer")
            .unwrap(),
        "retained"
    );
    assert_eq!(row(&observer, demand).received_success, 8);
}

#[tokio::test]
async fn native_full_sdk_get_has_no_range_and_unknown_length_attempt_is_explicit() {
    let mut connector = ScriptConnector::new(vec![complete(200, b"abcdefgh")]);
    connector.expected_range = None;
    let calls = connector.calls.clone();
    let sdk = client(connector);
    let observer = Arc::new(ReadObserver::default());
    let tag = ReadContext {
        engine: super::super::Engine::Native,
        class: super::super::ReadClass::NativePayload,
        ..context(super::super::Origin::Demand)
    };
    let observed = ObservedHttpClient::with_request_length(
        sdk.config().http_client().unwrap(),
        observer.clone(),
        tag,
        None,
    );
    let output = sdk
        .get_object()
        .bucket("fixture")
        .key("arbitrary")
        .customize()
        .config_override(aws_sdk_s3::config::Builder::new().http_client(observed))
        .send()
        .await
        .unwrap();
    assert_eq!(
        output.body.collect().await.unwrap().into_bytes().as_ref(),
        b"abcdefgh"
    );
    let row = row(&observer, tag);
    assert_eq!(
        (
            row.started,
            row.success,
            row.requested,
            row.requested_unknown,
            row.received
        ),
        (1, 1, 0, 1, 8)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropped_error_body_is_terminal_http_failure_with_received_bytes() {
    let observer = Arc::new(ReadObserver::default());
    let tag = context(super::super::Origin::Demand);
    let mut body = ObservedHttpBody::new(
        SdkBody::from_body_1_x(ScriptBody {
            frames: vec![Ok(Frame::data(Bytes::from_static(b"retry-body")))].into(),
            pending: true,
            hint: 32,
        }),
        observer.start(Ledger::HttpAttempt, tag, 32),
        false,
    );

    assert_eq!(
        body.frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .as_ref(),
        b"retry-body".as_slice()
    );
    drop(body);

    let row = row(&observer, tag);
    assert_eq!((row.failed, row.cancelled, row.received_failed), (1, 0, 10));
    assert_eq!(row.failure_reasons[&FailureClass::HttpStatus], 1);
    assert!(row.conserved());
}
