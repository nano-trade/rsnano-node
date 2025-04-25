use super::WorkRequest;
use rsnano_core::{to_hex_string, Root, WorkNonce};
#[cfg(test)]
use rsnano_nullable_http_client::{ConfiguredHttpResponse, JsonResponse, Method, StatusCode};
use rsnano_nullable_http_client::{HttpClient, IntoUrl, NulledHttpClientBuilder, Url};
use rsnano_output_tracker::OutputListenerMt;
#[cfg(test)]
use rsnano_output_tracker::OutputTrackerMt;
#[cfg(test)]
use std::sync::Arc;

#[derive(serde::Serialize)]
struct HttpWorkRequest {
    action: &'static str,
    hash: String,
    difficulty: String,
}

impl HttpWorkRequest {
    pub fn new(root: Root, difficulty: u64) -> Self {
        Self {
            action: "work_generate",
            hash: root.to_string(),
            difficulty: to_hex_string(difficulty),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct HttpWorkResponse {
    work: WorkNonce,
}

#[derive(Default)]
pub(crate) struct DistributedWorkClient {
    http_client: HttpClient,
    request_listener: OutputListenerMt<(Url, WorkRequest)>,
}

impl DistributedWorkClient {
    #[cfg(test)]
    fn new(http_client: HttpClient) -> Self {
        Self {
            http_client,
            request_listener: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn new_null() -> Self {
        Self::new_null_with(42.into())
    }

    #[cfg(test)]
    pub fn new_null_with(response: WorkNonce) -> Self {
        Self::new(HttpClient::null_builder().respond(JsonResponse::new(
            StatusCode::OK,
            HttpWorkResponse { work: response },
        )))
    }

    #[cfg(test)]
    pub fn new_failing_null(error_message: impl Into<String>) -> Self {
        Self::new(HttpClient::null_builder().fail_with(error_message))
    }

    #[cfg(test)]
    pub fn new_halting_null() -> Self {
        Self::new(HttpClient::null_builder().halt())
    }

    #[cfg(test)]
    pub fn null_builder() -> NullDistributedWorkClientBuilder {
        NullDistributedWorkClientBuilder {
            http_client: HttpClient::null_builder(),
        }
    }

    pub async fn generate_work(
        &self,
        url: impl IntoUrl,
        request: WorkRequest,
    ) -> anyhow::Result<WorkNonce> {
        let url = url.into_url()?;
        self.request_listener.emit((url.clone(), request.clone()));

        let http_work_request = HttpWorkRequest::new(request.root, request.difficulty);

        let response: HttpWorkResponse = self
            .http_client
            .post_json(url, &http_work_request)
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(response.work)
    }

    #[cfg(test)]
    pub fn track_requests(&self) -> Arc<OutputTrackerMt<(Url, WorkRequest)>> {
        self.request_listener.track()
    }
}

#[allow(dead_code)]
pub(crate) struct NullDistributedWorkClientBuilder {
    http_client: NulledHttpClientBuilder,
}

#[cfg(test)]
impl NullDistributedWorkClientBuilder {
    pub fn response(mut self, url: impl IntoUrl, resp: ConfiguredWorkResponse) -> Self {
        self.http_client = self.http_client.respond_url(Method::POST, url, resp.into());
        self
    }

    pub fn finish(self) -> DistributedWorkClient {
        DistributedWorkClient::new(self.http_client.finish())
    }
}

#[cfg(test)]
pub(crate) enum ConfiguredWorkResponse {
    Ok(WorkNonce),
    Error(String),
    Halt,
}

#[cfg(test)]
impl From<ConfiguredWorkResponse> for ConfiguredHttpResponse {
    fn from(value: ConfiguredWorkResponse) -> Self {
        match value {
            ConfiguredWorkResponse::Ok(work) => ConfiguredHttpResponse::Json(JsonResponse::new(
                StatusCode::OK,
                HttpWorkResponse { work },
            )),
            ConfiguredWorkResponse::Error(error) => ConfiguredHttpResponse::Error(error),
            ConfiguredWorkResponse::Halt => ConfiguredHttpResponse::Halt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_nullable_http_client::{JsonResponse, Method};
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn post_work_request() {
        let http_client = HttpClient::null_builder().respond(JsonResponse::new(
            StatusCode::OK,
            HttpWorkResponse {
                work: WorkNonce::new(42),
            },
        ));
        let tracker = http_client.track_requests();
        let url: Url = "http://test-host:123".parse().unwrap();
        let work_client = DistributedWorkClient::new(http_client);

        let request = WorkRequest::new_test_instance();
        let work = work_client
            .generate_work(url.clone(), request)
            .await
            .unwrap();

        let output = tracker.output();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].method, Method::POST);
        assert_eq!(output[0].url, url);
        assert_eq!(work, WorkNonce::new(42));
    }

    #[tokio::test]
    async fn check_response_status() {
        let http_client = HttpClient::null_builder().respond(JsonResponse::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "error",
        ));
        let work_client = DistributedWorkClient::new(http_client);

        let url: Url = "http://test-host:123".parse().unwrap();
        let request = WorkRequest::new_test_instance();

        let err = work_client
            .generate_work(url, request)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("500 Internal Server Error"),
            "error was: {}",
            err
        );
    }

    #[tokio::test]
    async fn can_be_nulled() {
        let client = DistributedWorkClient::new_null();
        let result = client
            .generate_work("http://nulled-host", WorkRequest::new_test_instance())
            .await
            .unwrap();
        assert_eq!(result, WorkNonce::new(42));
    }

    #[tokio::test]
    async fn failing_null() {
        let client = DistributedWorkClient::new_failing_null("an error");

        let err = client
            .generate_work("http://nulled-host", WorkRequest::new_test_instance())
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "an error");
    }

    #[tokio::test]
    async fn configure_multiple_responses() {
        let client = DistributedWorkClient::null_builder()
            .response(
                "http://host1",
                ConfiguredWorkResponse::Ok(WorkNonce::new(1)),
            )
            .response(
                "http://host2",
                ConfiguredWorkResponse::Ok(WorkNonce::new(2)),
            )
            .finish();

        let request = WorkRequest::new_test_instance();

        let work1 = client
            .generate_work("http://host1", request.clone())
            .await
            .unwrap();

        let work2 = client.generate_work("http://host2", request).await.unwrap();

        assert_eq!(work1, 1.into());
        assert_eq!(work2, 2.into());
    }

    #[tokio::test]
    async fn configure_error_per_url() {
        let client = DistributedWorkClient::null_builder()
            .response(
                "http://host1",
                ConfiguredWorkResponse::Error("test error".to_string()),
            )
            .finish();

        let error = client
            .generate_work("http://host1", WorkRequest::new_test_instance())
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "test error");
    }

    #[tokio::test]
    async fn configure_halt_per_url() {
        let client = DistributedWorkClient::null_builder()
            .response("http://host1", ConfiguredWorkResponse::Halt)
            .finish();

        let result = timeout(
            Duration::ZERO,
            client.generate_work("http://host1", WorkRequest::new_test_instance()),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn can_track_requests() {
        let client = DistributedWorkClient::new_null();
        let tracker = client.track_requests();
        let request = WorkRequest::new_test_instance();
        let url = Url::parse("http://127.0.0.1:1234").unwrap();

        client
            .generate_work(url.clone(), request.clone())
            .await
            .unwrap();

        let output = tracker.output();
        assert_eq!(output.len(), 1, "nothing tracked");
        assert_eq!(output[0], (url, request));
    }
}
