use super::WorkRequest;
use rsnano_core::{to_hex_string, Root, WorkNonce};
use rsnano_nullable_http_client::{ConfiguredResponse, HttpClient, IntoUrl, StatusCode, Url};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
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
    fn new(http_client: HttpClient) -> Self {
        Self {
            http_client,
            request_listener: Default::default(),
        }
    }

    pub fn new_null() -> Self {
        Self::new_null_with(42.into())
    }

    pub fn new_null_with(response: WorkNonce) -> Self {
        Self::new(HttpClient::null_builder().respond(ConfiguredResponse::new(
            StatusCode::OK,
            HttpWorkResponse { work: response },
        )))
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

    pub fn track_requests(&self) -> Arc<OutputTrackerMt<(Url, WorkRequest)>> {
        self.request_listener.track()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_nullable_http_client::{ConfiguredResponse, Method};

    #[tokio::test]
    async fn post_work_request() {
        let http_client = HttpClient::null_builder().respond(ConfiguredResponse::new(
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
        let http_client = HttpClient::null_builder().respond(ConfiguredResponse::new(
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
