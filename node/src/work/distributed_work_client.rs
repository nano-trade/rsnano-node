use super::WorkRequest;
use rsnano_core::{to_hex_string, Root, WorkNonce};
use rsnano_nullable_http_client::{HttpClient, Url};

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
    work: String,
}

pub(crate) struct DistributedWorkClient {
    http_client: HttpClient,
}

impl DistributedWorkClient {
    fn new(http_client: HttpClient) -> Self {
        Self { http_client }
    }

    async fn generate_work(&self, url: Url, request: WorkRequest) -> anyhow::Result<WorkNonce> {
        let http_work_request = HttpWorkRequest::new(request.root, request.difficulty);
        let response: HttpWorkResponse = self
            .http_client
            .post_json(url, &http_work_request)
            .await?
            .json()
            .await?;

        let work = response.work.parse()?;

        Ok(WorkNonce::new(work))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_nullable_http_client::{ConfiguredResponse, Method, StatusCode};

    #[tokio::test]
    async fn post_work_request() {
        let http_client = HttpClient::null_builder().respond(ConfiguredResponse::new(
            StatusCode::OK,
            HttpWorkResponse {
                work: "42".to_string(),
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
}
