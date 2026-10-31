use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn create_bucket_accepts_owned_conflicts_but_rejects_foreign_names() {
    for (region, status, code, success) in [
        ("us-east-1", 200, "", true),
        ("eu-west-1", 200, "", true),
        ("us-east-1", 409, "BucketAlreadyOwnedByYou", true),
        ("us-east-1", 409, "Conflict", true),
        ("us-east-1", 409, "BucketAlreadyExists", false),
        ("us-east-1", 403, "AccessDenied", false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                assert_eq!(socket.read(&mut byte).await.unwrap(), 1);
                request.push(byte[0]);
                assert!(request.len() < 32768);
            }
            assert!(request.starts_with(b"PUT /workspace-upper"));
            let headers = String::from_utf8(request).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let mut payload = vec![0; length];
            socket.read_exact(&mut payload).await.unwrap();
            if region == "us-east-1" {
                assert!(payload.is_empty());
            } else {
                assert!(
                    String::from_utf8(payload)
                        .unwrap()
                        .contains("<LocationConstraint>eu-west-1</LocationConstraint>")
                );
            }
            let body = if code.is_empty() {
                String::new()
            } else {
                format!("<Error><Code>{code}</Code><Message>bucket test</Message></Error>")
            };
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let backend = S3Backend::with_static_credentials(
            S3Config {
                bucket: "shared-lower".into(),
                region: Some(region.into()),
                endpoint: Some(format!("http://{address}")),
                force_path_style: true,
                ..Default::default()
            },
            "test".into(),
            "test".into(),
        )
        .await
        .unwrap();
        backend.set_bucket("workspace-upper").unwrap();
        let result = backend.create_bucket_idempotent().await;
        assert_eq!(
            result.is_ok(),
            success,
            "status={status} code={code}: {result:?}"
        );
        if code == "BucketAlreadyExists" {
            assert!(result.unwrap_err().to_string().contains("name collision"));
        }
        server.await.unwrap();
    }
}
