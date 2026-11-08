//! 验证来源预检使用固定索引和精确 tag，不改变安装状态
use super::*;
use gateway_admin::model::system::{
    SystemReleaseSource as Source, SystemSourceCheckStatus as Status,
    SystemUpdateChannel as Channel,
};

fn release(version: &str, url: &str) -> serde_json::Value {
    serde_json::json!({"tag_name":format!("v{version}"),"draft":false,"prerelease":version.contains('-'),"body":"release notes","assets":[
        {"name":archive_name(version),"browser_download_url":format!("{url}/archive"),"size":128},
        {"name":"checksums.txt","browser_download_url":format!("{url}/checksums"),"size":100}
    ]})
}

async fn index(server: &MockServer, repository: &str, stable: &str, rc: &str, beta: &str) {
    Mock::given(method("GET")).and(path(format!("/{repository}/refs/heads/main/release/version.yaml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!("version: 1.9.0\npublished:\n  '1':\n    stable: {stable}\n    rc: {rc}\n    beta: {beta}\n")))
        .mount(server).await;
}

#[tokio::test]
async fn sources_are_isolated_and_preview_never_changes_installation_or_authorizes_install() {
    let server = MockServer::start().await;
    let fixture = Fixture::new();
    for (source, repository, version) in [
        (Source::Official, "zyycn/codex-proxy-rs", "1.0.0"),
        (Source::Fork, "lsmallice/codex-proxy-rs", "1.2.0"),
    ] {
        index(&server, repository, version, version, version).await;
        Mock::given(method("GET"))
            .and(path(format!(
                "/repos/{repository}/releases/tags/v{version}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(release(version, &server.uri())))
            .expect(1)
            .mount(&server)
            .await;
        let config = fixture.config(&format!("{}/repos", server.uri()));
        let service = ProcessSystemOperations::new(CancellationToken::new(), config);
        let result = service
            .source_preview(source, Channel::Stable)
            .await
            .unwrap();
        assert_eq!(result.repository, repository);
        assert_eq!(result.latest_version.as_deref(), Some(version));
        assert!(!result.installation_allowed);
        assert_eq!(
            result
                .checks
                .iter()
                .find(|c| c.id == "source")
                .unwrap()
                .status,
            Status::Blocked
        );
        assert_eq!(
            result
                .checks
                .iter()
                .find(|c| c.id == "version")
                .unwrap()
                .status,
            if version == "1.0.0" {
                Status::Blocked
            } else {
                Status::Passed
            }
        );
        for id in ["database", "plugins", "deployment", "switch"] {
            assert_eq!(
                result.checks.iter().find(|c| c.id == id).unwrap().status,
                Status::Pending
            );
        }
        assert!(!fixture.state().exists());
        assert!(!fixture.lock().exists());
        assert!(!fixture.backup().exists());
        assert_eq!(
            fs::read_to_string(fixture.executable()).unwrap(),
            "old-binary"
        );
    }
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(
        |r| r.url.path().contains("/releases/tags/") || r.url.path().ends_with("/version.yaml")
    ));
}

#[tokio::test]
async fn development_builds_can_browse_but_cannot_bypass_version_checks() {
    let server = MockServer::start().await;
    let fixture = Fixture::new();
    let repository = "zyycn/codex-proxy-rs";
    index(&server, repository, "1.0.0", "1.2.0-rc.1", "1.3.0-beta.1").await;
    for version in ["1.0.0", "1.2.0-rc.1", "1.3.0-beta.1"] {
        Mock::given(method("GET"))
            .and(path(format!(
                "/repos/{repository}/releases/tags/v{version}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(release(version, &server.uri())))
            .mount(&server)
            .await;
    }
    let mut config = fixture.config(&format!("{}/repos", server.uri()));
    config.build_type = "dev".into();
    config.version = "1.1.0-local".into();
    let service = ProcessSystemOperations::new(CancellationToken::new(), config);
    for (channel, version) in [
        (Channel::Stable, "1.0.0"),
        (Channel::Rc, "1.2.0-rc.1"),
        (Channel::Beta, "1.3.0-beta.1"),
    ] {
        let result = service
            .source_preview(Source::Official, channel)
            .await
            .unwrap();
        assert_eq!(result.latest_version.as_deref(), Some(version));
        for id in ["build", "version"] {
            assert_eq!(
                result.checks.iter().find(|c| c.id == id).unwrap().status,
                Status::Blocked
            );
        }
    }
    assert!(
        service
            .source_preview(Source::Official, Channel::Experimental)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_assets_or_releases_are_blocked_and_invalid_identity_or_index_is_an_error() {
    for scenario in ["missing", "assets", "limited", "identity"] {
        let server = MockServer::start().await;
        let fixture = Fixture::new();
        let repository = "lsmallice/codex-proxy-rs";
        if scenario == "limited" {
            Mock::given(method("GET"))
                .and(path(format!(
                    "/{repository}/refs/heads/main/release/version.yaml"
                )))
                .respond_with(ResponseTemplate::new(403))
                .mount(&server)
                .await;
        } else {
            index(&server, repository, "1.2.0", "1.2.0", "1.2.0").await;
            let mut data = release("1.2.0", &server.uri());
            if scenario == "assets" {
                data["assets"] = serde_json::json!([]);
            }
            if scenario == "identity" {
                data["draft"] = serde_json::json!(true);
            }
            Mock::given(method("GET"))
                .and(path(format!("/repos/{repository}/releases/tags/v1.2.0")))
                .respond_with(if scenario == "missing" {
                    ResponseTemplate::new(404)
                } else {
                    ResponseTemplate::new(200).set_body_json(data)
                })
                .mount(&server)
                .await;
        }
        let result = fixture
            .service(&server)
            .source_preview(Source::Fork, Channel::Stable)
            .await;
        if matches!(scenario, "limited" | "identity") {
            assert!(result.is_err());
        } else {
            let result = result.unwrap();
            assert!(!result.installation_allowed);
            assert_eq!(
                result
                    .checks
                    .iter()
                    .find(|c| c.id == "assets")
                    .unwrap()
                    .status,
                Status::Blocked
            );
        }
    }
}
