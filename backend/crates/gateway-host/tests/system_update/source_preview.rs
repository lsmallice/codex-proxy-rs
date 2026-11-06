//! 验证固定发行来源、通道筛选、明确未知项与只读预检边界
use super::*;
use gateway_admin::model::system::{
    SystemReleaseSource as Source, SystemSourceCheckStatus as Status,
    SystemUpdateChannel as Channel,
};

fn release(version: &str, url: &str) -> serde_json::Value {
    serde_json::json!({"tag_name":format!("v{version}"),"prerelease":version.contains('-'),"body":"release notes","assets":[
        {"name":archive_name(version),"browser_download_url":format!("{url}/archive"),"size":128},
        {"name":"checksums.txt","browser_download_url":format!("{url}/checksums"),"size":100}
    ]})
}

#[tokio::test]
async fn sources_are_isolated_and_preview_never_changes_installation_or_authorizes_install() {
    let server = MockServer::start().await;
    let fixture = Fixture::new();
    for (source, repository, version) in [
        (Source::Official, "zyycn/codex-proxy-rs", "1.0.0"),
        (Source::Fork, "lsmallice/codex-proxy-rs", "1.2.0"),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/repos/{repository}/releases")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(vec![release(version, &server.uri())]),
            )
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
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path().ends_with("/releases"))
    );
}

#[tokio::test]
async fn development_builds_can_browse_but_cannot_bypass_version_checks() {
    let server = MockServer::start().await;
    let fixture = Fixture::new();
    let mut draft = release("1.9.0", &server.uri());
    draft["draft"] = serde_json::json!(true);
    Mock::given(method("GET"))
        .and(path("/repos/zyycn/codex-proxy-rs/releases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![
            release("1.0.0", &server.uri()),
            release("1.2.0-rc.1", &server.uri()),
            release("1.3.0-beta.1", &server.uri()),
            release("2.0.0", &server.uri()),
            draft,
        ]))
        .mount(&server)
        .await;
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
        assert_eq!(
            result
                .checks
                .iter()
                .find(|c| c.id == "build")
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
            Status::Blocked
        );
    }
    assert!(
        service
            .source_preview(Source::Official, Channel::Experimental)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_assets_and_empty_releases_are_not_reported_ready_and_errors_are_not_empty_success()
{
    for (status, body) in [
        (200, serde_json::json!([])),
        (
            200,
            serde_json::json!([{"tag_name":"v1.1.0","prerelease":false,"assets":[]}]),
        ),
        (403, serde_json::json!({"message":"limited"})),
    ] {
        let server = MockServer::start().await;
        let fixture = Fixture::new();
        Mock::given(method("GET"))
            .and(path("/repos/lsmallice/codex-proxy-rs/releases"))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&server)
            .await;
        let result = fixture
            .service(&server)
            .source_preview(Source::Fork, Channel::Stable)
            .await;
        if status == 403 {
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
