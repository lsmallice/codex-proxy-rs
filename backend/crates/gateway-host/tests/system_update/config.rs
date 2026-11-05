//! 更新仓库默认值、环境覆盖与显式配置的发行查询测试

use super::*;

#[tokio::test]
async fn configured_repository_controls_release_queries() {
    const CHILD_ENV: &str = "CPR_UPDATE_REPOSITORY_TEST_CASE";
    if let Ok(case) = std::env::var(CHILD_ENV) {
        let (configuration, expected) = match case.as_str() {
            "environment" => (serde_json::json!({}), "owner/custom"),
            "explicit" => (
                serde_json::json!({"update_repository": "owner/explicit"}),
                "owner/explicit",
            ),
            _ => (serde_json::json!({}), "lsmallice/codex-proxy-rs"),
        };
        let configured: SystemUpdateConfig =
            serde_json::from_value(configuration).expect("update configuration");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/repos/{expected}/releases")))
            .respond_with(release_response(TARGET_VERSION, Vec::new()))
            .expect(1)
            .mount(&server)
            .await;
        let fixture = Fixture::new();
        let mut config = fixture.config(&format!("{}/repos", server.uri()));
        config.update_repository = configured.update_repository;
        let service = ProcessSystemOperations::new(CancellationToken::new(), config);
        let detail = service.update_detail(true, None).await.expect("detail");
        assert!(detail.update_supported);
        assert!(detail.has_update);
        assert_eq!(detail.latest_version, TARGET_VERSION);
        server.verify().await;
        return;
    }

    // 子进程隔离环境变量，不影响并行运行的其他配置测试
    for (case, repository) in [
        ("default", None),
        ("empty", Some(" ")),
        ("environment", Some(" owner/custom ")),
        ("explicit", Some("owner/custom")),
    ] {
        let mut command =
            std::process::Command::new(std::env::current_exe().expect("test executable"));
        command
            .args([
                "--exact",
                "system_update::config::configured_repository_controls_release_queries",
                "--nocapture",
            ])
            .env(CHILD_ENV, case)
            .env_remove("CPR_UPDATE_REPOSITORY");
        if let Some(repository) = repository {
            command.env("CPR_UPDATE_REPOSITORY", repository);
        }
        let output = command.output().expect("configuration subprocess");
        assert!(
            output.status.success(),
            "{case}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
