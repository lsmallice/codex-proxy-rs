//! 验证后台二开配置不包含或覆盖上游地址及凭据配置
use provider_openai::{OpenAiConfig, OpenAiForkSettings};

#[test]
fn fork_settings_reuse_owner_validation_and_preserve_other_fields() {
    let mut config = OpenAiConfig::default();
    config.api.base_url = "https://example.org/backend-api".into();
    config.stream_max_retries = 7;
    let mut settings = OpenAiForkSettings::from(&config);
    settings.ws_response_recovery.max_retries = 3;
    settings.ws_preconnect.enabled = true;
    settings.apply(&mut config).unwrap();
    assert_eq!(config.ws_response_recovery.max_retries, 3);
    assert!(config.ws_preconnect.enabled);
    for (total, per_account, connecting, rate, ttl, retries, delay, backoff) in [
        (0, 1, 1, 1, 10, 0, 100, 5),
        (8193, 1, 1, 1, 10, 0, 100, 5),
        (8, 9, 1, 1, 10, 0, 100, 5),
        (8, 8, 9, 1, 10, 0, 100, 5),
        (8, 8, 1, 0, 10, 0, 100, 5),
        (8, 8, 1, 1, 601, 0, 100, 5),
        (8, 8, 1, 1, 10, 6, 100, 5),
        (8, 8, 1, 1, 10, 0, 99, 5),
        (8, 8, 1, 1, 10, 0, 100, 0),
    ] {
        let mut invalid = OpenAiForkSettings::from(&config);
        invalid.ws_preconnect = provider_openai::config::CodexWsPreconnect {
            enabled: true,
            max_connections: total,
            max_connections_per_account: per_account,
            max_connecting: connecting,
            starts_per_second: rate,
            idle_timeout_seconds: ttl,
            max_retries: retries,
            retry_delay_ms: delay,
            failure_backoff_seconds: backoff,
        };
        assert!(invalid.apply(&mut config).is_err());
        assert!(config.ws_preconnect.enabled);
        assert_eq!(config.ws_preconnect.max_connections, 512);
    }
    assert_eq!(config.api.base_url, "https://example.org/backend-api");
    assert_eq!(config.stream_max_retries, 7);
    assert_eq!(
        config.ws_response_recovery.encrypted_history_max_sessions,
        65536
    );
    assert_eq!(
        config
            .ws_response_recovery
            .encrypted_history_max_digests_per_session,
        512
    );
    for (sessions, digests) in [(0, 512), (65537, 512), (65536, 0), (65536, 513)] {
        let mut invalid = OpenAiForkSettings::from(&config);
        invalid.ws_response_recovery.encrypted_history_max_sessions = sessions;
        invalid
            .ws_response_recovery
            .encrypted_history_max_digests_per_session = digests;
        assert!(invalid.apply(&mut config).is_err());
    }
    let mut settings = OpenAiForkSettings::from(&config);
    settings.ws_response_recovery.max_retries = 6;
    assert!(settings.apply(&mut config).is_err());
    assert_eq!(config.ws_response_recovery.max_retries, 3);
    let mut value = serde_json::to_value(OpenAiForkSettings::from(&config)).unwrap();
    value["api"] = serde_json::json!({"base_url":"https://other.invalid"});
    assert!(serde_json::from_value::<OpenAiForkSettings>(value).is_err());
}
