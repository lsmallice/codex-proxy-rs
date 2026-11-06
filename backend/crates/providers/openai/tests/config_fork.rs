//! 验证后台二开配置不包含或覆盖上游地址及凭据配置
use provider_openai::{OpenAiConfig, OpenAiForkSettings};

#[test]
fn fork_settings_reuse_owner_validation_and_preserve_other_fields() {
    let mut config = OpenAiConfig::default();
    config.api.base_url = "https://example.org/backend-api".into();
    config.stream_max_retries = 7;
    let mut settings = OpenAiForkSettings::from(&config);
    settings.ws_response_recovery.max_retries = 3;
    settings.apply(&mut config).unwrap();
    assert_eq!(config.ws_response_recovery.max_retries, 3);
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
