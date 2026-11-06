//! 验证 OpenAI 配置解析、路径派生与上游地址约束

use std::{path::Path, time::Duration};

use provider_openai::config::{MAX_STREAM_MAX_RETRIES, OpenAiConfig};

#[test]
fn openai_config_preserves_optional_routing_header_values() {
    for (document, expected) in [
        (r#"{}"#, (None, None)),
        (
            r#"{"residency":null,"account_routing_override":null}"#,
            (None, None),
        ),
        (
            r#"{"residency":"us","account_routing_override":"us"}"#,
            (Some("us"), Some("us")),
        ),
        (
            r#"{"residency":"custom-region","account_routing_override":"custom-route"}"#,
            (Some("custom-region"), Some("custom-route")),
        ),
        (
            r#"{"residency":"","account_routing_override":"NO_CONSTRAINT"}"#,
            (Some(""), Some("NO_CONSTRAINT")),
        ),
    ] {
        let config: OpenAiConfig = serde_json::from_str(document).unwrap();
        assert_eq!(
            (
                config.residency.as_deref(),
                config.account_routing_override.as_deref()
            ),
            expected,
            "{document}",
        );
    }
}

#[test]
fn openai_config_derives_identity_secret_from_runtime_data_dir() {
    let mut config = valid_config();
    config
        .resolve_and_validate(Path::new("/srv/gateway/runtime-data"))
        .expect("valid OpenAI config");

    assert!(format!("{config:?}").contains("/srv/gateway/runtime-data/identity_hmac_secret"));
}

#[test]
fn openai_config_restricts_upstream_base_url_to_https_or_loopback_http() {
    for base_url in [
        "http://internal.example.com/backend-api",
        "http://10.0.0.7/backend-api",
        "https://chatgpt.com/backend-api?debug=1",
        "https://user:pass@chatgpt.com/backend-api",
        "https://chatgpt.com/backend-api#fragment",
        "ftp://chatgpt.com/backend-api",
    ] {
        let mut config = valid_config();
        config.api.base_url = base_url.to_owned();
        assert!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_err(),
            "expected {base_url} to be rejected"
        );
    }

    for base_url in [
        "https://chatgpt.com/backend-api",
        "http://127.0.0.1:8080/backend-api",
        "http://localhost:8080/backend-api",
        "http://[::1]:8080/backend-api",
    ] {
        let mut config = valid_config();
        config.api.base_url = base_url.to_owned();
        assert!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_ok(),
            "expected {base_url} to be accepted"
        );
    }
}

#[test]
fn openai_stream_retry_budget_uses_the_official_hard_cap() {
    let mut config = OpenAiConfig::default();
    config.stream_max_retries = MAX_STREAM_MAX_RETRIES + 1;

    assert_eq!(
        config.stream_max_retries(),
        u32::try_from(MAX_STREAM_MAX_RETRIES).expect("retry cap fits u32")
    );
}

#[test]
fn websocket_pool_idle_timeout_accepts_existing_configs_and_explicit_overrides() {
    for (idle_timeout_ms, expected) in [(None, 300_000), (Some(600_000), 600_000)] {
        let mut document = serde_json::json!({
            "ws_pool": {
                "enabled": true,
                "max_age_ms": 3_300_000,
                "max_connecting": 8,
                "stream_idle_timeout_ms": 300_000
            }
        });
        if let Some(idle_timeout_ms) = idle_timeout_ms {
            document["ws_pool"]["idle_timeout_ms"] = serde_json::json!(idle_timeout_ms);
        }
        let mut config: OpenAiConfig = serde_json::from_value(document).unwrap();
        config
            .resolve_and_validate(Path::new("/srv/gateway"))
            .unwrap();
        assert_eq!(
            config.websocket_pool_config().idle_timeout,
            Duration::from_millis(expected)
        );
    }
}

#[test]
fn websocket_pool_idle_timeout_rejects_zero() {
    let mut config = valid_config();
    config.ws_pool.idle_timeout_ms = 0;
    assert!(
        config
            .resolve_and_validate(Path::new("/srv/gateway"))
            .is_err()
    );
}

fn valid_config() -> OpenAiConfig {
    OpenAiConfig::default()
}

#[test]
fn websocket_response_recovery_is_bounded_and_opt_in() {
    let config: OpenAiConfig = serde_json::from_str("{}").unwrap();
    assert!(!config.ws_response_recovery.enabled);
    assert!(!config.ws_response_recovery.full_history);
    assert_eq!(config.ws_response_recovery.max_retries, 1);
    for (retries, valid) in [
        (0, true),
        (1, true),
        (3, true),
        (5, true),
        (6, false),
        (u32::MAX, false),
    ] {
        let mut config: OpenAiConfig = serde_json::from_value(
            serde_json::json!({"ws_response_recovery":{"max_retries":retries}}),
        )
        .unwrap();
        assert_eq!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_ok(),
            valid
        );
    }
    for retries in [
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("2"),
    ] {
        assert!(
            serde_json::from_value::<OpenAiConfig>(
                serde_json::json!({"ws_response_recovery":{"max_retries":retries}})
            )
            .is_err()
        );
    }
    for (window, bytes, valid) in [
        (1, 1024, true),
        (5000, 65536, true),
        (10000, 262144, true),
        (0, 1024, false),
        (10001, 1024, false),
        (5000, 262145, false),
    ] {
        let mut config:OpenAiConfig=serde_json::from_value(serde_json::json!({"ws_response_recovery":{"enabled":true,"full_history":true,"window_ms":window,"max_prefix_bytes":bytes}})).unwrap();
        assert_eq!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_ok(),
            valid
        );
    }
    for (codes, valid) in [
        (vec![1000, 1001, 1011, 1012, 1013], true),
        (vec![], true),
        (vec![1008], false),
        (vec![1009], false),
        (vec![1006], false),
    ] {
        let mut config: OpenAiConfig = serde_json::from_value(
            serde_json::json!({"ws_response_recovery":{"close_codes":codes}}),
        )
        .unwrap();
        assert_eq!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_ok(),
            valid
        );
    }
}

#[test]
fn model_unsupported_cooldown_accepts_zero_and_bounded_seconds() {
    let config: OpenAiConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(config.model_unsupported_cooldown_seconds, 1800);
    assert_eq!(config, OpenAiConfig::default());
    for seconds in [0, 1, 60, 1800, 86400, 86401, u64::MAX] {
        let mut config: OpenAiConfig = serde_json::from_value(serde_json::json!({
            "model_unsupported_cooldown_seconds":seconds
        }))
        .unwrap();
        assert_eq!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_ok(),
            seconds <= 86400,
            "{seconds}",
        );
    }
    for invalid in [
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("0"),
    ] {
        assert!(
            serde_json::from_value::<OpenAiConfig>(serde_json::json!({
                "model_unsupported_cooldown_seconds":invalid
            }))
            .is_err()
        );
    }
}

#[test]
fn opening_recovery_is_opt_in_and_rejects_unbounded_settings() {
    let config: OpenAiConfig = serde_json::from_str("{}").unwrap();
    assert!(!config.ws_opening_recovery.enabled);
    assert_eq!(config.ws_opening_recovery.max_retries, 2);
    for (retries, delay, valid) in [
        (0, 50, true),
        (5, 5000, true),
        (6, 200, false),
        (2, 0, false),
        (2, 5001, false),
    ] {
        let mut config: OpenAiConfig = serde_json::from_value(serde_json::json!({
            "ws_opening_recovery":{"enabled":true,"max_retries":retries,"retry_delay_ms":delay,"http_fallback":false}
        })).unwrap();
        assert_eq!(
            config
                .resolve_and_validate(Path::new("/srv/gateway"))
                .is_ok(),
            valid
        );
    }
}
