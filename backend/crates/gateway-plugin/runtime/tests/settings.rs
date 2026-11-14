//! 验证插件启动容量与 HTTP 精确路径筛选配置
use gateway_plugin_runtime::PluginStartupConfig;
use serde_json::json;

#[test]
fn capacity_defaults_and_bounded_overrides() {
    let defaults: PluginStartupConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(defaults.rpc_limits().maximum_calls, 256);
    for (calls, executing, queued, callbacks, valid) in [
        (512, 64, 64, 1024, true),
        (1024, 128, 128, 2048, true),
        (0, 1, 0, 1, false),
        (1025, 64, 64, 1024, false),
        (64, 65, 0, 1, false),
        (64, 1, 65, 1, false),
        (256, 64, 64, 4097, false),
    ] {
        let c:PluginStartupConfig=serde_json::from_value(json!({"rpc":{"maximum_calls":calls,"maximum_executing_calls":executing,"maximum_queued_calls":queued,"maximum_callbacks":callbacks}})).unwrap();
        assert_eq!(c.validate().is_ok(), valid);
    }
}

#[test]
fn maintenance_overrides_accept_five_seconds_and_preserve_other_instances() {
    let defaults: PluginStartupConfig = serde_json::from_value(json!({})).unwrap();
    assert!(defaults.maintenance_intervals_seconds.is_empty());
    let id = "a0f1ee31-0005-8e2a-bfd9-30e7de51c321";
    for (seconds, valid) in [
        (0, false),
        (4, false),
        (5, true),
        (30, true),
        (3600, true),
        (3601, false),
    ] {
        let settings: PluginStartupConfig = serde_json::from_value(json!({
            "maintenance_intervals_seconds": {id: seconds}
        }))
        .unwrap();
        assert_eq!(settings.validate().is_ok(), valid);
        assert_eq!(
            settings.rpc_limits().maximum_queue_timeout,
            defaults.rpc_limits().maximum_queue_timeout
        );
    }
    let invalid: PluginStartupConfig =
        serde_json::from_value(json!({"maintenance_intervals_seconds":{"not-an-id":5}})).unwrap();
    assert!(invalid.validate().is_err());
}

#[test]
fn http_filters_require_explicit_methods_and_paths() {
    for (method, path, valid) in [
        ("POST", "/v1/responses", true),
        ("GET", "/healthz", true),
        ("post", "/v1/responses", false),
        ("POST", "/v1/*", false),
        ("POST", "/v1/responses?x=1", false),
        ("POST", "https://example.org", false),
    ] {
        let c:PluginStartupConfig=serde_json::from_value(json!({"http_filters":{"a0f1ee31-0005-8e2a-bfd9-30e7de51c321":[{"method":method,"path":path}]}})).unwrap();
        assert_eq!(c.validate().is_ok(), valid);
    }
}
