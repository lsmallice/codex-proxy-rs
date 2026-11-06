//! 验证新 SDK 从宿主启动环境取得容量且不突破有限硬上限
use super::*;

#[test]
fn host_capacity_environment_is_bounded_and_process_local() {
    const CHILD: &str = "CPR_CAPACITY_CONFIG_TEST";
    if std::env::var_os(CHILD).is_some() {
        let config = SessionConfig::default();
        let expected: usize = std::env::var(CHILD).unwrap().parse().unwrap();
        assert_eq!(config.maximum_calls, expected);
        assert_eq!(
            config.maximum_callbacks,
            if expected == 256 { 320 } else { 2048 }
        );
        return;
    }
    for (calls, callbacks, expected) in [
        ("512", "2048", 512),
        ("1024", "2048", 1024),
        ("1025", "4097", 256),
        ("0", "0", 256),
        ("bad", "bad", 256),
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", std::thread::current().name().unwrap()])
            .env(CHILD, expected.to_string())
            .env("CPR_PLUGIN_MAXIMUM_CALLS", calls)
            .env("CPR_PLUGIN_MAXIMUM_CALLBACKS", callbacks)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
