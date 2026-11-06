//! 组装非敏感二开配置投影，各字段仍由所属 Provider 和 Runtime 校验
use super::GatewayConfig;
use gateway_admin::model::AdminError;
use gateway_host::{
    ConfigError,
    fork_settings::{ForkSettingsStore, PreparedActivation, read_saved},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Values {
    openai: provider_openai::OpenAiForkSettings,
    plugins: gateway_plugin_runtime::PluginStartupConfig,
}
fn validate(mut value: serde_json::Value) -> Result<serde_json::Value, AdminError> {
    // 已保存配置只补新增安全默认值，其他缺失或未知字段仍严格拒绝
    if let Some(openai) = value
        .get_mut("openai")
        .and_then(serde_json::Value::as_object_mut)
    {
        openai.entry("request_compatibility").or_insert_with(|| {
            serde_json::json!(provider_openai::config::CodexRequestCompatibility::default())
        });
    }
    if let Some(recovery) = value
        .pointer_mut("/openai/ws_response_recovery")
        .and_then(serde_json::Value::as_object_mut)
    {
        recovery
            .entry("encrypted_history")
            .or_insert(serde_json::Value::Bool(false));
        recovery
            .entry("encrypted_history_max_sessions")
            .or_insert(serde_json::json!(65536));
        recovery
            .entry("encrypted_history_max_digests_per_session")
            .or_insert(serde_json::json!(512));
    }
    let parsed: Values = serde_json::from_value(value.clone())
        .map_err(|_| AdminError::invalid("二开配置字段格式无效"))?;
    parsed
        .openai
        .clone()
        .apply(&mut provider_openai::OpenAiConfig::default())
        .map_err(|error| AdminError::invalid("OpenAI 二开配置超出允许范围").with_source(error))?;
    parsed
        .plugins
        .validate()
        .map_err(|field| AdminError::invalid(format!("配置字段 {field} 不合法")))?;
    let normalized =
        serde_json::to_value(parsed).map_err(|_| AdminError::internal("二开配置序列化失败"))?;
    if normalized != value {
        return Err(AdminError::invalid(
            "二开配置包含未知或缺失字段，请刷新后重试",
        ));
    }
    Ok(normalized)
}
pub(super) fn initialize(config: &mut GatewayConfig) -> Result<(), ConfigError> {
    let path = config.host.runtime_data_dir().join("fork-settings.json");
    let baseline = serde_json::to_value(Values {
        openai: (&config.openai).into(),
        plugins: config.plugins.clone(),
    })
    .map_err(|_| ConfigError::InvalidField("fork_settings"))?;
    let active = read_saved(&path)
        .map_err(|_| ConfigError::InvalidField("fork-settings.json"))?
        .unwrap_or_else(|| baseline.clone());
    let active = validate(active).map_err(|_| ConfigError::InvalidField("fork-settings.json"))?;
    let values: Values = serde_json::from_value(active.clone())
        .map_err(|_| ConfigError::InvalidField("fork-settings.json"))?;
    values
        .openai
        .apply(&mut config.openai)
        .map_err(|_| ConfigError::InvalidField("fork-settings.json"))?;
    config.plugins = values.plugins;
    let runtime = Arc::new(provider_openai::OpenAiForkRuntime::new(
        (&config.openai).into(),
    ));
    config.openai.fork_runtime = Some(runtime.clone());
    let startup = active.clone();
    let prepare = Arc::new(move |saved: &serde_json::Value| {
        let mut active = saved.clone();
        // 插件进程与缓存容量仍沿用启动资源，只发布请求级策略
        active["plugins"] = startup["plugins"].clone();
        for field in [
            "encrypted_history_max_sessions",
            "encrypted_history_max_digests_per_session",
        ] {
            active["openai"]["ws_response_recovery"][field] =
                startup["openai"]["ws_response_recovery"][field].clone();
        }
        let policy: provider_openai::OpenAiForkSettings =
            serde_json::from_value(active["openai"].clone())
                .map_err(|_| AdminError::invalid("OpenAI 运行配置无效"))?;
        let policy = Arc::new(policy);
        let runtime = runtime.clone();
        Ok(PreparedActivation {
            active,
            publish: Box::new(move || runtime.publish(policy)),
        })
    });
    config.fork_settings = Some(Arc::new(
        ForkSettingsStore::new(path, baseline, active, Arc::new(validate)).with_activation(prepare),
    ));
    Ok(())
}
