//! 管理员二开配置保存与运行策略发布接口，不触发服务重启
use super::{AdminAuth, AdminEnvelope, AdminError, AdminJson, wire::map_admin_service_error};
use crate::auth::SessionState;
use axum::{Router, extract::State, routing::get};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Save {
    revision: String,
    values: Value,
}
pub(super) fn router<S: SessionState + Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new().route("/api/admin/settings/fork", get(load::<S>).post(save::<S>))
}
async fn load<S: SessionState>(
    _auth: AdminAuth,
    State(state): State<S>,
) -> Result<
    axum::Json<AdminEnvelope<gateway_admin::ports::fork_settings::ForkSettingsSnapshot>>,
    AdminError,
> {
    let data = state
        .admin_services()
        .fork_settings()
        .map_err(map_admin_service_error)?
        .load()
        .await
        .map_err(map_admin_service_error)?;
    Ok(axum::Json(AdminEnvelope::ok(data)))
}
async fn save<S: SessionState>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(input): AdminJson<Save>,
) -> Result<
    axum::Json<AdminEnvelope<gateway_admin::ports::fork_settings::ForkSettingsSnapshot>>,
    AdminError,
> {
    let data = state
        .admin_services()
        .fork_settings()
        .map_err(map_admin_service_error)?
        .save(
            input.revision,
            input.values,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_admin_service_error)?;
    Ok(axum::Json(AdminEnvelope::ok(data)))
}
