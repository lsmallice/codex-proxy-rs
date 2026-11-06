//! 验证二开配置管理接口的鉴权、版本冲突和保存响应
use super::{AdminTestFixture, AdminTestState};
use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gateway_admin::{
    model::{AdminError, MutationContext},
    ports::fork_settings::{ForkSettingsService, ForkSettingsSnapshot},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

struct Settings(Mutex<ForkSettingsSnapshot>);
#[async_trait]
impl ForkSettingsService for Settings {
    async fn load(&self) -> Result<ForkSettingsSnapshot, AdminError> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn save(
        &self,
        revision: String,
        values: Value,
        _context: &MutationContext,
    ) -> Result<ForkSettingsSnapshot, AdminError> {
        let mut state = self.0.lock().unwrap();
        if state.revision != revision {
            return Err(AdminError::conflict("配置已被修改"));
        }
        state.saved = values;
        state.revision = "next".into();
        state.restart_required = state.saved != state.active;
        Ok(state.clone())
    }
}
fn request(body: Option<Value>) -> Request<Body> {
    Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri("/api/admin/settings/fork")
        .header("cookie", "cpr_session=valid-session")
        .header("content-type", "application/json")
        .header("x-request-id", "req_fork_settings")
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .unwrap()
}
#[tokio::test]
async fn fork_settings_auth_save_conflict_and_strict_envelope() {
    let fixture = AdminTestFixture::new().await;
    let service = Arc::new(Settings(Mutex::new(ForkSettingsSnapshot {
        revision: "initial".into(),
        active: json!({"value":1}),
        saved: json!({"value":1}),
        restart_required: false,
    })));
    let app = gateway_api::admin::router::<AdminTestState>().with_state(AdminTestState(
        fixture.services.clone().with_fork_settings(service),
    ));
    for body in [
        None,
        Some(json!({"revision":"initial","values":{"value":2}})),
    ] {
        assert_eq!(
            app.clone().oneshot(request(body)).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    fixture.auth.insert_session("valid-session");
    let read = app.clone().oneshot(request(None)).await.unwrap();
    assert_eq!(read.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(read.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(body["data"]["revision"], "initial");
    let update = json!({"revision":"initial","values":{"value":2}});
    let response = app
        .clone()
        .oneshot(request(Some(update.clone())))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(body["data"]["active"], json!({"value":1}));
    assert_eq!(body["data"]["saved"], json!({"value":2}));
    assert_eq!(body["data"]["restartRequired"], true);
    assert_eq!(
        app.clone()
            .oneshot(request(Some(update)))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let invalid = json!({"revision":"next","values":{"value":3},"unknown":true});
    assert_eq!(
        app.oneshot(request(Some(invalid))).await.unwrap().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}
