//! 交接记录分页的管理员身份与查询参数边界
use super::super::{AdminTestFixture, AdminTestState};
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use gateway_api::admin;
use tower::ServiceExt as _;

#[tokio::test]
async fn handoff_record_pages_require_admin_and_reject_invalid_queries() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("valid-session");
    let router = admin::accounts::router::<AdminTestState>().with_state(fixture.state());
    for (auth, query, expected) in [
        (false, "view=history", StatusCode::UNAUTHORIZED),
        (true, "page=0", StatusCode::BAD_REQUEST),
        (true, "pageSize=0", StatusCode::BAD_REQUEST),
        (true, "pageSize=101", StatusCode::BAD_REQUEST),
        (true, "view=all", StatusCode::BAD_REQUEST),
        (true, "page=-1", StatusCode::BAD_REQUEST),
        (true, "extra=1", StatusCode::BAD_REQUEST),
    ] {
        let mut request = Request::builder()
            .uri(format!("/api/admin/accounts/handoffs/records?{query}"))
            .header("x-request-id", "req_handoff_records");
        if auth {
            request = request.header(header::COOKIE, "cpr_session=valid-session");
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{query}");
    }
}
