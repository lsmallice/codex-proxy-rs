//! 账号并发交接的管理员 HTTP 合同

use super::*;
use crate::auth::SessionState;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CancelRequest {
    id: String,
    revision: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PolicyRequest {
    enabled: bool,
    revision: u64,
    config: gateway_core::account::AccountHandoffConfig,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordsView {
    #[default]
    Active,
    History,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecordsQuery {
    #[serde(default)]
    view: RecordsView,
    page: Option<u32>,
    page_size: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RecordView {
    #[serde(flatten)]
    record: gateway_core::account::AccountHandoffSnapshot,
    created_at_display: Option<String>,
    finished_at_display: Option<String>,
}

impl RecordView {
    fn new(
        record: gateway_core::account::AccountHandoffSnapshot,
        time: crate::time::TimePresenter,
    ) -> Self {
        let display = |ms: u64| {
            i64::try_from(ms)
                .ok()
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .map(|at| time.datetime(&at))
        };
        let created_at_display = display(record.state.created_at_ms);
        let finished_at_display = record.state.finished_at_ms.and_then(display);
        Self {
            record,
            created_at_display,
            finished_at_display,
        }
    }
}

pub(super) fn router<S>() -> Router<S>
where
    S: SessionState + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/admin/accounts/handoffs", get(list::<S>))
        .route("/api/admin/accounts/handoffs/records", get(records::<S>))
        .route(
            "/api/admin/accounts/handoffs/policy",
            get(policy::<S>).post(save_policy::<S>),
        )
        .route("/api/admin/accounts/handoffs/start", post(start::<S>))
        .route("/api/admin/accounts/handoffs/cancel", post(cancel::<S>))
}

async fn records<S>(
    _: AdminAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<RecordsQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let time = crate::time::TimePresenter::new(state.admin_services().timezone());
    let history = matches!(query.view, RecordsView::History);
    let page = state
        .admin_services()
        .accounts()
        .handoff_records(
            history,
            query.page.unwrap_or(1),
            query.page_size.unwrap_or(20),
        )
        .await
        .map_err(map_service_error)?;
    let items: Vec<_> = page
        .items
        .into_iter()
        .map(|item| RecordView::new(item, time))
        .collect();
    let total_pages = page.total.div_ceil(u64::from(page.page_size));
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(serde_json::json!({
            "items": items,
            "page": {"page":page.page,"pageSize":page.page_size,"total":page.total,"totalPages":total_pages},
            "activeTotal": page.active_total,
            "occupiedAccountIds": page.occupied_account_ids,
            "historyRetentionDays": gateway_core::account::HANDOFF_HISTORY_RETENTION_SECONDS / 86400,
            "historyMaximumRecords": gateway_core::account::MAX_HANDOFF_HISTORY_RECORDS,
        })),
    ))
}

async fn policy<S>(_: AdminAuth, State(state): State<S>) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let result = state
        .admin_services()
        .accounts()
        .handoff_policy()
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn save_policy<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<PolicyRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let result = state
        .admin_services()
        .accounts()
        .set_handoff_policy(
            request.enabled,
            request.config,
            request.revision,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn list<S>(_: AdminAuth, State(state): State<S>) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let items = state
        .admin_services()
        .accounts()
        .handoffs()
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(items)))
}

async fn start<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<gateway_core::account::StartAccountHandoff>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let result = state
        .admin_services()
        .accounts()
        .start_handoff(request, &auth.context().mutation_context())
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn cancel<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<CancelRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    state
        .admin_services()
        .accounts()
        .cancel_handoff(
            request.id,
            request.revision,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(serde_json::json!({})),
    ))
}
