//! IPv6 分配队列、独占归属与账号绑定的事务存储

use super::{append_admin_audit_event_in_transaction, bump_config_revision_in_transaction};
use crate::{admin_revision, mutation_audit};
use async_trait::async_trait;
use gateway_admin::{
    model::{MutationContext, Revision, audit::MutationAuditOperation, ipv6::*},
    ports::{
        ipv6::Ipv6ProxyStore,
        proxy::ProxyImportGuard,
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
    },
};
use gateway_core::account::OutboundProxy;
use sqlx::{PgConnection, PgPool, Postgres, Row as _, Transaction, postgres::PgRow};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct PgIpv6Repository {
    pool: PgPool,
    slots: Arc<Semaphore>,
}

impl PgIpv6Repository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            slots: Arc::new(Semaphore::new(2)),
        }
    }
}

struct Guard {
    _connection: PgConnection,
    _slot: OwnedSemaphorePermit,
}
impl ProxyImportGuard for Guard {}

fn conflict() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Conflict,
        "IPv6",
        "lease state conflict",
    )
}
fn unavailable(error: impl std::error::Error + Send + Sync + 'static) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "IPv6",
        "storage unavailable",
    )
    .with_source(error)
}
fn sql_error(error: sqlx::Error) -> AdminStoreError {
    if error
        .as_database_error()
        .is_some_and(|e| e.is_unique_violation() || e.is_foreign_key_violation())
    {
        conflict()
    } else {
        unavailable(error)
    }
}
fn row(row: PgRow) -> AdminStoreResult<Ipv6Lease> {
    let state: String = row.try_get("state").map_err(unavailable)?;
    let state = match state.as_str() {
        "queued" => Ipv6LeaseState::Queued,
        "allocating" => Ipv6LeaseState::Allocating,
        "unknown" => Ipv6LeaseState::Unknown,
        "pending" => Ipv6LeaseState::Pending,
        "active" => Ipv6LeaseState::Active,
        "retiring" => Ipv6LeaseState::Retiring,
        "retired" => Ipv6LeaseState::Retired,
        _ => return Err(conflict()),
    };
    let address: Option<String> = row.try_get("address").map_err(unavailable)?;
    let lease_id: Option<String> = row.try_get("lease_id").map_err(unavailable)?;
    let proxy_url: Option<String> = row.try_get("proxy_url").map_err(unavailable)?;
    let remote = match (lease_id, address.as_ref(), proxy_url) {
        (Some(id), Some(address), Some(url)) => Some(Ipv6RemoteLease {
            id,
            address: address.parse().map_err(unavailable)?,
            proxy: OutboundProxy::parse(&url).map_err(unavailable)?,
        }),
        _ if state == Ipv6LeaseState::Retired || address.is_none() => None,
        _ => return Err(conflict()),
    };
    Ok(Ipv6Lease {
        view: Ipv6LeaseView {
            request_id: row.try_get("request_id").map_err(unavailable)?,
            controller_id: row.try_get("controller_id").map_err(unavailable)?,
            name: row.try_get("name").map_err(unavailable)?,
            state,
            address,
            proxy_id: row.try_get("proxy_id").map_err(unavailable)?,
            owner_account_id: row.try_get("owner_account_id").map_err(unavailable)?,
        },
        target_account_id: row.try_get("target_account_id").map_err(unavailable)?,
        remote,
    })
}

async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    context: &MutationContext,
    operation: MutationAuditOperation,
    id: &str,
    fields: &[&str],
) -> AdminStoreResult<Revision> {
    let revision = bump_config_revision_in_transaction(tx)
        .await
        .map_err(unavailable)?;
    append_admin_audit_event_in_transaction(
        tx,
        mutation_audit(
            context,
            operation,
            id,
            fields.iter().map(|f| (*f).to_owned()).collect(),
        ),
        revision,
    )
    .await
    .map_err(unavailable)?;
    admin_revision(revision)
}

#[async_trait]
impl Ipv6ProxyStore for PgIpv6Repository {
    async fn lock(&self, request_id: &str) -> AdminStoreResult<Box<dyn ProxyImportGuard>> {
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| conflict())?;
        let mut connection = self.pool.acquire().await.map_err(unavailable)?.detach();
        let acquired: bool =
            sqlx::query_scalar("select pg_try_advisory_lock(hashtextextended($1,932817))")
                .bind(request_id)
                .fetch_one(&mut connection)
                .await
                .map_err(unavailable)?;
        if !acquired {
            return Err(conflict());
        }
        Ok(Box::new(Guard {
            _connection: connection,
            _slot: slot,
        }))
    }
    async fn policy(&self) -> AdminStoreResult<Ipv6ImportPolicy> {
        let (controller_id, revision): (Option<String>, i64) = sqlx::query_as(
            "select controller_id,revision from managed_ipv6_settings where singleton",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(Ipv6ImportPolicy {
            enabled: controller_id.is_some(),
            controller_id,
            revision: u64::try_from(revision).map_err(unavailable)?,
        })
    }
    async fn set_policy(
        &self,
        controller_id: Option<&str>,
        revision: u64,
        context: &MutationContext,
    ) -> AdminStoreResult<Ipv6ImportPolicy> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        audit(
            &mut tx,
            context,
            MutationAuditOperation::Ipv6PolicyUpdate,
            "ipv6",
            &["controller_id"],
        )
        .await?;
        let next: Option<i64> = sqlx::query_scalar("update managed_ipv6_settings set controller_id=$1,revision=revision+1 where singleton and revision=$2 returning revision")
            .bind(controller_id).bind(i64::try_from(revision).map_err(unavailable)?)
            .fetch_optional(&mut *tx).await.map_err(unavailable)?;
        let next = next.ok_or_else(conflict)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ipv6ImportPolicy {
            enabled: controller_id.is_some(),
            controller_id: controller_id.map(str::to_owned),
            revision: u64::try_from(next).map_err(unavailable)?,
        })
    }
    async fn begin(
        &self,
        input: Ipv6Allocation,
        context: &MutationContext,
    ) -> AdminStoreResult<(Ipv6Lease, bool)> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        if let Some(existing) = sqlx::query("select * from managed_ipv6_leases where request_id=$1")
            .bind(&input.request_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?
        {
            let existing = row(existing)?;
            if existing.view.controller_id != input.controller_id
                || existing.view.name != input.name
                || existing.target_account_id != input.account_id
            {
                return Err(conflict());
            }
            return Ok((existing, false));
        }
        audit(
            &mut tx,
            context,
            MutationAuditOperation::Ipv6Allocate,
            &input.request_id,
            &["name", "target_account_id"],
        )
        .await?;
        if let Some(account) = &input.account_id {
            let eligible: Option<bool> = sqlx::query_scalar("select outbound_proxy_id is null and outbound_proxy_url is null from provider_accounts where id=$1 for update")
                .bind(account).fetch_optional(&mut *tx).await.map_err(unavailable)?;
            if eligible != Some(true) {
                return Err(conflict());
            }
        }
        let inserted = sqlx::query("insert into managed_ipv6_leases(request_id,controller_id,name,target_account_id,state) values($1,$2,$3,$4,'queued') returning *")
            .bind(&input.request_id).bind(&input.controller_id).bind(&input.name).bind(&input.account_id)
            .fetch_one(&mut *tx).await.map_err(sql_error)?;
        tx.commit().await.map_err(unavailable)?;
        Ok((row(inserted)?, true))
    }
    async fn get(&self, request_id: &str) -> AdminStoreResult<Ipv6Lease> {
        let record = sqlx::query("select * from managed_ipv6_leases where request_id=$1")
            .bind(request_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| {
                AdminStoreError::new(AdminStoreErrorKind::NotFound, "IPv6", "lease not found")
            })?;
        row(record)
    }
    async fn list(&self) -> AdminStoreResult<Vec<Ipv6LeaseView>> {
        let rows = sqlx::query("select * from managed_ipv6_leases order by (state='retired'),created_at desc limit 200")
            .fetch_all(&self.pool).await.map_err(unavailable)?;
        rows.into_iter().map(|r| row(r).map(|l| l.view)).collect()
    }
    async fn pending(&self, controller_id: &str) -> AdminStoreResult<Vec<String>> {
        sqlx::query_scalar("select request_id from managed_ipv6_leases where controller_id=$1 and state in ('queued','allocating','unknown','pending','retiring') and next_attempt_at <= now() order by next_attempt_at,created_at limit 20")
            .bind(controller_id).fetch_all(&self.pool).await.map_err(unavailable)
    }
    async fn start_allocation(&self, request_id: &str) -> AdminStoreResult<()> {
        let changed = sqlx::query("update managed_ipv6_leases set state='allocating',updated_at=now(),next_attempt_at=now()+interval '120 seconds' where request_id=$1 and (state='queued' or (state='unknown' and lease_id is null and next_attempt_at <= now()))")
            .bind(request_id).execute(&self.pool).await.map_err(unavailable)?;
        if changed.rows_affected() != 1 {
            return Err(conflict());
        }
        Ok(())
    }
    async fn postpone(&self, request_id: &str) -> AdminStoreResult<()> {
        sqlx::query("update managed_ipv6_leases set next_attempt_at=greatest(next_attempt_at,now()+interval '30 seconds') where request_id=$1")
            .bind(request_id).execute(&self.pool).await.map_err(unavailable)?;
        Ok(())
    }
    async fn save_remote(&self, request_id: &str, lease: Ipv6RemoteLease) -> AdminStoreResult<()> {
        let changed = sqlx::query("update managed_ipv6_leases set lease_id=$2,address=$3,proxy_url=$4,state=case when state='retiring' then state else 'pending' end,updated_at=now(),next_attempt_at=now() where request_id=$1 and state in ('allocating','retiring') and lease_id is null")
            .bind(request_id).bind(&lease.id).bind(lease.address.to_string()).bind(lease.proxy.expose_url())
            .execute(&self.pool).await.map_err(sql_error)?;
        if changed.rows_affected() != 1 {
            return Err(conflict());
        }
        Ok(())
    }
    async fn mark_unknown(&self, request_id: &str) -> AdminStoreResult<()> {
        sqlx::query("update managed_ipv6_leases set state='unknown',updated_at=now(),next_attempt_at=now()+interval '16 minutes' where request_id=$1 and state='allocating' and lease_id is null")
            .bind(request_id).execute(&self.pool).await.map_err(unavailable)?;
        Ok(())
    }
    async fn complete(
        &self,
        request_id: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        let current = Ipv6ProxyStore::get(self, request_id).await?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let revision = audit(
            &mut tx,
            context,
            MutationAuditOperation::Ipv6Bind,
            request_id,
            &["proxy_id", "owner_account_id"],
        )
        .await?;
        // 账号行先于租约行锁定，与账号归属触发器保持同一顺序
        if let Some(account) = &current.target_account_id {
            let eligible: Option<bool> = sqlx::query_scalar("select outbound_proxy_id is null and outbound_proxy_url is null from provider_accounts where id=$1 for update")
                .bind(account).fetch_optional(&mut *tx).await.map_err(unavailable)?;
            if eligible != Some(true) {
                return Err(conflict());
            }
        }
        let record =
            sqlx::query("select * from managed_ipv6_leases where request_id=$1 for update")
                .bind(request_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        let automatic: bool = record.try_get("automatic").map_err(unavailable)?;
        let resume: bool = record.try_get("resume_enabled").map_err(unavailable)?;
        let lease = row(record)?;
        if lease.view.state != Ipv6LeaseState::Pending {
            return Err(conflict());
        }
        let remote = lease.remote.ok_or_else(conflict)?;
        let proxy_id = format!("proxy_{}", uuid::Uuid::now_v7().simple());
        sqlx::query("insert into outbound_proxies(id,name,proxy_url) values($1,$2,$3)")
            .bind(&proxy_id)
            .bind(&lease.view.name)
            .bind(remote.proxy.expose_url())
            .execute(&mut *tx)
            .await
            .map_err(sql_error)?;
        sqlx::query("update managed_ipv6_leases set proxy_id=$2,owner_account_id=target_account_id,state='active',updated_at=now() where request_id=$1")
            .bind(request_id).bind(&proxy_id).execute(&mut *tx).await.map_err(unavailable)?;
        if let Some(account) = &lease.target_account_id {
            sqlx::query("update provider_accounts set outbound_proxy_id=$1,outbound_proxy_url=$2,enabled=case when $4 then $5 else enabled end,provider_quota_json=null,quota_observed_at=null,updated_at=greatest(now(),updated_at) where id=$3")
                .bind(&proxy_id).bind(remote.proxy.expose_url()).bind(account).bind(automatic).bind(resume)
                .execute(&mut *tx).await.map_err(sql_error)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(revision)
    }
    async fn begin_retire(
        &self,
        request_id: &str,
        cancel: bool,
        context: &MutationContext,
    ) -> AdminStoreResult<()> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        audit(
            &mut tx,
            context,
            MutationAuditOperation::Ipv6Retire,
            request_id,
            &["state"],
        )
        .await?;
        let lease = row(sqlx::query(
            "select * from managed_ipv6_leases where request_id=$1 for update",
        )
        .bind(request_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?)?;
        if let Some(proxy) = &lease.view.proxy_id {
            let bound: bool = sqlx::query_scalar(
                "select exists(select 1 from provider_accounts where outbound_proxy_id=$1)",
            )
            .bind(proxy)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            if bound {
                return Err(conflict());
            }
        }
        sqlx::query("update managed_ipv6_leases set state='retiring',cancelled=cancelled or $2,updated_at=now(),next_attempt_at=now() where request_id=$1 and state<>'retired'")
            .bind(request_id).bind(cancel).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
    async fn finish_retire(
        &self,
        request_id: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let revision = audit(
            &mut tx,
            context,
            MutationAuditOperation::Ipv6Retire,
            request_id,
            &["state", "proxy_id"],
        )
        .await?;
        let record =
            sqlx::query("select * from managed_ipv6_leases where request_id=$1 for update")
                .bind(request_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        let automatic: bool = record.try_get("automatic").map_err(unavailable)?;
        let cancelled: bool = record.try_get("cancelled").map_err(unavailable)?;
        let resume: bool = record.try_get("resume_enabled").map_err(unavailable)?;
        let lease = row(record)?;
        if lease.view.state != Ipv6LeaseState::Retiring {
            return Err(conflict());
        }
        let proxy_id = lease.view.proxy_id;
        let never_bound = proxy_id.is_none();
        sqlx::query("update managed_ipv6_leases set state='retired',proxy_id=null,proxy_url=null,updated_at=now() where request_id=$1")
            .bind(request_id).execute(&mut *tx).await.map_err(unavailable)?;
        if let Some(proxy) = proxy_id {
            sqlx::query("delete from outbound_proxies where id=$1")
                .bind(proxy)
                .execute(&mut *tx)
                .await
                .map_err(sql_error)?;
        }
        if automatic
            && !cancelled
            && never_bound
            && let Some(account) = lease.target_account_id
        {
            sqlx::query("insert into managed_ipv6_leases(request_id,controller_id,name,target_account_id,state,resume_enabled,automatic,next_attempt_at)
                    select 'ipv6_' || gen_random_uuid()::text,$1,$2,$3,'queued',$4,true,now()+interval '30 seconds'
                    where exists(select 1 from provider_accounts where id=$3 and outbound_proxy_id is null and outbound_proxy_url is null)
                    and not exists(select 1 from managed_ipv6_leases where target_account_id=$3 and state not in ('active','retired'))")
                    .bind(&lease.view.controller_id).bind(&lease.view.name).bind(account).bind(resume)
                    .execute(&mut *tx).await.map_err(sql_error)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(revision)
    }
}
