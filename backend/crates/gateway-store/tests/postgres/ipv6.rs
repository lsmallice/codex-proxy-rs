//! 独占 IPv6 的导入排队、幂等、归属保护与退役测试

use super::{TestDatabase, provider_accounts::account};
use gateway_admin::{
    model::{MutationActor, MutationContext, ipv6::*},
    ports::{
        ipv6::Ipv6ProxyStore,
        store::{AccountStore, AdminStoreErrorKind},
    },
};
use gateway_core::account::OutboundProxy;
use gateway_store::postgres::{
    PgIpv6Repository, PgProviderAccountRepository, ProviderAccountRepository,
};

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::AdminApiKey,
        request_id: "ipv6-test".to_owned(),
    }
}
fn remote(id: u32) -> Ipv6RemoteLease {
    Ipv6RemoteLease {
        id: id.to_string(),
        address: format!("2001:db8::{id}").parse().unwrap(),
        proxy: OutboundProxy::parse(&format!("http://test_{id}:test-only@127.0.0.1:11880"))
            .unwrap(),
    }
}

async fn pending_account(store: &PgIpv6Repository, account_id: &str) -> Option<String> {
    for id in store.pending("pool-test").await.unwrap() {
        if store.get(&id).await.unwrap().target_account_id.as_deref() == Some(account_id) {
            return Some(id);
        }
    }
    None
}

#[tokio::test]
async fn imports_are_held_until_binding_and_reimport_does_not_allocate_again() {
    let Some(db) = TestDatabase::create("ipv6_import").await else {
        return;
    };
    let store = PgIpv6Repository::new(db.pool.clone());
    let accounts = PgProviderAccountRepository::new(db.pool.clone());
    let policy = store.policy().await.unwrap();
    assert!(!policy.enabled);
    store
        .set_policy(Some("pool-test"), policy.revision, &context())
        .await
        .unwrap();
    accounts
        .insert_provider_account(account("auto-one", "auto-one"))
        .await
        .unwrap();
    let enabled: bool =
        sqlx::query_scalar("select enabled from provider_accounts where id='auto-one'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(!enabled);
    let id = pending_account(&store, "auto-one").await.unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().view.state,
        Ipv6LeaseState::Queued
    );
    sqlx::query("update provider_accounts set enabled=true where id='auto-one'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>("select enabled from provider_accounts where id='auto-one'")
            .fetch_one(&db.pool)
            .await
            .unwrap()
    );
    store.start_allocation(&id).await.unwrap();
    store.save_remote(&id, remote(1)).await.unwrap();
    store.complete(&id, &context()).await.unwrap();
    let lease = store.get(&id).await.unwrap();
    assert_eq!(lease.view.state, Ipv6LeaseState::Active);
    assert_eq!(lease.view.owner_account_id.as_deref(), Some("auto-one"));
    assert!(
        sqlx::query_scalar::<_, bool>("select enabled from provider_accounts where id='auto-one'")
            .fetch_one(&db.pool)
            .await
            .unwrap()
    );
    assert!(pending_account(&store, "auto-one").await.is_none());
    assert!(store.begin_retire(&id, true, &context()).await.is_err());
    // 仍绑定的账号不会因退役请求而改走直连
    assert_eq!(
        store.get(&id).await.unwrap().view.state,
        Ipv6LeaseState::Active
    );
    let proxy = lease.view.proxy_id.unwrap();
    assert!(
        sqlx::query("update outbound_proxies set enabled=false where id=$1")
            .bind(&proxy)
            .execute(&db.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("delete from outbound_proxies where id=$1")
            .bind(&proxy)
            .execute(&db.pool)
            .await
            .is_err()
    );
    sqlx::query("update provider_accounts set enabled=false where id='auto-one'")
        .execute(&db.pool)
        .await
        .unwrap();
    accounts.delete_provider_account("auto-one").await.unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().view.state,
        Ipv6LeaseState::Retiring
    );
    store.finish_retire(&id, &context()).await.unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().view.state,
        Ipv6LeaseState::Retired
    );
    assert_eq!(store.list().await.unwrap().len(), 1);
    db.close().await;
}

#[tokio::test]
async fn manual_proxy_is_exclusive_and_other_accounts_cannot_reuse_it() {
    let Some(db) = TestDatabase::create("ipv6_exclusive").await else {
        return;
    };
    let store = PgIpv6Repository::new(db.pool.clone());
    let accounts = PgProviderAccountRepository::new(db.pool.clone());
    let input = Ipv6Allocation {
        request_id: "request-one".into(),
        controller_id: "pool-test".into(),
        name: "Dedicated".into(),
        account_id: None,
    };
    assert!(store.begin(input.clone(), &context()).await.unwrap().1);
    assert!(!store.begin(input, &context()).await.unwrap().1);
    store.start_allocation("request-one").await.unwrap();
    store.save_remote("request-one", remote(2)).await.unwrap();
    store.complete("request-one", &context()).await.unwrap();
    let proxy = store
        .get("request-one")
        .await
        .unwrap()
        .view
        .proxy_id
        .unwrap();
    for id in ["acct_owner", "acct_other"] {
        accounts
            .insert_provider_account(account(id, id))
            .await
            .unwrap();
    }
    sqlx::query(
        "update provider_accounts set outbound_proxy_id=$1,outbound_proxy_url=$2 where id='acct_owner'",
    )
    .bind(&proxy)
    .bind(remote(2).proxy.expose_url())
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(sqlx::query("update provider_accounts set outbound_proxy_id=$1,outbound_proxy_url=$2 where id='acct_other'")
        .bind(&proxy).bind(remote(2).proxy.expose_url()).execute(&db.pool).await.is_err());
    let error = super::admin_account_store(&db.pool)
        .batch_update_accounts(
            gateway_admin::model::accounts::BatchUpdateAccounts {
                quota_limits: None,
                auto_use_quota_credits: None,
                account_ids: vec!["acct_other".to_owned()],
                enabled: None,
                concurrency_limit: None,
                weight: None,
                model_access: None,
                group_ids: None,
                outbound_proxy: Some(gateway_admin::model::proxies::AccountProxySelection::Saved(
                    proxy.clone(),
                )),
            },
            Default::default(),
            &context(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), AdminStoreErrorKind::Conflict);
    sqlx::query("update provider_accounts set outbound_proxy_id=null,outbound_proxy_url=null where id='acct_owner'").execute(&db.pool).await.unwrap();
    assert_eq!(
        store.get("request-one").await.unwrap().view.state,
        Ipv6LeaseState::Retiring
    );
    assert!(sqlx::query("update provider_accounts set outbound_proxy_id=$1,outbound_proxy_url=$2 where id='acct_other'")
        .bind(&proxy).bind(remote(2).proxy.expose_url()).execute(&db.pool).await.is_err());
    store
        .finish_retire("request-one", &context())
        .await
        .unwrap();
    assert_eq!(store.list().await.unwrap().len(), 1);
    db.close().await;
}

#[tokio::test]
async fn explicit_proxy_bypasses_auto_allocation_and_unknown_creation_is_not_replayed_early() {
    let Some(db) = TestDatabase::create("ipv6_explicit").await else {
        return;
    };
    let store = PgIpv6Repository::new(db.pool.clone());
    store
        .set_policy(Some("pool-test"), 1, &context())
        .await
        .unwrap();
    let accounts = PgProviderAccountRepository::new(db.pool.clone());
    let mut input = account("explicit", "explicit");
    input.outbound_proxy = Some(OutboundProxy::parse("http://127.0.0.1:18888").unwrap());
    accounts.insert_provider_account(input).await.unwrap();
    assert!(pending_account(&store, "explicit").await.is_none());
    accounts
        .insert_provider_account(account("unknown", "unknown"))
        .await
        .unwrap();
    let id = pending_account(&store, "unknown").await.unwrap();
    store.start_allocation(&id).await.unwrap();
    store.mark_unknown(&id).await.unwrap();
    assert!(store.start_allocation(&id).await.is_err());
    assert!(store.pending("pool-test").await.unwrap().is_empty());
    db.close().await;
}
