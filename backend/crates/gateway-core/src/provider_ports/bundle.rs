//! 按能力提供 Provider 所需的存储端口

use super::{
    OAuthPendingFlowPort, ProviderArtifactProfileCachePort, ProviderCatalogCachePort,
    ProviderCooldownPort, ProviderCredentialStatePort, ProviderLeasePort,
    ProviderRuntimePolicyPort, ProviderSessionAffinityPort, ProviderSessionExclusionPort,
};
use crate::account::{AccountFeedbackStats, ProviderAccountStore};
use std::{fmt, sync::Arc};

/// Provider 只能按能力取用端口，无法取得 Redis client 或 repository 集合
#[derive(Clone)]
pub struct ProviderStorePorts {
    dispatch: Arc<dyn super::AccountDispatchStore>,
    traffic_runtime: Arc<crate::account::AccountTrafficRuntime>,
    handoffs: Option<Arc<dyn crate::provider_ports::AccountHandoffStore>>,
    accounts: Arc<dyn ProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
    session_affinity: Arc<dyn ProviderSessionAffinityPort>,
    session_exclusions: Arc<dyn ProviderSessionExclusionPort>,
    account_feedback: Arc<AccountFeedbackStats>,
    catalog_cache: Arc<dyn ProviderCatalogCachePort>,
    artifact_profiles: Arc<dyn ProviderArtifactProfileCachePort>,
    credential_state: Arc<dyn ProviderCredentialStatePort>,
    cooldowns: Arc<dyn ProviderCooldownPort>,
    runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
    oauth_pending: Arc<dyn OAuthPendingFlowPort>,
    diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
}

impl ProviderStorePorts {
    #[must_use]
    // 每个参数代表独立能力端口；合并为单一配置对象会隐藏 Provider 能力边界
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        accounts: Arc<dyn ProviderAccountStore>,
        leases: Arc<dyn ProviderLeasePort>,
        session_affinity: Arc<dyn ProviderSessionAffinityPort>,
        session_exclusions: Arc<dyn ProviderSessionExclusionPort>,
        catalog_cache: Arc<dyn ProviderCatalogCachePort>,
        artifact_profiles: Arc<dyn ProviderArtifactProfileCachePort>,
        credential_state: Arc<dyn ProviderCredentialStatePort>,
        cooldowns: Arc<dyn ProviderCooldownPort>,
        runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
        oauth_pending: Arc<dyn OAuthPendingFlowPort>,
        diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
    ) -> Self {
        Self {
            dispatch: Arc::new(super::UnavailableDispatchStore),
            traffic_runtime: Arc::default(),
            handoffs: None,
            accounts,
            leases,
            session_affinity,
            session_exclusions,
            account_feedback: Arc::new(AccountFeedbackStats::default()),
            catalog_cache,
            artifact_profiles,
            credential_state,
            cooldowns,
            runtime_policy,
            oauth_pending,
            diagnostics,
        }
    }

    #[must_use]
    pub fn diagnostics(&self) -> Arc<dyn crate::diagnostics::OperationalDiagnostics> {
        self.diagnostics.clone()
    }

    pub fn with_dispatch_store(mut self, dispatch: Arc<dyn super::AccountDispatchStore>) -> Self {
        self.dispatch = dispatch;
        self
    }
    pub fn with_traffic_runtime(
        mut self,
        runtime: Arc<crate::account::AccountTrafficRuntime>,
    ) -> Self {
        self.traffic_runtime = runtime;
        self
    }
    pub fn dispatch_limiter(&self) -> Arc<super::AccountDispatchLimiter> {
        Arc::new(super::AccountDispatchLimiter::new(
            self.dispatch.clone(),
            self.traffic_runtime.clone(),
        ))
    }

    #[must_use]
    pub fn with_handoffs(
        mut self,
        handoffs: Arc<dyn crate::provider_ports::AccountHandoffStore>,
    ) -> Self {
        self.handoffs = Some(handoffs);
        self
    }

    pub fn handoffs(&self) -> Option<Arc<dyn crate::provider_ports::AccountHandoffStore>> {
        self.handoffs.clone()
    }

    #[must_use]
    pub fn accounts(&self) -> Arc<dyn ProviderAccountStore> {
        Arc::clone(&self.accounts)
    }

    #[must_use]
    pub fn leases(&self) -> Arc<dyn ProviderLeasePort> {
        Arc::clone(&self.leases)
    }

    #[must_use]
    pub fn session_affinity(&self) -> Arc<dyn ProviderSessionAffinityPort> {
        Arc::clone(&self.session_affinity)
    }

    #[must_use]
    pub fn session_exclusions(&self) -> Arc<dyn ProviderSessionExclusionPort> {
        Arc::clone(&self.session_exclusions)
    }

    #[must_use]
    pub fn account_feedback(&self) -> Arc<AccountFeedbackStats> {
        Arc::clone(&self.account_feedback)
    }

    #[must_use]
    pub fn catalog_cache(&self) -> Arc<dyn ProviderCatalogCachePort> {
        Arc::clone(&self.catalog_cache)
    }

    #[must_use]
    pub fn artifact_profiles(&self) -> Arc<dyn ProviderArtifactProfileCachePort> {
        Arc::clone(&self.artifact_profiles)
    }

    #[must_use]
    pub fn credential_state(&self) -> Arc<dyn ProviderCredentialStatePort> {
        Arc::clone(&self.credential_state)
    }

    #[must_use]
    pub fn cooldowns(&self) -> Arc<dyn ProviderCooldownPort> {
        Arc::clone(&self.cooldowns)
    }

    #[must_use]
    pub fn runtime_policy(&self) -> Arc<dyn ProviderRuntimePolicyPort> {
        Arc::clone(&self.runtime_policy)
    }

    #[must_use]
    pub fn oauth_pending(&self) -> Arc<dyn OAuthPendingFlowPort> {
        Arc::clone(&self.oauth_pending)
    }
}

impl fmt::Debug for ProviderStorePorts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderStorePorts([CAPABILITIES])")
    }
}
