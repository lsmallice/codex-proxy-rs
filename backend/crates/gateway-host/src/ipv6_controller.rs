//! 宿主 IPv6 控制协议，限制正文、禁止重定向并校验地址池与租约身份

use async_trait::async_trait;
use futures::StreamExt as _;
use gateway_admin::{
    model::{AdminError, ipv6::*},
    ports::ipv6::Ipv6Controller,
};
use gateway_core::account::OutboundProxy;
use secrecy::ExposeSecret as _;
use std::time::Duration;

pub struct HttpIpv6Controller {
    id: String,
    base_url: String,
    token: secrecy::SecretString,
    client: reqwest::Client,
    prefix: ipnet::Ipv6Net,
}

impl HttpIpv6Controller {
    pub fn new(config: &crate::Ipv6EgressConfig) -> Result<Self, AdminError> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(25))
            .build()
            .map_err(|_| AdminError::unavailable("IPv6 控制客户端初始化失败"))?;
        Ok(Self {
            id: format!(
                "{}@{}",
                config.id,
                config
                    .prefix
                    .parse::<ipnet::Ipv6Net>()
                    .map_err(|_| AdminError::invalid("IPv6 地址段格式不合法"))?
            ),
            base_url: config.control_url.trim_end_matches('/').to_owned(),
            token: config.token.clone().into(),
            client,
            prefix: config
                .prefix
                .parse()
                .map_err(|_| AdminError::invalid("IPv6 地址段格式不合法"))?,
        })
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> Result<T, AdminError> {
        let response = self
            .client
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(self.token.expose_secret())
            .send()
            .await
            .map_err(|_| {
                AdminError::upstream_result_unknown("IPv6 控制请求结果待确认，请刷新分配记录")
            })?;
        if !response.status().is_success() {
            return Err(match response.status().as_u16() {
                409 => AdminError::conflict("IPv6 租约不可用或已退役"),
                401 | 403 => AdminError::unavailable("IPv6 控制服务认证失败"),
                _ => AdminError::unavailable("IPv6 控制服务请求失败"),
            });
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                AdminError::upstream_result_unknown("IPv6 控制响应中断，请刷新分配记录")
            })?;
            if body.len().saturating_add(chunk.len()) > 16384 {
                return Err(AdminError::upstream_result_unknown("IPv6 控制响应超过限制"));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body)
            .map_err(|_| AdminError::upstream_result_unknown("IPv6 控制服务返回无效结果"))
    }

    fn check_identity(&self, value: &LeaseWire) -> Result<std::net::Ipv6Addr, AdminError> {
        let sequence = value.id.parse::<u128>().ok().filter(|n| *n > 0);
        let address: std::net::Ipv6Addr = value
            .address
            .parse()
            .map_err(|_| AdminError::upstream_result_unknown("IPv6 分配地址无效"))?;
        if sequence.is_none_or(|n| {
            n.to_string() != value.id
                || u128::from(self.prefix.network()).checked_add(n) != Some(u128::from(address))
        }) || !self.prefix.contains(&address)
        {
            return Err(AdminError::upstream_result_unknown(
                "IPv6 租约与地址池不匹配",
            ));
        }
        Ok(address)
    }
}

#[derive(serde::Deserialize)]
struct LeaseWire {
    id: String,
    #[serde(default)]
    address: String,
    state: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u16,
}

#[derive(serde::Deserialize)]
struct StatusWire {
    configured: bool,
    available: bool,
    prefix: String,
    issued: String,
}

#[async_trait]
impl Ipv6Controller for HttpIpv6Controller {
    fn id(&self) -> &str {
        &self.id
    }

    async fn status(&self) -> Result<Ipv6Status, AdminError> {
        let value: StatusWire = self.request(reqwest::Method::GET, "/v1/status").await?;
        if value.prefix.parse::<ipnet::Ipv6Net>().ok() != Some(self.prefix) {
            return Err(AdminError::conflict(
                "IPv6 分配服务的地址段与 CPR 配置不一致",
            ));
        }
        Ok(Ipv6Status {
            configured: value.configured,
            available: value.available,
            controller_id: Some(self.id.clone()),
            prefix: Some(self.prefix.to_string()),
            issued: Some(value.issued),
            message: None,
        })
    }

    async fn allocate(&self) -> Result<Ipv6RemoteLease, AdminError> {
        // 状态检查必须早于非幂等创建，配置不一致时不消耗地址
        if !self.status().await?.available {
            return Err(AdminError::unavailable("IPv6 分配服务不可用"));
        }
        let value: LeaseWire = self.request(reqwest::Method::POST, "/v1/leases").await?;
        let address = self.check_identity(&value)?;
        if value.state != "pending"
            || value.username.is_empty()
            || value.password.is_empty()
            || value.port == 0
        {
            return Err(AdminError::upstream_result_unknown(
                "IPv6 分配结果缺少有效代理",
            ));
        }
        let host: std::net::IpAddr = value
            .host
            .parse()
            .map_err(|_| AdminError::upstream_result_unknown("IPv6 代理入口地址无效"))?;
        let mut url = reqwest::Url::parse(&format!(
            "http://{}",
            std::net::SocketAddr::new(host, value.port)
        ))
        .map_err(|_| AdminError::upstream_result_unknown("IPv6 代理入口地址无效"))?;
        url.set_username(&value.username)
            .map_err(|()| AdminError::invalid("IPv6 代理认证无效"))?;
        url.set_password(Some(&value.password))
            .map_err(|()| AdminError::invalid("IPv6 代理认证无效"))?;
        let proxy = OutboundProxy::parse(url.as_str())
            .map_err(|_| AdminError::upstream_result_unknown("IPv6 代理配置无效"))?;
        Ok(Ipv6RemoteLease {
            id: value.id,
            address,
            proxy,
        })
    }

    async fn activate(&self, lease: &Ipv6RemoteLease) -> Result<(), AdminError> {
        let value: LeaseWire = self
            .request(
                reqwest::Method::POST,
                &format!("/v1/leases/{}/activate", lease.id),
            )
            .await?;
        if self.check_identity(&value)? != lease.address
            || value.id != lease.id
            || value.state != "active"
        {
            return Err(AdminError::upstream_result_unknown(
                "IPv6 激活结果与租约不匹配",
            ));
        }
        Ok(())
    }

    async fn retire(&self, lease: &Ipv6RemoteLease) -> Result<(), AdminError> {
        let value: LeaseWire = self
            .request(reqwest::Method::DELETE, &format!("/v1/leases/{}", lease.id))
            .await?;
        if value.id != lease.id || !matches!(value.state.as_str(), "retiring" | "retired") {
            return Err(AdminError::upstream_result_unknown(
                "IPv6 退役结果与租约不匹配",
            ));
        }
        Ok(())
    }
}
