//! IPv6 控制服务的地址段校验、凭据隔离与租约协议测试

use gateway_admin::ports::ipv6::Ipv6Controller;
use gateway_host::{Ipv6EgressConfig, ipv6_controller::HttpIpv6Controller};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

fn config(server: &MockServer) -> Ipv6EgressConfig {
    Ipv6EgressConfig {
        id: "pool-a".into(),
        control_url: server.uri(),
        token: "test-only".into(),
        prefix: "2001:db8::/80".into(),
    }
}

#[tokio::test]
async fn configured_prefix_is_checked_before_allocating() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/status"))
        .and(header("authorization", "Bearer test-only"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "configured":true,"available":true,"prefix":"2001:db8:1::/80","issued":"0"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/leases"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let client = HttpIpv6Controller::new(&config(&server)).unwrap();
    assert!(client.allocate().await.is_err());
}

#[tokio::test]
async fn proxy_credentials_are_encoded_and_redacted_and_redirects_are_not_followed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "configured":true,"available":true,"prefix":"2001:db8::/80","issued":"0"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST")).and(path("/v1/leases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":"1","address":"2001:db8::1","state":"pending","host":"127.0.0.1","port":11880,"username":"test-user","password":"test:p@ss"
        }))).mount(&server).await;
    let client = HttpIpv6Controller::new(&config(&server)).unwrap();
    let lease = client.allocate().await.unwrap();
    assert_eq!(lease.address.to_string(), "2001:db8::1");
    assert!(!format!("{lease:?}").contains("p@ss"));
    assert!(!format!("{:?}", config(&server)).contains("test-only"));
    assert!(lease.proxy.expose_url().contains("%40"));
    Mock::given(method("POST"))
        .and(path("/v1/leases/1/activate"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", format!("{}/redirect", server.uri())),
        )
        .mount(&server)
        .await;
    assert!(client.activate(&lease).await.is_err());
    assert!(
        !server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path() == "/redirect")
    );
}
