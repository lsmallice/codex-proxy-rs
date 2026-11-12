# v3.43.4

## 问题修复

- Lite 请求兼容：HTTP 与 WebSocket 明确启用 Lite 时，将顶层 parallel_tool_calls 固定为布尔 false，避免上游因并行工具参数拒绝请求
- 参数补正：覆盖参数缺省、true、字符串和其他非 false 值，reasoning.context 已为 all_turns 时仍执行补正
- 依赖安全：升级 OpenSSL 及 SQLx 固定版本，引入 libsqlite3-sys 安全修复，同时保留事务取消回滚修复

## 优化改进

- 二开配置：将“Lite 上下文补正”统一命名为“Lite 请求兼容”，同一开关同时处理上下文与并行工具参数

## 升级须知

- 无数据库迁移或新增配置，保留 openai.request_compatibility.lite_context 配置键及原有值，默认关闭
- 开关开启时仅修正明确 Lite 请求，禁用并行工具调用但保留普通工具调用、工具定义和历史，非 Lite 或关闭开关的请求保持原样
- WS 当前帧的 Lite 标记优先于握手头，明确关闭或非法标记不回退到旧握手状态
- 镜像为 ghcr.io/lsmallice/codex-proxy-rs:3.43.4，发布不自动升级运行实例
