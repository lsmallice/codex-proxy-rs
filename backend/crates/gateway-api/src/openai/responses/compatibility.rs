//! Responses 路由前的预算与工具结构补正，不删除工具或改写历史
use gateway_core::error::{GatewayError, GatewayErrorKind};
use gateway_core::operation::Operation;
use serde_json::Value;
use std::sync::Arc;

type PreparePolicy = dyn Fn(Operation) -> Result<(Operation, ResponsesIngressCompatibility), GatewayError>
    + Send
    + Sync;
/// 组合根提供同一份入站与 Provider 策略快照，API 不解释 Provider 私有字段
#[derive(Clone)]
pub struct ResponsesPolicySource(Arc<PreparePolicy>);
impl std::fmt::Debug for ResponsesPolicySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResponsesPolicySource")
    }
}
impl PartialEq for ResponsesPolicySource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ResponsesPolicySource {}
impl ResponsesPolicySource {
    pub fn new(
        prepare: impl Fn(Operation) -> Result<(Operation, ResponsesIngressCompatibility), GatewayError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self(Arc::new(prepare))
    }
    pub(in crate::openai) fn freeze(
        &self,
        operation: Operation,
    ) -> Result<(Operation, ResponsesIngressCompatibility), GatewayError> {
        (self.0)(operation)
    }
}

/// 由组合根注入的 Responses 入站策略，不依赖插件实例或配置格式
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResponsesIngressCompatibility {
    pub min_output_tokens_16: bool,
    pub tool_structure: bool,
}

pub(in crate::openai) fn prepare(
    operation: Operation,
    policy: ResponsesIngressCompatibility,
) -> Result<(Operation, usize, bool), GatewayError> {
    if !policy.min_output_tokens_16 && !policy.tool_structure {
        return Ok((operation, 0, false));
    }
    let Operation::Generate(request) = &operation else {
        return Ok((operation, 0, false));
    };
    let body = request.protocol_payload().body();
    if request.protocol_payload().protocol() != "openai"
        || !body
            .get("model")
            .and_then(Value::as_str)
            .is_some_and(|model| model.starts_with("gpt-") || model.starts_with("codex-"))
    {
        return Ok((operation, 0, false));
    }
    let minimum = policy.min_output_tokens_16
        && body
            .get("max_output_tokens")
            .and_then(Value::as_i64)
            .is_some_and(|value| value < 16);
    let mut patches = Vec::new();
    let mut choice = None;
    if policy.tool_structure {
        if let Some(tools) = body.get("tools").and_then(Value::as_array) {
            for (index, tool) in tools.iter().enumerate() {
                if let Some(normalized) = normalize_function(tool)? {
                    patches.push((index, normalized));
                }
            }
        }
        if let Some(original) = body.get("tool_choice") {
            choice = normalize_function(original)?;
            if original.get("type").and_then(Value::as_str) == Some("allowed_tools")
                && let Some(tools) = original.get("tools").and_then(Value::as_array)
            {
                let mut normalized = tools.clone();
                let mut changed = false;
                for tool in &mut normalized {
                    if let Some(next) = normalize_function(tool)? {
                        *tool = next;
                        changed = true;
                    }
                }
                if changed {
                    let mut next = original.clone();
                    next["tools"] = Value::Array(normalized);
                    choice = Some(next);
                }
            }
        }
    }
    if !minimum && patches.is_empty() && choice.is_none() {
        return Ok((operation, 0, false));
    }
    let normalized_count = patches.len() + usize::from(choice.is_some());
    let mut body = body.clone();
    if minimum {
        body.insert("max_output_tokens".into(), Value::from(16));
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for (index, tool) in patches {
            tools[index] = tool;
        }
    }
    if let Some(choice) = choice {
        body.insert("tool_choice".into(), choice);
    }
    let bytes = serde_json::to_vec(&body).map_err(|_| invalid_body())?;
    let operation = operation
        .replace_middleware_wire("openai", bytes.into())
        .map_err(|_| invalid_body())?;
    Ok((operation, normalized_count, minimum))
}
fn invalid_body() -> GatewayError {
    GatewayError::new(
        GatewayErrorKind::InvalidRequest,
        "Responses request could not be normalized.",
    )
}
fn valid_name(name: Option<&Value>) -> bool {
    name.and_then(Value::as_str).is_some_and(|name| {
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    })
}
fn invalid_name() -> GatewayError {
    GatewayError::new(GatewayErrorKind::InvalidRequest, "Tool names must contain only letters, digits, underscores or hyphens. Tools and history were not removed.")
        .with_client_code("invalid_tool_name")
}
fn normalize_function(tool: &Value) -> Result<Option<Value>, GatewayError> {
    let kind = tool.get("type").and_then(Value::as_str);
    if !matches!(kind, Some("function" | "custom")) {
        return Ok(None);
    }
    let Some(nested) = tool.get("function") else {
        return if valid_name(tool.get("name")) {
            Ok(None)
        } else {
            Err(invalid_name())
        };
    };
    if kind != Some("function") {
        return if valid_name(tool.get("name")) {
            Ok(None)
        } else {
            Err(invalid_name())
        };
    }
    let Some(function) = nested.as_object() else {
        return Err(GatewayError::new(
            GatewayErrorKind::InvalidRequest,
            "Nested function declaration must be an object.",
        )
        .with_client_code("invalid_tool_structure"));
    };
    // 仅提升规范已知字段；混合结构出现冲突时不猜测哪个定义正确
    if function.keys().any(|key| {
        !matches!(
            key.as_str(),
            "name" | "description" | "parameters" | "strict"
        )
    }) {
        return Err(GatewayError::new(
            GatewayErrorKind::InvalidRequest,
            "Nested function declaration contains unsupported fields.",
        )
        .with_client_code("invalid_tool_structure"));
    }
    for (key, value) in function {
        if tool.get(key).is_some_and(|outer| outer != value) {
            return Err(GatewayError::new(
                GatewayErrorKind::InvalidRequest,
                "Top-level and nested function definitions conflict.",
            )
            .with_client_code("invalid_tool_structure"));
        }
    }
    let name = tool.get("name").or_else(|| function.get("name"));
    if !valid_name(name) {
        return Err(invalid_name());
    }
    let mut normalized = tool
        .as_object()
        .expect("nested function belongs to an object")
        .clone();
    for (key, value) in function {
        normalized
            .entry(key.clone())
            .or_insert_with(|| value.clone());
    }
    normalized.remove("function");
    Ok(Some(Value::Object(normalized)))
}
