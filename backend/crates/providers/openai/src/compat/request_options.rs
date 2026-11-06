//! 选定上游后的 Lite、存储意图与 JSON 输出协议补正，不经过插件或重放请求
use crate::config::CodexRequestCompatibility;
use crate::transport::protocol::responses::CodexResponsesRequest;
use serde_json::{Map, Value, json};

#[derive(Default)]
pub(crate) struct Corrections {
    pub(crate) store: bool,
    pub(crate) json: bool,
}
impl Corrections {
    pub(crate) fn changed(&self) -> bool {
        self.store || self.json
    }
}

pub(crate) fn apply_selected_request_compatibility(
    request: &mut CodexResponsesRequest,
    policy: CodexRequestCompatibility,
    oauth: bool,
) -> Corrections {
    let mut result = Corrections::default();
    if !request.generate() || request.is_guardian() {
        return result;
    }
    request.lite_context_compat = policy.lite_context;
    let body = request.body_mut();
    if policy.store_false && oauth && body.get("store") != Some(&Value::Bool(false)) {
        body.insert("store".into(), Value::Bool(false));
        result.store = true;
    }
    if policy.json_object {
        result.json = normalize_json(body);
    }
    result
}

pub(crate) fn normalize_lite_context(
    body: &mut Value,
    headers: &reqwest::header::HeaderMap,
    websocket: bool,
) -> bool {
    use gateway_protocol::openai::{
        WS_REQUEST_HEADER_RESPONSES_LITE_CLIENT_METADATA_KEY as MARKER,
        X_OPENAI_INTERNAL_CODEX_RESPONSES_LITE_HEADER as HEADER,
    };
    let Some(body) = body.as_object_mut() else {
        return false;
    };
    let header = || {
        let mut values = headers.get_all(HEADER).iter();
        values
            .next()
            .is_some_and(|value| value.as_bytes() == b"true")
            && values.all(|value| value.as_bytes() == b"true")
    };
    let lite = if websocket {
        match body.get("client_metadata") {
            Some(Value::Object(metadata)) => metadata
                .get(MARKER)
                .map_or_else(header, |value| value.as_str() == Some("true")),
            None | Some(Value::Null) => header(),
            _ => false,
        }
    } else {
        header()
    };
    if !lite {
        return false;
    }
    match body.get_mut("reasoning") {
        None | Some(Value::Null) => {
            body.insert("reasoning".into(), json!({"context":"all_turns"}));
        }
        Some(Value::Object(reasoning))
            if reasoning.get("context").and_then(Value::as_str) != Some("all_turns") =>
        {
            reasoning.insert("context".into(), json!("all_turns"));
        }
        _ => return false,
    }
    true
}

fn contains_json(text: &str) -> bool {
    text.as_bytes()
        .windows(4)
        .any(|word| word.eq_ignore_ascii_case(b"json"))
}
fn message_mentions_json(item: &Value) -> bool {
    if item.get("type").is_some_and(|kind| kind != "message")
        || !matches!(
            item.get("role").and_then(Value::as_str),
            Some("user" | "assistant" | "system" | "developer")
        )
    {
        return false;
    }
    match item.get("content") {
        Some(Value::String(text)) => contains_json(text),
        Some(Value::Array(parts)) => parts.iter().any(|part| {
            matches!(
                part.get("type").and_then(Value::as_str),
                Some("input_text" | "output_text")
            ) && part
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(contains_json)
        }),
        _ => false,
    }
}
fn normalize_json(body: &mut Map<String, Value>) -> bool {
    if body
        .get("text")
        .and_then(|text| text.pointer("/format/type"))
        .and_then(Value::as_str)
        != Some("json_object")
    {
        return false;
    }
    let Some(input) = body.get_mut("input") else {
        return false;
    };
    let instruction =
        json!({"role":"developer","content":"Return your response as a valid JSON object."});
    match input {
        Value::String(text) if !contains_json(text) => {
            let original = std::mem::take(text);
            *input = json!([instruction,{"role":"user","content":original}]);
        }
        Value::Array(items)
            if !items.is_empty()
                && items.iter().all(Value::is_object)
                && !items.iter().any(message_mentions_json) =>
        {
            items.insert(0, instruction);
        }
        _ => return false,
    }
    true
}
