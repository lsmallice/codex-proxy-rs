//! 非敏感设置的原子持久化与已校验运行快照发布
use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, MutationContext},
    ports::fork_settings::{ForkSettingsService, ForkSettingsSnapshot},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const MAX_BYTES: usize = 64 * 1024;
pub type Validator = Arc<dyn Fn(Value) -> Result<Value, AdminError> + Send + Sync>;
/// 业务 owner 预先构造新快照，落盘后只执行不可失败的内存发布
pub struct PreparedActivation {
    pub active: Value,
    pub publish: Box<dyn FnOnce() + Send>,
}
pub type ActivationPreparer =
    Arc<dyn Fn(&Value) -> Result<PreparedActivation, AdminError> + Send + Sync>;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    schema: u32,
    values: Value,
}

pub fn read_saved(path: &Path) -> Result<Option<Value>, AdminError> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(AdminError::internal("读取二开配置失败")),
    };
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| AdminError::internal("读取二开配置失败"))?;
    if bytes.len() > MAX_BYTES {
        return Err(AdminError::invalid("二开配置超过大小上限"));
    }
    let saved: Saved =
        serde_json::from_slice(&bytes).map_err(|_| AdminError::invalid("二开配置文件格式无效"))?;
    if saved.schema != 1 {
        return Err(AdminError::invalid("二开配置文件版本不受支持"));
    }
    Ok(Some(saved.values))
}

pub struct ForkSettingsStore {
    path: PathBuf,
    baseline: Value,
    active: Mutex<Value>,
    activation: Option<ActivationPreparer>,
    validate: Validator,
    lock: tokio::sync::Mutex<()>,
}
impl std::fmt::Debug for ForkSettingsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkSettingsStore").finish_non_exhaustive()
    }
}
impl ForkSettingsStore {
    #[must_use]
    pub fn new(path: PathBuf, baseline: Value, active: Value, validate: Validator) -> Self {
        Self {
            path,
            baseline,
            active: Mutex::new(active),
            activation: None,
            validate,
            lock: tokio::sync::Mutex::new(()),
        }
    }
    #[must_use]
    pub fn with_activation(mut self, prepare: ActivationPreparer) -> Self {
        self.activation = Some(prepare);
        self
    }
    fn activate(&self, prepared: Option<PreparedActivation>) {
        if let Some(prepared) = prepared {
            (prepared.publish)();
            *self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = prepared.active;
        }
    }
    fn snapshot(&self) -> Result<ForkSettingsSnapshot, AdminError> {
        let mut saved =
            (self.validate)(read_saved(&self.path)?.unwrap_or_else(|| self.baseline.clone()))?;
        saved.sort_all_objects();
        let bytes =
            serde_json::to_vec(&saved).map_err(|_| AdminError::internal("读取二开配置失败"))?;
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(ForkSettingsSnapshot {
            revision: hex::encode(Sha256::digest(bytes)),
            restart_required: saved != active,
            active,
            saved,
        })
    }
}
#[async_trait]
impl ForkSettingsService for ForkSettingsStore {
    async fn load(&self) -> Result<ForkSettingsSnapshot, AdminError> {
        let _guard = self.lock.lock().await;
        self.snapshot()
    }
    async fn save(
        &self,
        revision: String,
        values: Value,
        context: &MutationContext,
    ) -> Result<ForkSettingsSnapshot, AdminError> {
        let _guard = self.lock.lock().await;
        let current = self.snapshot()?;
        if current.revision != revision {
            return Err(AdminError::conflict("二开配置已被修改，请刷新后重试"));
        }
        let values = (self.validate)(values)?;
        let activation = self
            .activation
            .as_ref()
            .map(|prepare| prepare(&values))
            .transpose()?;
        if values == current.saved {
            self.activate(activation);
            return self.snapshot();
        }
        let bytes = serde_json::to_vec_pretty(&Saved { schema: 1, values })
            .map_err(|_| AdminError::invalid("二开配置无效"))?;
        if bytes.len() > MAX_BYTES {
            return Err(AdminError::invalid("二开配置超过大小上限"));
        }
        let parent = self
            .path
            .parent()
            .ok_or_else(|| AdminError::internal("二开配置保存位置无效"))?;
        fs::create_dir_all(parent).map_err(|_| AdminError::internal("二开配置目录不可写"))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| AdminError::internal("系统时钟无效"))?
            .as_nanos();
        let temp = self
            .path
            .with_extension(format!("{}-{nonce}.next", std::process::id()));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .map_err(|_| AdminError::internal("二开配置暂存文件不可写"))?;
        let result = (|| {
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            if self.path.exists() {
                fs::copy(&self.path, self.path.with_extension("backup.json"))?;
            }
            fs::rename(&temp, &self.path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
            return Err(AdminError::internal("保存二开配置失败，原配置未确认变更"));
        }
        self.activate(activation);
        tracing::info!(request_id=%context.request_id,"Fork settings saved and available runtime policy published");
        self.snapshot()
    }
}
