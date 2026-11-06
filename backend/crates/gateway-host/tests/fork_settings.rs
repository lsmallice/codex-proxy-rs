//! 验证二开设置的保存、生效分离、冲突与损坏文件保护
use gateway_admin::{
    model::{AdminError, MutationActor, MutationContext},
    ports::fork_settings::ForkSettingsService,
};
use gateway_host::fork_settings::{ForkSettingsStore, PreparedActivation, read_saved};
use serde_json::json;
use std::sync::Arc;

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "fixture".into(),
    }
}
fn validate() -> gateway_host::fork_settings::Validator {
    Arc::new(|value| {
        if value["value"].as_u64().is_some_and(|n| n <= 5) {
            Ok(value)
        } else {
            Err(AdminError::invalid("测试配置无效"))
        }
    })
}

#[tokio::test]
async fn hot_settings_publish_only_after_persistence_and_keep_startup_resources_pending() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork-settings.json");
    let published = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let publisher = published.clone();
    let saved_path = path.clone();
    let baseline = json!({"value":1,"capacity":10});
    let store = ForkSettingsStore::new(path.clone(), baseline.clone(), baseline, validate())
        .with_activation(Arc::new(move |values| {
            if values["value"] == 4 {
                return Err(AdminError::invalid("测试发布准备失败"));
            }
            let value = values["value"].as_u64().unwrap();
            let mut active = values.clone();
            active["capacity"] = json!(10);
            let publisher = publisher.clone();
            let path = saved_path.clone();
            Ok(PreparedActivation {
                active,
                publish: Box::new(move || {
                    assert_eq!(read_saved(&path).unwrap().unwrap()["value"], value);
                    publisher.store(value, std::sync::atomic::Ordering::SeqCst);
                }),
            })
        }));
    let before = store.load().await.unwrap();
    let hot = store
        .save(
            before.revision,
            json!({"value":2,"capacity":10}),
            &context(),
        )
        .await
        .unwrap();
    assert!(!hot.restart_required);
    assert_eq!(hot.active, hot.saved);
    let mixed = store
        .save(hot.revision, json!({"value":3,"capacity":20}), &context())
        .await
        .unwrap();
    assert!(mixed.restart_required);
    assert_eq!(mixed.active, json!({"value":3,"capacity":10}));
    assert_eq!(published.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert!(
        store
            .save(
                mixed.revision.clone(),
                json!({"value":4,"capacity":20}),
                &context()
            )
            .await
            .is_err()
    );
    assert_eq!(read_saved(&path).unwrap().unwrap(), mixed.saved);
    // 备份路径无法写入时，原配置文件与内存策略都不变
    let backup = path.with_extension("backup.json");
    std::fs::remove_file(&backup).unwrap();
    std::fs::create_dir(&backup).unwrap();
    assert!(
        store
            .save(mixed.revision, json!({"value":5,"capacity":20}), &context())
            .await
            .is_err()
    );
    assert_eq!(published.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(read_saved(&path).unwrap().unwrap(), mixed.saved);
    assert_eq!(store.load().await.unwrap().active, mixed.active);
}
#[tokio::test]
async fn save_is_persistent_but_activation_requires_new_process() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork-settings.json");
    let baseline = json!({"value":1});
    let store =
        ForkSettingsStore::new(path.clone(), baseline.clone(), baseline.clone(), validate());
    let initial = store.load().await.unwrap();
    assert!(!initial.restart_required);
    let saved = store
        .save(initial.revision.clone(), json!({"value":2}), &context())
        .await
        .unwrap();
    assert_eq!(saved.active, baseline);
    assert_eq!(saved.saved, json!({"value":2}));
    assert!(saved.restart_required);
    assert!(
        store
            .save(initial.revision, json!({"value":3}), &context())
            .await
            .is_err()
    );
    assert!(
        store
            .save(saved.revision.clone(), json!({"value":9}), &context())
            .await
            .is_err()
    );
    let restored = read_saved(&path).unwrap().unwrap();
    assert_eq!(restored, json!({"value":2}));
    let restarted = ForkSettingsStore::new(path.clone(), baseline, restored, validate());
    assert!(!restarted.load().await.unwrap().restart_required);
    let unchanged = store
        .save(saved.revision.clone(), saved.saved.clone(), &context())
        .await
        .unwrap();
    assert_eq!(unchanged.revision, saved.revision);
    store
        .save(saved.revision, json!({"value":3}), &context())
        .await
        .unwrap();
    assert_eq!(
        read_saved(&path.with_extension("backup.json"))
            .unwrap()
            .unwrap(),
        json!({"value":2})
    );
}
#[tokio::test]
async fn broken_configuration_is_not_silently_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork-settings.json");
    let store = ForkSettingsStore::new(
        path.clone(),
        json!({"value":1}),
        json!({"value":1}),
        validate(),
    );
    std::fs::write(&path, b"broken").unwrap();
    assert!(store.load().await.is_err());
    assert!(
        store
            .save("old".into(), json!({"value":2}), &context())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"broken");
}

#[tokio::test]
async fn size_limits_and_concurrent_writes_preserve_last_committed_values() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork-settings.json");
    let store = ForkSettingsStore::new(
        path.clone(),
        json!({"value":1}),
        json!({"value":1}),
        validate(),
    );
    let revision = store.load().await.unwrap().revision;
    assert!(
        store
            .save(
                revision.clone(),
                json!({"value":2,"padding":"x".repeat(65536)}),
                &context()
            )
            .await
            .is_err()
    );
    assert!(!path.exists());
    let context = context();
    let (first, second) = tokio::join!(
        store.save(revision.clone(), json!({"value":2}), &context),
        store.save(revision, json!({"value":3}), &context)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let committed = first.or(second).unwrap();
    assert_eq!(read_saved(&path).unwrap().unwrap(), committed.saved);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    std::fs::write(&path, vec![b' '; 65537]).unwrap();
    assert!(read_saved(&path).is_err());
    std::fs::write(&path, json!({"schema":2,"values":{"value":1}}).to_string()).unwrap();
    assert!(read_saved(&path).is_err());
}
