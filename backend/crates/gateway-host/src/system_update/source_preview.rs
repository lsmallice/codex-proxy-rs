//! 固定发行来源的只读查询与切换前置条件，不下载包或修改安装状态
use super::download::{MAX_CHECKSUM_SIZE, MAX_DOWNLOAD_SIZE};
use super::release::{
    fetch_preview_release, select_archive, validate_download_url, validate_repository,
    validate_update_target,
};
use super::{OperationError, SystemUpdateConfig, invalid};
use gateway_admin::model::system::{
    SystemReleaseSource, SystemSourceCheck, SystemSourceCheckStatus as Status, SystemSourcePreview,
    SystemUpdateChannel,
};

pub(super) async fn preview(
    config: &SystemUpdateConfig,
    source: SystemReleaseSource,
    channel: SystemUpdateChannel,
) -> Result<SystemSourcePreview, OperationError> {
    if channel == SystemUpdateChannel::Experimental {
        return Err(invalid("发行来源预检不支持实验通道"));
    }
    let repository = match source {
        SystemReleaseSource::Fork => "lsmallice/codex-proxy-rs",
        SystemReleaseSource::Official => "zyycn/codex-proxy-rs",
    };
    let release = fetch_preview_release(
        &config.github_api_base,
        repository,
        &config.version,
        channel,
    )
    .await?;
    let latest = release
        .as_ref()
        .map(|r| r.tag_name.trim().trim_start_matches('v').to_owned());
    let configured = config
        .update_repository
        .as_ref()
        .filter(|value| validate_repository(value).is_ok())
        .cloned();
    let mut checks = Vec::new();
    let mut check = |id: &str, status, message: &str| {
        checks.push(SystemSourceCheck {
            id: id.to_owned(),
            status,
            message: message.to_owned(),
        })
    };
    check(
        "source",
        if configured.as_deref() == Some(repository) {
            Status::Passed
        } else {
            Status::Blocked
        },
        if configured.as_deref() == Some(repository) {
            "当前有效更新源与目标一致，仍需核对部署文件"
        } else {
            "更新源与目标不同，需同步修改 YAML 或 Compose，查询不会保存来源"
        },
    );
    if let Some(reason) = config.update_support_error() {
        check("build", Status::Blocked, &reason);
    } else {
        check("build", Status::Passed, "当前构建满足原生在线更新前提");
    }
    match latest.as_deref() {
        None => check("version", Status::Blocked, "所选通道未找到当前大版本的发行"),
        Some(version) => match validate_update_target(&config.version, version, channel) {
            Ok(()) => check(
                "version",
                Status::Passed,
                "目标满足原生版本规则，不代表已验证数据兼容",
            ),
            Err(error) => check("version", Status::Blocked, error.message()),
        },
    }
    let assets = release
        .as_ref()
        .zip(latest.as_deref())
        .is_some_and(|(release, version)| {
            select_archive(release, version).is_ok_and(|asset| {
                asset.size > 0
                    && asset.size <= MAX_DOWNLOAD_SIZE
                    && validate_download_url(&asset.browser_download_url, &config.github_api_base)
                        .is_ok()
            }) && release.assets.iter().any(|asset| {
                asset.name == "checksums.txt"
                    && asset.size > 0
                    && asset.size <= MAX_CHECKSUM_SIZE
                    && validate_download_url(&asset.browser_download_url, &config.github_api_base)
                        .is_ok()
            })
        });
    check(
        "assets",
        if assets {
            Status::Passed
        } else {
            Status::Blocked
        },
        if assets {
            "发行元数据包含本机平台包和校验文件，尚未下载验真"
        } else {
            "缺少本机平台包或校验文件，或下载地址与大小不符合要求"
        },
    );
    check(
        "database",
        Status::Pending,
        "数据库尚未验证，需备份并在隔离库核对目标迁移，不能仅凭版本号判断兼容",
    );
    check(
        "plugins",
        Status::Pending,
        "插件尚未验证，需校验目标发行清单并检查已启用插件",
    );
    check(
        "deployment",
        Status::Pending,
        if config.deployment_mode == "docker" {
            "需同步 Compose 镜像与更新源并保留数据卷，否则重建容器可能回到原来源"
        } else {
            "需持久化目标更新源，并保留完整发行文件和数据备份"
        },
    );
    check(
        "switch",
        Status::Pending,
        "仅提供只读预检，不执行安装、切换或重启",
    );
    Ok(SystemSourcePreview {
        source,
        repository: repository.to_owned(),
        configured_repository: configured,
        channel,
        current_version: config.version.clone(),
        latest_version: latest,
        // 使用受控仓库和已验证 SemVer 生成链接，不将任意远端 html_url 作为页面跳转
        release_url: release.as_ref().map(|r| {
            format!(
                "https://github.com/{repository}/releases/tag/{}",
                r.tag_name.trim()
            )
        }),
        notes: release.and_then(|r| r.body.or(r.name)),
        installation_allowed: false,
        checks,
    })
}
