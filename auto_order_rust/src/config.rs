use std::path::{Path, PathBuf};

use tokio::fs;

use crate::models::{normalize_strategy_task, normalize_task, ConfigFile};

pub async fn load_config(path: &Path) -> ConfigFile {
    let content = match fs::read_to_string(path).await {
        Ok(text) => text,
        Err(_) => return ConfigFile::default(),
    };
    if content.trim().is_empty() {
        return ConfigFile::default();
    }
    let parsed: serde_json::Value = match serde_json::from_str(&content) {
        Ok(value) => value,
        Err(_) => return ConfigFile::default(),
    };
    let mut config: ConfigFile = serde_json::from_value(parsed).unwrap_or_default();
    config.tasks = config
        .tasks
        .iter()
        .filter_map(|task| normalize_task(task).ok())
        .collect();
    config.strategy_tasks = config
        .strategy_tasks
        .iter()
        .filter_map(|task| normalize_strategy_task(task).ok())
        .collect();
    config
}

pub async fn save_config(path: &Path, config: &ConfigFile) -> anyhow::Result<()> {
    let mut normalized = ConfigFile {
        tasks: vec![],
        strategy_tasks: vec![],
        enabled: config.enabled,
    };
    for task in &config.tasks {
        normalized.tasks.push(normalize_task(task)?);
    }
    for task in &config.strategy_tasks {
        normalized.strategy_tasks.push(normalize_strategy_task(task)?);
    }
    let text = serde_json::to_string_pretty(&normalized)?;
    fs::write(path, text).await?;
    Ok(())
}

pub fn default_config_path() -> PathBuf {
    std::env::var("AUTO_ORDER_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config.json"))
}
