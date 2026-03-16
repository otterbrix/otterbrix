use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) level: i32,
    pub(crate) log_path: PathBuf,
    pub(crate) wal_path: PathBuf,
    pub(crate) disk_path: PathBuf,
    pub(crate) main_path: PathBuf,
    pub(crate) wal_on: bool,
    pub(crate) disk_on: bool,
    pub(crate) sync_to_disk: bool,
}

impl Config {
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        let base = base_path.as_ref();
        Config {
            level: 0,
            log_path: base.join("log"),
            wal_path: base.join("wal"),
            disk_path: base.join("disk"),
            main_path: base.join("main"),
            wal_on: false,
            disk_on: false,
            sync_to_disk: false,
        }
    }

    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    level: i32,
    log_path: Option<PathBuf>,
    wal_path: Option<PathBuf>,
    disk_path: Option<PathBuf>,
    main_path: Option<PathBuf>,
    wal_on: bool,
    disk_on: bool,
    sync_to_disk: bool,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            level: 0,
            log_path: None,
            wal_path: None,
            disk_path: None,
            main_path: None,
            wal_on: true,
            disk_on: true,
            sync_to_disk: true,
        }
    }
}

impl ConfigBuilder {
    pub fn level(mut self, level: i32) -> Self {
        self.level = level;
        self
    }

    pub fn log_path(mut self, path: impl AsRef<Path>) -> Self {
        self.log_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn wal_path(mut self, path: impl AsRef<Path>) -> Self {
        self.wal_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn disk_path(mut self, path: impl AsRef<Path>) -> Self {
        self.disk_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn main_path(mut self, path: impl AsRef<Path>) -> Self {
        self.main_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn wal_on(mut self, on: bool) -> Self {
        self.wal_on = on;
        self
    }

    pub fn disk_on(mut self, on: bool) -> Self {
        self.disk_on = on;
        self
    }

    pub fn sync_to_disk(mut self, sync: bool) -> Self {
        self.sync_to_disk = sync;
        self
    }

    pub fn build(self) -> Config {
        let base = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Config {
            level: self.level,
            log_path: self.log_path.unwrap_or_else(|| base.join("log")),
            wal_path: self.wal_path.unwrap_or_else(|| base.join("wal")),
            disk_path: self.disk_path.unwrap_or_else(|| base.join("disk")),
            main_path: self.main_path.unwrap_or_else(|| base.clone()),
            wal_on: self.wal_on,
            disk_on: self.disk_on,
            sync_to_disk: self.sync_to_disk,
        }
    }
}
