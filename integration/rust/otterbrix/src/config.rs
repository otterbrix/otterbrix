use std::path::{Path, PathBuf};

/// Configuration for opening a [`Database`](crate::Database).
///
/// `Config` describes where the engine stores its on-disk artefacts and how
/// it manages durability. Construct it either with [`Config::new`] (defaults
/// suitable for examples and tests, with all on-disk subsystems disabled) or
/// with [`Config::builder`] for fine-grained control.
///
/// All path fields are owned [`PathBuf`]s; relative paths are resolved by the
/// engine relative to the process's current directory.
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
    /// Builds a `Config` rooted at `base_path` with all durability features
    /// disabled.
    ///
    /// The four storage subdirectories are placed under `base_path`:
    ///
    /// | Field        | Path                  |
    /// |--------------|-----------------------|
    /// | `log_path`   | `<base_path>/log`     |
    /// | `wal_path`   | `<base_path>/wal`     |
    /// | `disk_path`  | `<base_path>/disk`    |
    /// | `main_path`  | `<base_path>/main`    |
    ///
    /// `wal_on`, `disk_on`, and `sync_to_disk` are all set to `false`. This
    /// makes the configuration suitable for ephemeral databases (tests,
    /// benchmarks, in-memory experiments) but **not** for production data.
    /// For a durable configuration, use [`Config::builder`].
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

    /// Returns a fresh [`ConfigBuilder`] with default values.
    ///
    /// See [`ConfigBuilder`] for the list of defaults.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

/// Fluent builder for [`Config`].
///
/// Obtain an instance via [`Config::builder`] or [`ConfigBuilder::default`],
/// chain setter methods, and call [`ConfigBuilder::build`] to produce a
/// [`Config`].
///
/// # Defaults
///
/// | Field          | Default                                 |
/// |----------------|------------------------------------------|
/// | `level`        | `0`                                     |
/// | `log_path`     | `<cwd>/log`                             |
/// | `wal_path`     | `<cwd>/wal`                             |
/// | `disk_path`    | `<cwd>/disk`                            |
/// | `main_path`    | `<cwd>` (current directory)             |
/// | `wal_on`       | `true`                                  |
/// | `disk_on`      | `true`                                  |
/// | `sync_to_disk` | `true`                                  |
///
/// `<cwd>` is the process's current working directory at the moment
/// [`build`](ConfigBuilder::build) is called.
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
    /// Sets the engine log verbosity level.
    pub fn level(mut self, level: i32) -> Self {
        self.level = level;
        self
    }

    /// Sets the directory where the engine writes its operational log.
    pub fn log_path(mut self, path: impl AsRef<Path>) -> Self {
        self.log_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the directory for the write-ahead log.
    ///
    /// Has no effect when [`wal_on`](ConfigBuilder::wal_on) is `false`.
    pub fn wal_path(mut self, path: impl AsRef<Path>) -> Self {
        self.wal_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the directory for on-disk storage segments.
    ///
    /// Has no effect when [`disk_on`](ConfigBuilder::disk_on) is `false`.
    pub fn disk_path(mut self, path: impl AsRef<Path>) -> Self {
        self.disk_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the engine's main working directory.
    pub fn main_path(mut self, path: impl AsRef<Path>) -> Self {
        self.main_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Enables or disables the write-ahead log.
    pub fn wal_on(mut self, on: bool) -> Self {
        self.wal_on = on;
        self
    }

    /// Enables or disables on-disk storage segments.
    pub fn disk_on(mut self, on: bool) -> Self {
        self.disk_on = on;
        self
    }

    /// Enables or disables synchronous flushes when writing to disk.
    pub fn sync_to_disk(mut self, sync: bool) -> Self {
        self.sync_to_disk = sync;
        self
    }

    /// Consumes the builder and produces a [`Config`].
    ///
    /// Any path that was not explicitly set is computed from the current
    /// working directory (see the table in [`ConfigBuilder`]). If reading the
    /// current directory fails, the literal path `.` is used as a fallback.
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
