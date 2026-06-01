use std::fmt::{self, Debug};
use std::path::Path;
use std::str::FromStr;

use futures_core::future::BoxFuture;
use log::LevelFilter;
use otterbrix::Config;
use sqlx_core::connection::{ConnectOptions, LogSettings};
use sqlx_core::error::Error;
use sqlx_core::Url;

use crate::connection::OtterbrixConnection;

/// Connection options for the [`Otterbrix`](crate::Otterbrix) SQLx driver.
///
/// `OtterbrixConnectOptions` is the [`ConnectOptions`] implementation used
/// by SQLx to open an [`OtterbrixConnection`]. The options carry the
/// underlying [`otterbrix::Config`], the filesystem base path used to build
/// it, and SQLx's standard [`LogSettings`].
///
/// # URL syntax
///
/// The driver registers the `otterbrix` URL scheme. The path component of
/// the URL is the base directory where the engine stores its log, WAL and
/// data segments:
///
/// ```text
/// otterbrix:///var/lib/myapp
/// otterbrix://./relative/dir
/// ```
///
/// All three forms `otterbrix://<path>`, `otterbrix:<path>` and a bare
/// path (no scheme) are accepted by [`FromStr`]; [`from_url`](ConnectOptions::from_url)
/// requires the explicit `otterbrix` scheme.
///
/// # Examples
///
/// ```no_run
/// use sqlx_core::connection::ConnectOptions;
/// use sqlx_otterbrix::OtterbrixConnectOptions;
///
/// # async fn run() -> Result<(), sqlx_core::error::Error> {
/// let opts = OtterbrixConnectOptions::new("./data");
/// let _conn = opts.connect().await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct OtterbrixConnectOptions {
    pub(crate) config: Config,
    /// Directory passed to [`Config::new`] when this options value was built.
    pub(crate) storage_dir: std::path::PathBuf,
    pub(crate) log_settings: LogSettings,
}

impl OtterbrixConnectOptions {
    /// Builds options from a base storage directory using
    /// [`Config::new`]'s defaults (no WAL, no on-disk segments).
    ///
    /// Suitable for examples and tests; for durable databases use
    /// [`OtterbrixConnectOptions::from_config`] with a builder-configured
    /// [`Config`].
    #[must_use]
    pub fn new(base: impl AsRef<std::path::Path>) -> Self {
        let base = base.as_ref().to_path_buf();
        Self {
            config: Config::new(&base),
            storage_dir: base,
            log_settings: LogSettings::default(),
        }
    }

    /// Builds options from a fully formed [`Config`] and a separate
    /// `storage_dir` used for diagnostic display and URL round-tripping.
    #[must_use]
    pub fn from_config(config: Config, storage_dir: impl AsRef<std::path::Path>) -> Self {
        Self {
            config,
            storage_dir: storage_dir.as_ref().to_path_buf(),
            log_settings: LogSettings::default(),
        }
    }

    /// Returns a shared reference to the underlying [`Config`].
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a mutable reference to the underlying [`Config`], allowing
    /// in-place tweaks before [`connect`](ConnectOptions::connect).
    pub fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }
}

impl Debug for OtterbrixConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtterbrixConnectOptions")
            .field("storage_dir", &self.storage_dir)
            .finish_non_exhaustive()
    }
}

impl FromStr for OtterbrixConnectOptions {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(rest) = s.strip_prefix("otterbrix://") {
            return Ok(Self::new(Path::new(rest)));
        }
        if let Some(rest) = s.strip_prefix("otterbrix:") {
            return Ok(Self::new(Path::new(rest)));
        }
        Ok(Self::new(Path::new(s)))
    }
}

impl ConnectOptions for OtterbrixConnectOptions {
    type Connection = OtterbrixConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "otterbrix" {
            return Err(Error::Configuration(
                format!("expected otterbrix URL scheme, got {}", url.scheme()).into(),
            ));
        }
        let path = url.path();
        if path.is_empty() || path == "/" {
            return Err(Error::Configuration(
                "otterbrix URL must include a filesystem base path".into(),
            ));
        }
        Ok(Self::new(path))
    }

    fn to_url_lossy(&self) -> Url {
        let path_str = self.storage_dir.to_string_lossy();
        Url::parse(&format!("otterbrix://{path_str}"))
            .unwrap_or_else(|_| Url::parse("otterbrix://.").expect("static URL"))
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>>
    where
        Self::Connection: Sized,
    {
        let cfg = self.config.clone();
        let log_settings = self.log_settings.clone();
        Box::pin(async move {
            let db = tokio::task::spawn_blocking(move || otterbrix::Database::open(cfg))
                .await
                .map_err(|e| Error::protocol(format!("task join: {e}")))?
                .map_err(crate::convert::map_otterbrix_error)?;
            Ok(OtterbrixConnection {
                inner: std::sync::Arc::new(parking_lot::Mutex::new(db)),
                log_settings,
            })
        })
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: std::time::Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx_core::connection::ConnectOptions;
    use sqlx_core::Url;

    #[test]
    fn from_str_with_otterbrix_scheme_double_slash() {
        let opts: OtterbrixConnectOptions = "otterbrix:///tmp/foo".parse().expect("parse");
        assert_eq!(opts.storage_dir, std::path::PathBuf::from("/tmp/foo"));
    }

    #[test]
    fn from_str_with_otterbrix_scheme_single_slash() {
        let opts: OtterbrixConnectOptions = "otterbrix:/tmp/foo".parse().expect("parse");
        assert_eq!(opts.storage_dir, std::path::PathBuf::from("/tmp/foo"));
    }

    #[test]
    fn from_str_with_bare_path_falls_back_to_path() {
        let opts: OtterbrixConnectOptions = "/tmp/bar".parse().expect("parse");
        assert_eq!(opts.storage_dir, std::path::PathBuf::from("/tmp/bar"));
    }

    #[test]
    fn from_url_accepts_otterbrix_scheme() {
        let url: Url = "otterbrix:///tmp/baz".parse().expect("url");
        let opts = OtterbrixConnectOptions::from_url(&url).expect("from_url");
        assert_eq!(opts.storage_dir, std::path::PathBuf::from("/tmp/baz"));
    }

    #[test]
    fn from_url_rejects_other_schemes() {
        let url: Url = "postgres://user@host/db".parse().expect("url");
        let err = OtterbrixConnectOptions::from_url(&url).expect_err("must reject");
        let msg = err.to_string();
        assert!(msg.contains("otterbrix"), "msg = {msg}");
    }

    #[test]
    fn from_url_rejects_empty_path() {
        let url: Url = "otterbrix://".parse().expect("url");
        let err = OtterbrixConnectOptions::from_url(&url).expect_err("must reject");
        let msg = err.to_string();
        assert!(msg.contains("filesystem base path"), "msg = {msg}");
    }

    #[test]
    fn to_url_lossy_round_trips_to_otterbrix_scheme() {
        let opts = OtterbrixConnectOptions::new("/tmp/qux");
        let url = opts.to_url_lossy();
        assert_eq!(url.scheme(), "otterbrix");
        assert!(url.as_str().contains("/tmp/qux"));
    }

    #[test]
    fn debug_includes_storage_dir() {
        let opts = OtterbrixConnectOptions::new("/tmp/dbg");
        let s = format!("{opts:?}");
        assert!(s.contains("storage_dir"), "got {s}");
        assert!(s.contains("/tmp/dbg"), "got {s}");
    }
}
