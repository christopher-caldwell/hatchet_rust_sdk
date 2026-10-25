//! Engine versions and the prerequisites published by the official SDKs.
use crate::{Error, Result};

/// A parsed engine release. Development or malformed versions are rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineVersion(semver::Version);

/// Features with a documented minimum engine release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineFeature {
    SlotConfig,
    DurableEviction,
    Observability,
    RetryAfter,
    /// SDK support floor verified against v0.111.0; not a claim about the first release.
    WorkflowPause,
    /// Conservative SDK support floor verified against official release contracts.
    Idempotency,
    /// Requires the newer PauseRateLimit RPC, present in v0.114.0.
    RateLimitPause,
    /// Conservative preview support floor, verified against v0.114.0.
    BatchTasks,
}

impl EngineFeature {
    #[must_use]
    pub const fn minimum_version(self) -> &'static str {
        match self {
            Self::SlotConfig => "0.78.23",
            Self::DurableEviction => "0.80.0",
            Self::Observability => "0.82.0",
            Self::RetryAfter | Self::WorkflowPause | Self::Idempotency => "0.111.0",
            Self::RateLimitPause | Self::BatchTasks => "0.114.0",
        }
    }
}

impl EngineVersion {
    /// Parses a semantic release, with an optional `v` prefix.
    /// # Errors
    /// Returns an error when the engine does not identify a semantic release.
    pub fn parse(version: &str) -> Result<Self> {
        semver::Version::parse(version.strip_prefix('v').unwrap_or(version))
            .map(Self)
            .map_err(|error| {
                Error::Transport(format!("invalid engine version {version:?}: {error}"))
            })
    }

    #[must_use]
    pub fn supports(&self, feature: EngineFeature) -> bool {
        self.0
            >= semver::Version::parse(feature.minimum_version()).expect("static semantic version")
    }

    /// # Errors
    /// Returns an unsupported-operation error for an older engine.
    pub fn require(&self, feature: EngineFeature) -> Result<()> {
        if self.supports(feature) {
            Ok(())
        } else {
            Err(Error::UnsupportedOperation(format!(
                "{feature:?} requires engine >= {}; connected engine is {self}",
                feature.minimum_version()
            )))
        }
    }
}

impl std::fmt::Display for EngineVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_respect_boundaries_prereleases_and_unknown_versions() {
        assert!(
            EngineVersion::parse("v0.94.14")
                .unwrap()
                .supports(EngineFeature::DurableEviction)
        );
        assert!(
            !EngineVersion::parse("0.79.9")
                .unwrap()
                .supports(EngineFeature::DurableEviction)
        );
        assert!(
            !EngineVersion::parse("0.80.0-rc.1")
                .unwrap()
                .supports(EngineFeature::DurableEviction)
        );
        assert!(
            EngineVersion::parse("0.80.0+build.1")
                .unwrap()
                .supports(EngineFeature::DurableEviction)
        );
        assert!(EngineVersion::parse("development").is_err());
        assert!(EngineVersion::parse("").is_err());
        assert!(
            EngineVersion::parse("0.94.14")
                .unwrap()
                .require(EngineFeature::RetryAfter)
                .is_err()
        );
    }
}
