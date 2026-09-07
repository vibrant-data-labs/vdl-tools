# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.1.0] - 2026-09-07

### Added
- `vdl_tools.causal_networks`: structural analysis of voted causal networks (keystone leverage,
  trophic level / upstream position, ensemble of vote thresholds, monte-carlo link deletion,
  Excel output, Altair scatter, openmappr player), with README, runnable examples and tests.
  Works on any nodes/links tables; Undercurrent and weighted Kumu exports are loaders.

### Changed
- Output naming for causal networks: `Reach in 2 Hops (Pct)` is now a true percent, trophic level is
  reported as `Trophic Level` (0 = upstream) plus `Upstream Score`/`Upstream Rank` (high = upstream).
  Monte-carlo trials now keep every node in every trial graph.

### Fixed
- `import vdl_tools.py2mappr` no longer requires a `[postgres]` config section (the geo lookup is
  imported only when a geo layout is built).

### Deprecated
- `vdl_tools.network_tools.trophiclevel` and `vdl_tools.network_tools.node_similarity` now re-export
  from `vdl_tools.causal_networks.metrics` with a `DeprecationWarning`; removed in 3.0.

### Removed
- `vdl_tools.network_tools.causal_network_metrics` (was unimportable; superseded by `causal_networks.metrics`).


### Added
- GitHub Actions CI workflow for automated testing
- GitHub Actions release workflow for automated PyPI publishing
- Dependabot configuration for automated dependency updates
- Pre-commit hooks configuration
- Hatch environments for testing, linting, and docs
- Enhanced Makefile with Hatch-based commands
- Comprehensive development tooling (ruff, black, mypy, pytest)

### Changed
- Improved `pyproject.toml` with optional dependencies and Hatch environments
- Enhanced build and publishing workflow using Hatch

## [1.0.1] - 2024-10-24

### Changed
- Previous release (details to be filled in from git history)

## [0.0.2] - Earlier

### Changed
- Initial development version

[Unreleased]: https://github.com/vibrant-data-labs/vdl-tools/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/vibrant-data-labs/vdl-tools/releases/tag/v0.0.5
[0.0.2]: https://github.com/vibrant-data-labs/vdl-tools/releases/tag/v0.0.2

