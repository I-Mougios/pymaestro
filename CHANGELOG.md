# Changelog

## [0.2.0] – 2025-12-30

### Added
- `inject_dependencies` decorator for improved dependency injection in jobs.  
- Example-specific requirements file: `docs/01_example/requirements.txt`.

### Changed
- GitHub Actions release workflow now uploads releases to **PyPI** instead of TestPyPI.  
- Updated README installation instructions to reference **PyPI**.  
- Refactored project structure: moved `serialize` and `deserialize` logic from `jobs.py` to a proper `utils` package. This improves modularity and removes unnecessary dependencies from `jobs.py`.

### Fixed
- Tests for job dependency resolution now correctly handle warnings when a job is executed multiple times.
