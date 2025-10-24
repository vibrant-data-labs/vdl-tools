# Contributing to VDL Tools

Thank you for your interest in contributing to VDL Tools! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [Hatch](https://hatch.pypa.io/) for environment and package management

### Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/vibrant-data-labs/vdl-tools.git
   cd vdl-tools
   ```

2. **Install Hatch:**
   ```bash
   pip install hatch
   ```

3. **Create development environment:**
   ```bash
   hatch env create
   ```

4. **Install pre-commit hooks (optional but recommended):**
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Development Workflow

### Running Tests

```bash
# Run tests with current Python version
make test

# Run tests across all Python versions (3.10, 3.11, 3.12)
make test-all

# Run tests with coverage report
make test-cov
```

### Code Quality

```bash
# Run all linters
make lint

# Format code automatically
make format

# Run type checking
make typing
```

### Building the Package

```bash
# Build distribution packages
make build

# Check current version
make version
```

## Making Changes

1. **Create a new branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and ensure:
   - Code is properly formatted (`make format`)
   - All tests pass (`make test`)
   - Linters pass (`make lint`)
   - New code has appropriate tests

3. **Commit your changes:**
   ```bash
   git add .
   git commit -m "feat: description of your changes"
   ```

   We follow [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `test:` for test changes
   - `refactor:` for code refactoring
   - `chore:` for maintenance tasks

4. **Push to your fork and create a Pull Request:**
   ```bash
   git push origin feature/your-feature-name
   ```

## Release Process

> **Note:** This section is for maintainers only.

1. Ensure all tests pass:
   ```bash
   make test-all
   ```

2. Update `CHANGELOG.md` with changes for the new version.

3. Bump the version:
   ```bash
   make version-patch  # for 0.0.X
   # or
   make version-minor  # for 0.X.0
   # or
   make version-major  # for X.0.0
   ```

4. Commit the version bump:
   ```bash
   git add vdl_tools/__init__.py CHANGELOG.md
   git commit -m "chore: bump version to $(hatch version)"
   ```

5. Create and push a tag:
   ```bash
   git tag -a "v$(hatch version)" -m "Release v$(hatch version)"
   git push origin main
   git push origin "v$(hatch version)"
   ```

6. GitHub Actions will automatically:
   - Run tests
   - Build the package
   - Publish to PyPI
   - Create a GitHub release

## Code Style

- **Line length:** 100 characters
- **Formatting:** Black (runs automatically with `make format`)
- **Linting:** Ruff (runs with `make lint`)
- **Type hints:** Encouraged but not required (checked with `make typing`)

## Testing Guidelines

- Write tests for new features and bug fixes
- Maintain or improve code coverage
- Tests should be in the `tests/` directory
- Use descriptive test names: `test_<function_name>_<scenario>_<expected_outcome>`

Example:
```python
def test_process_data_with_empty_input_returns_empty_list():
    result = process_data([])
    assert result == []
```

## Documentation

- Update docstrings for new functions and classes
- Follow [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- Update README.md if adding new major features
- Update CHANGELOG.md for all user-facing changes

## Getting Help

- Open an issue for bugs or feature requests
- Reach out to the maintainers for questions

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on collaboration and learning

Thank you for contributing to VDL Tools!

