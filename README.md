# VDL Tools

This repository contains a set of tools which are used at Vibrant Data Labs.

## Tools

- [Network Tools](./vdl_tools/network-tools/)
- [LinkedIn](./vdl_tools/LinkedIn/) - Set of methods to interact with LinkedIn via CoreSignal API
- [Download Process Images](./vdl_tools/download_process_images) - Helper methods to download, convert and save images to S3 bucket
- [Py2Mappr](./vdl_tools/py2mappr) - Python wrapper for generating the OpenMappr player
- [Scrape Enrich](./vdl_tools/scrape_enrich/)
- [Tag2Network](./vdl_tools/tag2network/)
- [Shared Tools](./vdl_tools/shared_tools/)

## Installation

### For Users

Install the package from PyPI:

```bash
pip install vdl-tools
```

### For Developers

1. Clone the repository:
```bash
git clone https://github.com/vibrant-data-labs/vdl-tools.git
cd vdl-tools
```

2. Install [Hatch](https://hatch.pypa.io/):
```bash
pip install hatch
```

3. Create development environment:
```bash
hatch env create
```

4. (Optional) Install pre-commit hooks:
```bash
pip install pre-commit
pre-commit install
```

## Development

This project uses [Hatch](https://hatch.pypa.io/) for environment management, building, and publishing.

### Quick Commands

```bash
# Run tests
make test

# Run tests across all Python versions (3.10, 3.11, 3.12)
make test-all

# Run tests with coverage
make test-cov

# Run linters
make lint

# Format code
make format

# Build package
make build

# Show all available commands
make help
```

### Using Hatch Directly

```bash
# Enter development shell
hatch shell

# Run tests
hatch run test

# Run linters
hatch run lint:all

# Format code
hatch run lint:fmt

# Build package
hatch build

# Bump version
hatch version patch  # or minor/major

# Publish to PyPI
hatch publish
```

## Adding a New Dependency

1. Add the package to `pyproject.toml` in the `[project.dependencies]` section
2. Update your environment:
```bash
hatch env prune  # Remove old environment
hatch env create  # Create new environment with updated dependencies
```

Or if using pip directly:
```bash
pip install -e .
```

## Contributing

Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed guidelines on:
- Development setup
- Code style and quality standards
- Testing requirements
- Pull request process
- Release workflow

## CI/CD

This project uses GitHub Actions for continuous integration and deployment:

- **CI**: Runs tests, linters, and builds on every push and PR
- **Dependabot**: Automatically updates dependencies weekly
- **Release**: Automatically publishes to PyPI when a version tag is pushed

## License

See LICENSE file for details.
