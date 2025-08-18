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

## Installing a new package

The package is intended to work with `pip`, however `pip` does not support adding package reference to `pyproject.toml`. So that the most reliable way to keep the dependencies in sync is to:

1. Add the package to `pyproject.toml` file, to the `[dependencies]` section
2. Run `pip install -e .` to install the package in editable mode, which will trigger the installation of the dependencies

## Version Management and Publishing

This project uses [Hatchling](https://hatch.pypa.io/latest/) as the build backend and follows semantic versioning.

### Bumping the Package Version

The version is stored in `vdl_tools/__init__.py` and managed through hatchling. To bump the version:

1. **Manual version update:**
   ```bash
   # Edit vdl_tools/__init__.py and update the __version__ string
   # For example, change from "0.0.2" to "0.0.3"
   ```

2. **Using hatch (recommended):**
   ```bash
   # Patch version bump (0.0.2 -> 0.0.3)
   hatch version patch
   
   # Minor version bump (0.0.2 -> 0.1.0)
   hatch version minor
   
   # Major version bump (0.0.2 -> 1.0.0)
   hatch version major
   
   # Set specific version
   hatch version "0.1.0"
   ```

### Building the Package

To build the distribution packages (wheel and source distribution):

```bash
# Install hatch if not already installed
pip install hatch

# Clean any previous builds
rm -rf dist/

# Build the package
hatch build
```

This will create:
- `dist/vdl_tools-<version>-py3-none-any.whl` (wheel distribution)
- `dist/vdl-tools-<version>.tar.gz` (source distribution)

### Publishing to PyPI

#### Prerequisites

1. **Install publishing tools:**
   ```bash
   pip install twine
   ```

2. **Configure PyPI credentials:**
   
   Option A - Using `.pypirc` file:
   ```bash
   # Create ~/.pypirc with your credentials
   cat > ~/.pypirc << EOF
   [distutils]
   index-servers = pypi
   
   [pypi]
   username = __token__
   password = <your-pypi-api-token>
   EOF
   ```
   
   Option B - Using environment variables:
   ```bash
   export TWINE_USERNAME=__token__
   export TWINE_PASSWORD=<your-pypi-api-token>
   ```

#### Publishing Process

1. **Test on PyPI Test (recommended first step):**
   ```bash
   # Upload to test PyPI first
   twine upload --repository-url https://test.pypi.org/legacy/ dist/*
   
   # Test installation from test PyPI
   pip install --index-url https://test.pypi.org/simple/ vdl-tools
   ```

2. **Publish to production PyPI:**
   ```bash
   # Upload to production PyPI
   twine upload dist/*
   ```

#### Complete Release Workflow

Here's the complete workflow for releasing a new version:

```bash
# 1. Ensure you're on the main branch and up to date
git checkout main
git pull origin main

# 2. Bump the version
hatch version patch  # or minor/major as appropriate

# 3. Commit the version bump
git add vdl_tools/__init__.py
git commit -m "Bump version to $(hatch version)"

# 4. Create a git tag
git tag "v$(hatch version)"

# 5. Build the package
rm -rf dist/
hatch build

# 6. Upload to test PyPI (optional but recommended)
twine upload --repository-url https://test.pypi.org/legacy/ dist/*

# 7. Upload to production PyPI
twine upload dist/*

# 8. Push changes and tags to GitHub
git push origin main
git push origin --tags
```

### Verification

After publishing, verify the package is available:

```bash
# Check on PyPI
pip install vdl-tools==<new-version>

# Or upgrade existing installation
pip install --upgrade vdl-tools
```
