.PHONY: help install test test-all test-cov test-deps check-deps verify-dep-update lint format typing clean build version version-patch version-minor version-major release-patch release-minor release-major publish publish-test shell create-migration run-migration

help:  ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install package in development mode
	hatch env create

test:  ## Run tests with current Python
	hatch run test

test-all:  ## Run tests across all Python versions
	hatch run test:run

test-cov:  ## Run tests with coverage
	hatch run test-cov

test-deps:  ## Run dependency smoke tests only
	hatch run pytest tests/test_dependencies.py -v

check-deps:  ## Check for dependency conflicts and issues
	@echo "Checking for dependency conflicts..."
	@hatch run pip check || echo "⚠️  Dependency conflicts found!"
	@echo "\nChecking for outdated packages..."
	@hatch run pip list --outdated

verify-dep-update:  ## Verify dependency update (use: make verify-dep-update PACKAGE=pandas)
	@if [ -z "$(PACKAGE)" ]; then \
		echo "Usage: make verify-dep-update PACKAGE=<package-name>"; \
		echo "Example: make verify-dep-update PACKAGE=pandas"; \
		exit 1; \
	fi
	@./scripts/check_dependency_update.sh $(PACKAGE)

lint:  ## Run all linters
	hatch run lint:all

format:  ## Format code
	hatch run lint:fmt

typing:  ## Run type checking
	hatch run lint:typing

clean:  ## Clean build artifacts
	hatch clean
	rm -rf .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

build:  ## Build distribution packages
	hatch build

version:  ## Show current version
	@hatch version

version-patch:  ## Bump patch version (0.0.X)
	hatch version patch
	@echo "New version: $$(hatch version)"

version-minor:  ## Bump minor version (0.X.0)
	hatch version minor
	@echo "New version: $$(hatch version)"

version-major:  ## Bump major version (X.0.0)
	hatch version major
	@echo "New version: $$(hatch version)"

release-patch:  ## Create and release a patch version (tests, commits, tags, pushes)
	@./scripts/release.sh patch

release-minor:  ## Create and release a minor version (tests, commits, tags, pushes)
	@./scripts/release.sh minor

release-major:  ## Create and release a major version (tests, commits, tags, pushes)
	@./scripts/release.sh major

publish-test:  ## Publish to TestPyPI
	hatch publish -r test

publish:  ## Publish to PyPI
	hatch publish

shell:  ## Start a shell in the default environment
	hatch shell

# Database migrations
create-migration:  ## Create database migration (use: make create-migration COMMENT="message")
	@ alembic revision --autogenerate -m "$$COMMENT"

run-migration:  ## Run database migrations
	@ alembic upgrade head