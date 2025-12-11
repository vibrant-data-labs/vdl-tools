#!/bin/bash
# Script to safely test dependency updates

set -e  # Exit on error

PACKAGE_NAME=$1

if [ -z "$PACKAGE_NAME" ]; then
    echo "Usage: ./scripts/check_dependency_update.sh <package-name>"
    echo "Example: ./scripts/check_dependency_update.sh pandas"
    exit 1
fi

echo "🔍 Checking dependency update for: $PACKAGE_NAME"
echo "================================================"

# Save current state
echo "📸 Saving current environment state..."
hatch run pip freeze > /tmp/deps-before.txt
OLD_VERSION=$(hatch run pip show "$PACKAGE_NAME" 2>/dev/null | grep "Version:" || echo "Not found")
echo "Current: $OLD_VERSION"

# Clean and recreate environment
echo ""
echo "🧹 Cleaning old environment..."
hatch env prune

echo "📦 Creating fresh environment with updated dependencies..."
hatch env create

# Check new version
NEW_VERSION=$(hatch run pip show "$PACKAGE_NAME" 2>/dev/null | grep "Version:" || echo "Not found")
echo "New: $NEW_VERSION"

# Check for conflicts
echo ""
echo "🔎 Checking for dependency conflicts..."
if hatch run pip check; then
    echo "✅ No conflicts detected"
else
    echo "❌ Dependency conflicts found!"
    exit 1
fi

# Show what changed
echo ""
echo "📊 Dependency changes:"
hatch run pip freeze > /tmp/deps-after.txt
diff /tmp/deps-before.txt /tmp/deps-after.txt | grep "^[<>]" | head -20 || echo "No changes or too many to display"

# Run dependency smoke tests
echo ""
echo "🧪 Running dependency smoke tests..."
if hatch run pytest tests/test_dependencies.py -v --tb=short; then
    echo "✅ Dependency tests passed"
else
    echo "❌ Dependency tests failed!"
    exit 1
fi

# Run full test suite (optional, comment out if too slow)
echo ""
echo "🧪 Running full test suite..."
if hatch run pytest tests/ -v --tb=short; then
    echo "✅ All tests passed"
else
    echo "⚠️  Some tests failed - review carefully"
    exit 1
fi

echo ""
echo "✅ Dependency update verification complete!"
echo "================================================"
echo "Next steps:"
echo "  1. Review the changes above"
echo "  2. Test your actual application code"
echo "  3. Commit the updated pyproject.toml"
echo "  4. Push and let CI run full tests"

