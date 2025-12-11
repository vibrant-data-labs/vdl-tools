#!/bin/bash
# Script to create a new release

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if version bump type is provided
BUMP_TYPE=$1

if [ -z "$BUMP_TYPE" ]; then
    echo -e "${RED}Usage: ./scripts/release.sh [patch|minor|major]${NC}"
    echo ""
    echo "Examples:"
    echo "  ./scripts/release.sh patch   # 0.0.5 -> 0.0.6"
    echo "  ./scripts/release.sh minor   # 0.0.5 -> 0.1.0"
    echo "  ./scripts/release.sh major   # 0.0.5 -> 1.0.0"
    exit 1
fi

if [[ ! "$BUMP_TYPE" =~ ^(patch|minor|major)$ ]]; then
    echo -e "${RED}Error: Version bump must be 'patch', 'minor', or 'major'${NC}"
    exit 1
fi

echo -e "${GREEN}🚀 Starting release process...${NC}"
echo ""

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${RED}Error: You have uncommitted changes. Please commit or stash them first.${NC}"
    git status --short
    exit 1
fi

# Check we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo -e "${YELLOW}⚠️  Warning: You're on branch '$CURRENT_BRANCH', not 'main'${NC}"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Get current version
CURRENT_VERSION=$(hatch version)
echo -e "Current version: ${YELLOW}$CURRENT_VERSION${NC}"

# Bump version
echo -e "${GREEN}Bumping $BUMP_TYPE version...${NC}"
hatch version $BUMP_TYPE
NEW_VERSION=$(hatch version)
echo -e "New version: ${GREEN}$NEW_VERSION${NC}"
echo ""

# Ask for confirmation
read -p "Proceed with release v$NEW_VERSION? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}Release cancelled. Reverting version...${NC}"
    hatch version $CURRENT_VERSION
    exit 1
fi

# Run tests
echo -e "${GREEN}Running tests...${NC}"
if ! make test; then
    echo -e "${RED}Tests failed! Reverting version...${NC}"
    hatch version $CURRENT_VERSION
    exit 1
fi
echo ""

# Check for linting issues
echo -e "${GREEN}Running linters...${NC}"
if ! make lint; then
    echo -e "${YELLOW}⚠️  Linting issues found. Continue anyway? (y/N): ${NC}"
    read -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        hatch version $CURRENT_VERSION
        exit 1
    fi
fi
echo ""

# Commit version bump
echo -e "${GREEN}Committing version bump...${NC}"
git add vdl_tools/__init__.py
git commit -m "chore: bump version to $NEW_VERSION"

# Create tag
echo -e "${GREEN}Creating tag v$NEW_VERSION...${NC}"
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"

# Push changes
echo -e "${GREEN}Pushing changes to origin...${NC}"
git push origin $CURRENT_BRANCH

echo -e "${GREEN}Pushing tag to origin...${NC}"
git push origin "v$NEW_VERSION"

echo ""
echo -e "${GREEN}✅ Release process complete!${NC}"
echo ""
echo "Next steps:"
echo "  1. Monitor GitHub Actions: https://github.com/vibrant-data-labs/vdl-tools/actions"
echo "  2. Check PyPI after publish: https://pypi.org/project/vdl-tools/"
echo "  3. Verify GitHub Release: https://github.com/vibrant-data-labs/vdl-tools/releases/tag/v$NEW_VERSION"
echo ""
echo -e "${YELLOW}Note: The release workflow will automatically publish to PyPI${NC}"

