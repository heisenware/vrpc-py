#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define some colors for clean CLI output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

TARGET_BRANCH="master"

echo -e "${YELLOW}Starting VRPC Distribution & Git Tagging Release Process...${NC}"

# 1. Ensure we are running inside the active virtual environment
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo -e "${RED}Error: Virtual environment not detected.${NC}"
    echo -e "Please activate your virtual environment (e.g., 'source vrpc-env/bin/activate') before running this script."
    exit 1
fi

# 2. Verify current Git branch matches target branch ('master')
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "$TARGET_BRANCH" ]]; then
    echo -e "${RED}Error: You are currently on branch '$CURRENT_BRANCH'.${NC}"
    echo -e "Releases must be executed explicitly from the '$TARGET_BRANCH' branch."
    exit 1
fi

# 3. Check for any uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    echo -e "${YELLOW}Warning: You have uncommitted changes in your working directory.${NC}"
    read -p "Do you want to proceed anyway? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Release aborted to safe-keep uncommitted changes.${NC}"
        exit 1
    fi
fi

# 4. Extract the version directly from pyproject.toml
VERSION=$(grep -E '^version[[:space:]]*=[[:space:]]*' pyproject.toml | head -n 1 | cut -d'"' -f2)
if [[ -z "$VERSION" ]]; then
    # Fallback if single quotes are used
    VERSION=$(grep -E '^version[[:space:]]*=[[:space:]]*' pyproject.toml | head -n 1 | cut -d"'" -f2)
fi

if [[ -z "$VERSION" ]]; then
    echo -e "${RED}Error: Could not parse version string from pyproject.toml.${NC}"
    exit 1
fi

TAG_NAME="v$VERSION"
echo -e "${GREEN}Detected version: ${VERSION} (Target Git Tag: ${TAG_NAME})${NC}"

# 5. Clean up old build artifacts to avoid token conflicts
echo -e "${YELLOW}Cleaning old distribution artifacts...${NC}"
rm -rf dist/ build/ *.egg-info

# 6. Ensure essential packaging tooling is fully up-to-date
echo -e "${YELLOW}Upgrading packaging tools...${NC}"
pip install --upgrade pip build twine

# 7. Compile the clean source distribution and wheel packages
echo -e "${YELLOW}Building binary wheel and source distribution...${NC}"
python -m build

# 8. Perform built-in package validation checks
echo -e "${YELLOW}Validating package metadata description...${NC}"
twine check dist/*

# 9. Execute final verification prompt before hitting the wire and tagging
echo -e "${GREEN}Build successful! Ready to publish and tag version ${TAG_NAME}.${NC}"
read -p "Are you absolutely ready to publish to PyPI and push the Git tag? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # A. Upload distributions to PyPI via Twine
    echo -e "${YELLOW}Uploading distributions to PyPI...${NC}"
    twine upload dist/*

    # B. Handle Git Tagging
    echo -e "${YELLOW}Creating local Git tag: ${TAG_NAME}...${NC}"
    # Delete tag locally first if it already exists to handle reruns cleanly
    if git rev-parse "$TAG_NAME" >/dev/null 2>&1; then
        echo -e "${YELLOW}Overwriting pre-existing local tag...${NC}"
        git tag -d "$TAG_NAME"
    fi
    git tag -a "$TAG_NAME" -m "Release version $VERSION"

    echo -e "${YELLOW}Pushing tag '${TAG_NAME}' to remote repository...${NC}"
    git push origin "$TARGET_BRANCH" --tags

    echo -e "${GREEN}🚀 Success! vrpc ${VERSION} has been published to PyPI, tagged, and pushed to ${TARGET_BRANCH}!${NC}"
else
    echo -e "${RED}Release aborted by user.${NC}"
    exit 0
fi
