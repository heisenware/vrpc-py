#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define some colors for clean CLI output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Starting VRPC Python Distribution Release Process...${NC}"

# 1. Ensure we are running inside the active virtual environment
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo -e "${RED}Error: Virtual environment not detected.${NC}"
    echo -e "Please activate your virtual environment (e.g., 'source vrpc-env/bin/activate') before running this script."
    exit 1
fi

# 2. Clean up old build artifacts to avoid token/403 multi-package conflicts
echo -e "${YELLOW}Cleaning old distribution artifacts...${NC}"
rm -rf dist/ build/ *.egg-info

# 3. Ensure essential packaging tooling is fully up-to-date
echo -e "${YELLOW}Upgrading packaging tools...${NC}"
pip install --upgrade pip build twine

# 4. Compile the clean source distribution and wheel packages
echo -e "${YELLOW}Building binary wheel and source distribution...${NC}"
python -m build

# 5. Perform built-in package validation checks
echo -e "${YELLOW}Validating package metadata description...${NC}"
twine check dist/*

# 6. Execute final verification prompt before hitting the wire
echo -e "${GREEN}Build successful! The following packages are prepared for PyPI:${NC}"
ls -l dist/
echo ""
read -p "Are you absolutely ready to publish this release to PyPI? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Uploading distributions to PyPI via Twine...${NC}"
    # This triggers the standard secure API token prompt cleanly
    twine upload dist/*
    echo -e "${GREEN}🚀 Success! vrpc has been successfully updated on PyPI.${NC}"
else
    echo -e "${RED}Release aborted by user.${NC}"
    exit 0
fi
