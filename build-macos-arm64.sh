#!/bin/bash
# macOS ARM64 wheel builder
# Usage: ./build-macos-arm64.sh <version> [python-versions] [--test]
# Example: ./build-macos-arm64.sh 3.6.0 "3.9 3.10 3.11 3.12 3.13"
# Example with test: ./build-macos-arm64.sh 3.6.0 "3.11" --test
# Note: Must run on macOS ARM64 (M1/M2/M3/M4)

set -e

VERSION=${1:-"0.0.0.dev0"}
PYTHON_VERSIONS=${2:-"3.9 3.10 3.11 3.12 3.13"}
RUN_TEST=false

# Check for --test flag
for arg in "$@"; do
  if [ "$arg" = "--test" ]; then
    RUN_TEST=true
  fi
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building macOS ARM64 wheels for version: ${VERSION}${NC}"
echo -e "${GREEN}Python versions: ${PYTHON_VERSIONS}${NC}"

# Verify we're on macOS ARM64
if [[ $(uname) != "Darwin" ]]; then
  echo -e "${RED}ERROR: This script must run on macOS${NC}"
  exit 1
fi

if [[ $(uname -m) != "arm64" ]]; then
  echo -e "${RED}ERROR: This script must run on ARM64 (Apple Silicon)${NC}"
  echo "Current architecture: $(uname -m)"
  exit 1
fi

# Verify current directory is project root
if [[ ! -f "setup.py" ]]; then
  echo -e "${RED}ERROR: setup.py not found in current directory.${NC}"
  echo "Run this script from the project root directory."
  exit 1
fi

# Create wheelhouse directory
mkdir -p wheelhouse

# Function to find Python binary
find_python() {
  local version=$1
  # Try common locations
  for path in \
    "/opt/homebrew/opt/python@${version}/bin/python${version}" \
    "/usr/local/bin/python${version}" \
    "$(which python${version} 2>/dev/null)" \
    "/Library/Frameworks/Python.framework/Versions/${version}/bin/python${version}"; do
    if [ -x "$path" ]; then
      echo "$path"
      return 0
    fi
  done
  return 1
}

# Build wheels for each Python version
for PYVER in ${PYTHON_VERSIONS}; do
  echo -e "${GREEN}=========================================${NC}"
  echo -e "${GREEN}Building wheel for Python ${PYVER}${NC}"
  echo -e "${GREEN}=========================================${NC}"

  # Find Python binary
  PYTHON_BIN=$(find_python ${PYVER})

  if [ -z "$PYTHON_BIN" ]; then
    echo -e "${YELLOW}WARNING: Python ${PYVER} not found. Skipping.${NC}"
    echo -e "${YELLOW}Install with: brew install python@${PYVER}${NC}"
    continue
  fi

  echo "Using Python: ${PYTHON_BIN}"
  ${PYTHON_BIN} --version

  # Install build dependencies
  ${PYTHON_BIN} -m pip install --upgrade pip
  ${PYTHON_BIN} -m pip install 'build>=1.0.0,<2.0' 'wheel>=0.42.0,<0.45' 'setuptools>=78,<80' 'delocate>=0.10.0'

  # Set environment variables
  export PROJECT_VERSION="${VERSION}"
  export CMAKE_PRESET=release

  # Build wheel
  ${PYTHON_BIN} -m build --wheel --outdir wheelhouse/

  echo -e "${GREEN}Wheel built for Python ${PYVER}${NC}"
done

# Repair wheels to ensure portability
echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}Repairing wheels with delocate${NC}"
echo -e "${GREEN}=========================================${NC}"

for whl in wheelhouse/*.whl; do
  if [[ -f "$whl" ]]; then
    echo "Repairing: $whl"
    delocate-wheel -v "$whl"
  fi
done

echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}All wheels built successfully${NC}"
echo -e "${GREEN}=========================================${NC}"
ls -lh wheelhouse/

# Run tests if requested
if [ "$RUN_TEST" = true ]; then
  echo ""
  echo -e "${GREEN}Running tests on built wheels...${NC}"
  ./test-wheels.sh wheelhouse
fi

echo ""
echo -e "${GREEN}macOS ARM64 wheels built successfully in wheelhouse/${NC}"
