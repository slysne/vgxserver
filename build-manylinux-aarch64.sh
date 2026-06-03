#!/bin/bash
# Local manylinux ARM64 wheel builder using Docker
# Usage: ./build-manylinux-aarch64.sh <version> [python-versions] [--test]
# Example: ./build-manylinux-aarch64.sh 3.6.0 "cp39-cp39 cp310-cp310 cp311-cp311"
# Example with test: ./build-manylinux-aarch64.sh 3.6.0 "cp311-cp311" --test
# Note: Requires ARM64 host or QEMU emulation (will be slow)

VERSION=${1:-"0.0.0.dev0"}
PYTHON_VERSIONS=${2:-"cp39-cp39 cp310-cp310 cp311-cp311 cp312-cp312 cp313-cp313"}
RUN_TEST=false

# Check for --test flag
for arg in "$@"; do
  if [ "$arg" = "--test" ]; then
    RUN_TEST=true
  fi
done

MANYLINUX_IMAGE="quay.io/pypa/manylinux2014_aarch64:latest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building manylinux ARM64 wheels for version: ${VERSION}${NC}"
echo -e "${YELLOW}Note: This will be slow on x86_64 hosts (requires QEMU)${NC}"

# Enable QEMU if on x86_64
if [[ $(uname -m) == "x86_64" ]]; then
  echo -e "${YELLOW}Enabling QEMU for ARM64 emulation...${NC}"
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
fi

# Verify current directory is project root
if [[ ! -f "setup.py" ]]; then
  echo -e "${RED}ERROR: setup.py not found in current directory.${NC}"
  echo "Run this script from the project root directory."
  exit 1
fi

# Create wheelhouse directory
mkdir -p wheelhouse

docker run --rm --platform linux/arm64 \
  -v "$(pwd):/io" \
  -e PROJECT_VERSION="${VERSION}" \
  -e CMAKE_PRESET=release \
  -e PLAT=manylinux2014_aarch64 \
  ${MANYLINUX_IMAGE} \
  /bin/bash -c "
    set -e
    yum install -y clang llvm-devel cmake3
    ln -sf /usr/bin/cmake3 /usr/bin/cmake
    
    for PYBIN in ${PYTHON_VERSIONS}; do
      PYBIN_PATH=/opt/python/\${PYBIN}/bin
      if [ -d \"\${PYBIN_PATH}\" ]; then
        echo \"Building wheel for \${PYBIN}\"
        \${PYBIN_PATH}/pip install --upgrade pip
        \${PYBIN_PATH}/pip install 'build>=1.0.0,<2.0' 'wheel>=0.42.0,<0.45' 'setuptools>=78,<80' cmake
        cd /io
        \${PYBIN_PATH}/python -m build --wheel --outdir /io/wheelhouse/
      fi
    done
    
    for whl in /io/wheelhouse/*.whl; do
      if [[ \$whl != *manylinux* ]]; then
        auditwheel repair \${whl} -w /io/wheelhouse/
        rm \${whl}
      fi
    done
    
    ls -lh /io/wheelhouse/
  "
ls -lh wheelhouse/

# Run tests if requested
if [ "$RUN_TEST" = true ]; then
  echo ""
  echo -e "${GREEN}Running tests on built wheels...${NC}"
  ./test-wheels.sh wheelhouse
fi

echo -e "${GREEN}ARM64 wheels built successfully in wheelhouse/${NC}"
