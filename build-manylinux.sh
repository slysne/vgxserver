#!/bin/bash
# Local manylinux wheel builder using Docker
# Usage: ./build-manylinux.sh <version> [python-versions] [--test]
# Example: ./build-manylinux.sh 3.6.0 "cp39-cp39 cp310-cp310 cp311-cp311 cp312-cp312 cp313-cp313"
# Example with test: ./build-manylinux.sh 3.6.0 "cp311-cp311" --test

set -e

VERSION=${1:-"0.0.0.dev0"}
PYTHON_VERSIONS=${2:-"cp39-cp39 cp310-cp310 cp311-cp311 cp312-cp312 cp313-cp313"}
RUN_TEST=false

# Check for --test flag
for arg in "$@"; do
  if [ "$arg" = "--test" ]; then
    RUN_TEST=true
  fi
done

MANYLINUX_IMAGE="quay.io/pypa/manylinux2014_x86_64:latest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building manylinux wheels for version: ${VERSION}${NC}"
echo -e "${GREEN}Python versions: ${PYTHON_VERSIONS}${NC}"
echo -e "${GREEN}Manylinux image: ${MANYLINUX_IMAGE}${NC}"

# Verify current directory is project root
if [[ ! -f "setup.py" ]]; then
  echo -e "${RED}ERROR: setup.py not found in current directory.${NC}"
  echo "Run this script from the project root directory."
  exit 1
fi

# Create wheelhouse directory
mkdir -p wheelhouse

# Run docker to build wheels
docker run --rm \
  -v "$(pwd):/io" \
  -e PROJECT_VERSION="${VERSION}" \
  -e CMAKE_PRESET=release \
  -e PLAT=manylinux2014_x86_64 \
  ${MANYLINUX_IMAGE} \
  /bin/bash -c "
    set -e
    
    # Install dependencies
    yum install -y clang llvm-devel cmake3
    ln -sf /usr/bin/cmake3 /usr/bin/cmake
    
    # Build wheels for each Python version
    for PYBIN in ${PYTHON_VERSIONS}; do
      PYBIN_PATH=/opt/python/\${PYBIN}/bin
      if [ -d \"\${PYBIN_PATH}\" ]; then
        echo '========================================='
        echo \"Building wheel for \${PYBIN}\"
        echo '========================================='
        
        # Install build dependencies
        \${PYBIN_PATH}/pip install --upgrade pip
        \${PYBIN_PATH}/pip install 'build>=1.0.0,<2.0' 'wheel>=0.42.0,<0.45' 'setuptools>=78,<80' cmake
        
        # Build wheel
        cd /io
        \${PYBIN_PATH}/python -m build --wheel --outdir /io/wheelhouse/
        
        echo \"Wheel built for \${PYBIN}\"
      else
        echo \"Skipping \${PYBIN} (not found)\"
      fi
    done
    
    # Repair wheels to ensure manylinux compatibility
    for whl in /io/wheelhouse/*.whl; do
      if [[ \$whl != *manylinux* ]]; then
        echo \"Repairing \${whl}\"
        auditwheel repair \${whl} -w /io/wheelhouse/
        rm \${whl}
      fi
    done
    
    echo '========================================='
    echo 'All wheels built successfully'
    echo '========================================='
    ls -lh /io/wheelhouse/
  "

echo -e "${GREEN}Wheels built successfully in wheelhouse/${NC}"
ls -lh wheelhouse/

# Run tests if requested
if [ "$RUN_TEST" = true ]; then
  echo ""
  echo -e "${GREEN}Running tests on built wheels...${NC}"
  ./test-wheels.sh wheelhouse
fi
