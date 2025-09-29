#!/bin/bash

VERSION=3.6

OS_NAME="$(uname)"
if [[ "$OS_NAME" == "Darwin" ]]; then
    export MACOSX_DEPLOYMENT_TARGET=14.0
fi

set -e


# Defaults
TYPE="release"
TEST="none"

# Parse flags
while [[ $# -gt 0 ]]; do
    case "$1" in
        --type)
            TYPE="$2"
            if [[ "$TYPE" != "debug" && "$TYPE" != "release" ]]; then
                echo "Error: Invalid build type: '$TYPE'"
                echo "Allowed values: debug, release"
                exit 1
            fi
            shift 2
            ;;
        --test)
            TEST="$2"
            if [[ "$TEST" != "quick" && "$TEST" != "full" ]]; then
                echo "Error: Invalid test type: '$TEST'"
                echo "Allowed values: quick, full"
                exit 1
            fi
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 --type <debug|release> [--test <quick|most|all>]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Verify current directory is project root by checking for setup.py
if [[ ! -f "setup.py" ]]; then
  echo "ERROR: setup.py not found in current directory."
  echo "Run this script from the project root directory."
  exit 1
fi

pip uninstall pyvgx

echo "Cleanup done."


PRESET="${TYPE:-release}"



export PROJECT_VERSION=${VERSION}
export CMAKE_PRESET=${PRESET}
export BUILT_BY=`hostname`
export BUILD_NUMBER=1
export BUILD_DIR=../build-pyvgx

echo "Build directory: ${BUILD_DIR}"
mkdir -p ${BUILD_DIR}

SENTINEL=".tmp-pyvgx-build-dir"

safe_clear_dir() {
    local dir="$1"
    local sentinel="$2"

    if [ -n "$dir" ] &&
       [ "$dir" != "/" ] &&
       [ "$dir" != "." ] &&
       [ "$dir" != ".." ] &&
       [ -d "$dir" ] &&
       { [ -e "$dir/$sentinel" ] || [ -z "$(ls -A "$dir")" ]; }; then

        echo "Clearing contents of $dir"
        rm -rf "$dir"/* "$dir"/.[!.]* "$dir"/..?*
    else
        echo "Manually remove ${BUILD_DIR} first"
        return 1
    fi
}


safe_clear_dir ${BUILD_DIR} ${SENTINEL}

touch ${BUILD_DIR}/${SENTINEL}
cp -rp . ${BUILD_DIR}

pushd ${BUILD_DIR}

# Build
python -m build --wheel

# Find and install wheel
pushd dist
WHEEL=./*cp$(python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")*.whl
pip install $WHEEL
popd

python -c "from pyvgx import *; print( f'SUCCESS {version(1)}')"

# Test
if [[ "$TEST" == "quick" || "$TEST" == "full" ]]; then
    mkdir -p test
    cp -rp pyvgx/test/* test
    pushd test
    if [ "$TEST" == "quick" ]; then
        python test_pyvgx.py -x --quick=1
    else
        python test_pyvgx.py -x 
    fi
    popd
fi

popd
