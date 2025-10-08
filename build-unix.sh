#!/bin/bash

WHEEL_CONFIG_SETTINGS=""
OS_NAME="$(uname)"
if [[ "$OS_NAME" == "Darwin" ]]; then
    export MACOSX_DEPLOYMENT_TARGET=14.0
    WHEEL_CONFIG_SETTINGS="--config-settings=--plat-name=macosx_14_0_arm64"
fi

set -e


usage() {
    echo "Usage: $0 --version <x.y> --type <debug|release> [--mcpu <name>] [--test <quick|most|all>]"
    exit 0
}

 

# Defaults
VERSION=""
TYPE=""
MCPU=""
TEST="none"

# Parse flags
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            VERSION="$2"
            if [[ "$VERSION" =~ ^([0-9]+!)?([0-9]+)(\.[0-9]+){0,2}((a|b|rc)[0-9]+)?(\.post[0-9]+)?(\.dev[0-9]+)?$ ]]; then
                echo "Building version $VERSION"
            else
                echo "Error: Invalid build type: '$VERSION'"
                exit 1
            fi
            shift 2
            ;;
        --type)
            TYPE="$2"
            if [[ "$TYPE" != "debug" && "$TYPE" != "release" ]]; then
                echo "Error: Invalid build type: '$TYPE'"
                echo "Allowed values: debug, release"
                exit 1
            fi
            shift 2
            ;;
        --mcpu)
            MCPU="$2"
            export COMPILER_OPTION_MCPU="$MCPU"
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
            usage
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done


if [ -z "$VERSION" ] || [ -z "$TYPE" ]; then
    usage
fi


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

ABI_TAG=$(python -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')")
WHEEL_FILENAME=$(basename ./*$ABI_TAG*.whl)

if [[ "$OS_NAME" == "Darwin" ]]; then
    if command -v delocate-wheel >/dev/null 2>&1; then
        echo "Running delocate-wheel to fix macOS wheel tags..."
        delocate-wheel -w ./wheelhouse $WHEEL_FILENAME
        rm $WHEEL_FILENAME
        mv wheelhouse/$WHEEL_FILENAME .
        rmdir wheelhouse
    else
        echo "Warning: delocate-wheel not found. Skipping wheel tag fix."
        echo "To install: pip install delocate"
    fi
else
    if command -v auditwheel >/dev/null 2>&1; then
        echo "Running auditwheel to fix linux wheel tags..."
        auditwheel repair $WHEEL_FILENAME -w ./
        rm $WHEEL_FILENAME
        WHEEL_FILENAME=$(basename ./*$ABI_TAG*.whl)
    else
        echo "Warning: auditwheel not found. Skipping wheel tag fix."
        echo "To install: pip install auditwheel"
    fi
fi

pip install $WHEEL_FILENAME
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

