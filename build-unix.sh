#!/bin/bash

OS_NAME="$(uname)"
if [[ "$OS_NAME" == "Darwin" ]]; then
    export MACOSX_DEPLOYMENT_TARGET=14.0
fi

set -e

# Verify current directory is project root by checking for setup.py
if [[ ! -f "setup.py" ]]; then
  echo "ERROR: setup.py not found in current directory."
  echo "Please run this script from the project root directory."
  exit 1
fi

pip uninstall pyvgx

echo "Cleanup done."

PRESET="${1:-release}"
VERSION=3.6

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

cd ${BUILD_DIR}

python setup.py build_ext
python -m build --wheel

WHEEL=dist/*cp$(python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")*.whl

pip install $WHEEL

cd -









