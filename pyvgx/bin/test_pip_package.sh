#!/usr/bin/env bash

PYVGX_VERSION=$(python3 -c "import pyvgx;print(pyvgx.version(0))")
PIP_PACKAGE_VERSION=$(python -c 'import importlib.metadata; print(importlib.metadata.version("pyvgx"))')

[ "${PYVGX_VERSION}" == "pyvgx v${PIP_PACKAGE_VERSION}" ] || { echo "something wrong with pip package installation" && exit 1; }

vgxadmin --help || { echo "'vgxadmin' script not found" && exit 1; }
python3 -c "import vgxadmin" || { echo "'vgxadmin' module not found" && exit 1; }
python3 -c "import vgxinstance" || { echo "'vgxinstance' module not found" && exit 1; }

vgxdemoservice multi || { echo "Error starting 'vgxdemoservice'" && exit 1; }
INSTANCE_COUNT=$(vgxadmin 127.0.0.1:9001 --status '*' | grep S-IN | wc -l)

[ "${INSTANCE_COUNT}" == "6" ] || { echo "Error getting status from 'vgxdemoservice'" && exit 1; }
vgxdemoservice stop || { echo "Error stopping 'vgxdemoservice'" && exit 1; }