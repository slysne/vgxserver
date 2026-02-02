# Manylinux Build Setup

This directory contains configuration and scripts for building manylinux-compatible Python wheels using pypa/manylinux Docker images.

## Overview

The project uses two approaches for building manylinux wheels for Linux:

1. **GitHub Actions (CI/CD)** - Automated builds via `.github/workflows/build-wheels.yml`
2. **Local Docker builds** - Manual builds using `build-manylinux.sh` scripts

Both x86_64 and ARM64/aarch64 architectures are supported.

## Quick Start

### Local Build (x86_64)

```bash
# Build wheels for all supported Python versions
./build-manylinux.sh 3.6.0

# Build for specific Python versions
./build-manylinux.sh 3.6.0 "cp311-cp311 cp312-cp312"

# Build and test in one command
./build-manylinux.sh 3.6.0 "cp311-cp311 cp312-cp312" --test
```

### Local Build (ARM64/aarch64)

```bash
# Requires ARM64 host or QEMU emulation
./build-manylinux-aarch64.sh 3.6.0
```

### Using cibuildwheel

```bash
# Install cibuildwheel
pip install cibuildwheel

# Build wheels (uses pyproject.toml configuration)
# Version is read from VERSION file by default, or set explicitly:
export PROJECT_VERSION=3.6.0
export CMAKE_PRESET=release
python -m cibuildwheel --platform linux --output-dir wheelhouse

# Using Makefile for easier syntax (ARCH auto-detects from uname -m):
make cibuildwheel VERSION=3.6.0 PYVER=312
make cibuildwheel VERSION=3.6.0 PYVER=312 ARCH=x86_64
```

## Configuration

### pyproject.toml

The `[tool.cibuildwheel]` section configures:
- Python versions to build (3.9-3.13)
- Platform-specific settings
- Pre-build dependencies
- Test commands

### GitHub Actions

The workflow `.github/workflows/build-wheels.yml`:
- Builds wheels for Linux x86_64 and aarch64
- Automatically publishes to PyPI on tagged releases
- Runs tests on built wheels

## Manylinux Images

This project uses **manylinux2014** which provides:
- GCC 10+ compiler toolchain
- Compatible with most Linux distributions from ~2014+
- RHEL/CentOS 7 base

### Available Images:
- `manylinux2014_x86_64` - Intel/AMD 64-bit
- `manylinux2014_aarch64` - ARM 64-bit
- `manylinux2014_i686` - Intel/AMD 32-bit (not used)

## Dependencies

The build requires:
- **clang** - C compiler
- **llvm-devel** - LLVM development headers
- **cmake** - Build system generator
- **Python development headers** - Included in manylinux images

These are automatically installed by the `before-build` scripts.

## Build Process

1. **Docker container starts** with manylinux image
2. **Install build dependencies** (clang, llvm, cmake)
3. **For each Python version:**
   - Install Python build tools (pip, build, setuptools, wheel)
   - Run `python -m build --wheel` which triggers:
     - setup.py execution
     - CMake configuration
     - C extension compilation
     - Wheel packaging
4. **Repair wheels** with `auditwheel`:
   - Bundles required shared libraries
   - Adds manylinux platform tags
   - Verifies ABI compatibility
5. **Output wheels** to `wheelhouse/` directory

## Testing Wheels

Test built wheels using the test script:

```bash
# Test all wheels in wheelhouse/
./test-wheels.py

# Test wheels in a specific directory
./test-wheels.py dist/

# Or via Makefile
make test
```

The test script:
- **Auto-detects Python version** from wheel filename (e.g., cp312 → python:3.12-slim)
- Runs 6 comprehensive tests per wheel:
  1. Import pyvgx module
  2. Version consistency check
  3. vgxadmin CLI command availability
  4. vgxadmin module import
  5. vgxinstance module import
  6. vgxdemoservice functionality (start, verify 6 instances, stop)
- Tests in isolated Docker containers (Linux) or virtual environments (macOS)
- Shows command output for debugging

## Wheel Naming

Built wheels follow PEP 427 naming:
```
pyvgx-3.6.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
└─────┘ └───┘ └──────────┘ └──────────────────────────────────────────┘
  name  version   python          platform tags
```

- `cp311-cp311` - CPython 3.11 with stable ABI
- `manylinux_2_17` - Requires glibc 2.17+
- `manylinux2014` - Compatible with CentOS 7+

## Troubleshooting

### Build fails with "clang: not found"

The before-build script should install clang automatically. If it fails:
```bash
docker run -it --rm quay.io/pypa/manylinux2014_x86_64 bash
yum install -y clang llvm-devel
```

### Wheel is not manylinux compatible

Check `auditwheel` output:
```bash
auditwheel show wheelhouse/your-wheel.whl
```

Look for external shared library dependencies that need bundling.

### ARM64 build is very slow

This is expected when building ARM64 wheels on x86_64 hosts (QEMU emulation). Consider:
- Using a native ARM64 host
- Using GitHub Actions (free ARM64 runners)
- Building overnight

### CMake configuration fails

Check that:
- Python executable is found correctly
- CMAKE_PRESET environment variable is set
- PROJECT_VERSION environment variable is set

## Publishing to PyPI

### Manual publish:
```bash
pip install twine
twine upload wheelhouse/*.whl
```

### Automatic publish:
Tagged releases automatically publish via GitHub Actions:
```bash
git tag v3.6.0
git push origin v3.6.0
```

Requires `PYPI_API_TOKEN` secret in GitHub repository settings.

## Additional Resources

- [manylinux GitHub](https://github.com/pypa/manylinux)
- [cibuildwheel documentation](https://cibuildwheel.readthedocs.io/)
- [PEP 599 - manylinux2014](https://peps.python.org/pep-0599/)
- [auditwheel documentation](https://github.com/pypa/auditwheel)
