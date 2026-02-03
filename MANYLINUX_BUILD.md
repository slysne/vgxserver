# Multi-Platform Build Setup

This directory contains configuration and scripts for building portable Python wheels for all major platforms using cibuildwheel.

## Overview

The project uses cibuildwheel for building wheels across all platforms:

1. **GitHub Actions (CI/CD)** - Automated builds via `.github/workflows/build-wheels.yml` for all platforms
2. **Local builds** - Manual builds using cibuildwheel

**Supported Platforms:**
- **Linux**: x86_64, aarch64 (manylinux2014)
- **macOS**: arm64 only - Apple Silicon/M1+ (macOS 11.0+)
- **Windows**: AMD64

## Quick Start

### Using cibuildwheel

```bash
# Install cibuildwheel
pip install cibuildwheel

# Build wheels using Makefile
make cibuildwheel                                        # Auto-read VERSION file, all Python versions
make cibuildwheel VERSION=3.7.0                          # Explicit version, all Python versions
make cibuildwheel VERSION=3.7.0 PYVER=312               # Python 3.12 only
make cibuildwheel VERSION=3.7.0 PYVER=312 ARCH=x86_64   # Python 3.12, x86_64 only
make cibuildwheel VERSION=3.7.0 ARCH=aarch64            # Specific architecture
```

Wheels will be created in the `wheelhouse/` directory.

## Configuration

### pyproject.toml

The `[tool.cibuildwheel]` section configures:
- Python versions to build (3.9-3.13)
- Platform-specific settings (Linux, macOS, Windows)
- Pre-build dependencies
- Test commands

**Note on Linux architectures:** The `archs` setting under `[tool.cibuildwheel.linux]` is commented out to allow dynamic 
architecture selection via the Makefile's `ARCH` parameter. This enables building for specific architectures without modifying the configuration file:
- Use `ARCH=x86_64` for Intel/AMD 64-bit
- Use `ARCH=aarch64` for ARM 64-bit

### GitHub Actions

The workflow `.github/workflows/build-wheels.yml`:
- Builds wheels for all platforms:
  - Linux: x86_64, aarch64
  - macOS: arm64 only (Apple Silicon/M1+)
  - Windows: AMD64
- Automatically publishes to PyPI on tagged releases
- Runs tests on all built wheels

## Build Environments

### Linux (Manylinux Images)

This project uses **manylinux2014** which provides:
- GCC 10+ compiler toolchain
- Compatible with most Linux distributions from ~2014+
- RHEL/CentOS 7 base

Available images:
- `manylinux2014_x86_64` - Intel/AMD 64-bit
- `manylinux2014_aarch64` - ARM 64-bit

### macOS

- Minimum deployment target: macOS 11.0 (Big Sur)
- Target: arm64 only (Apple Silicon/M1+)
- Uses delocate for wheel repair

### Windows

- Target: Windows AMD64
- Uses MSVC compiler toolchain
- No special repair needed (native DLL handling)

## Dependencies

The build requires:
- **clang** - C compiler
- **llvm-devel** - LLVM development headers
- **cmake** - Build system generator
- **Python development headers** - Included in manylinux images

These are automatically installed by the `before-build` scripts.

## Build Process

cibuildwheel orchestrates the build process:

1. **Docker container starts** with manylinux image (Linux) or native environment (macOS/Windows)
2. **Install build dependencies** (clang, llvm, cmake on Linux; platform-specific tools on macOS/Windows)
3. **For each Python version:**
   - Install Python build tools (pip, build, setuptools, wheel)
   - Run setup.py to build the wheel:
     - CMake configuration with appropriate compiler and flags
     - C extension compilation
     - Wheel packaging
4. **Repair wheels**:
   - Linux: `auditwheel` bundles shared libraries and adds manylinux platform tags
   - macOS: `delocate` bundles dylib dependencies
   - Windows: Native DLL handling, no repair needed
5. **Run tests** using `test_pip_package.py` in clean environments
6. **Output wheels** to `wheelhouse/` directory

## Testing Wheels

cibuildwheel automatically runs tests after building each wheel using the test script configured in `pyproject.toml`.

### Manual Testing

To manually test a built wheel:

```bash
# Create a test environment
python -m venv test-env
source test-env/bin/activate  # On Windows: test-env\Scripts\activate

# Install the wheel
pip install wheelhouse/pyvgx-*.whl

# Run tests
python test_pip_package.py
```

The test script runs 4 comprehensive tests:
1. Module version check (pyvgx module version matches package version)
2. Script availability (vgxadmin command is available in PATH)
3. Module imports (vgxadmin, vgxinstance modules import successfully)
4. vgxdemoservice functionality (Linux/macOS x86_64/amd64 only - automatically skipped on Windows and ARM architectures)

## Wheel Naming

Built wheels follow PEP 427 naming:

**Release build (explicit VERSION):**
```
pyvgx-3.6.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
└─────┘ └───┘ └──────────┘ └──────────────────────────────────────────┘
  name  version   python          platform tags
```

**Development build (auto-read VERSION file):**
```
pyvgx-3.7.0.dev0+1770090416-cp312-cp312-manylinux_2_17_aarch64.manylinux2014_aarch64.whl
└─────┘ └─────────────────┘ └──────────┘ └────────────────────────────────────────────────┘
  name   version+timestamp    python               platform tags
```

**Version format:**
- Release: `3.6.0` - Set explicitly via `VERSION=3.6.0`
- Development: `3.7.0.dev0+1770090416` - Auto-read from VERSION file (3.7.0) + dev timestamp

**Python tags:**
- `cp311-cp311` - CPython 3.11 with stable ABI
- `cp312-cp312` - CPython 3.12 with stable ABI

**Platform tags:**
- `manylinux_2_17` - Requires glibc 2.17+
- `manylinux2014` - Compatible with CentOS 7+

## Troubleshooting

### Build fails with "clang: not found" (Linux)

The before-build script in `pyproject.toml` should install clang automatically. If it fails, check that the manylinux image has yum access to install packages. cibuildwheel handles this automatically.

### Wheel is not manylinux compatible (Linux)

cibuildwheel automatically runs `auditwheel repair` to ensure compatibility. If you encounter issues, check the build logs for external shared library dependencies that couldn't be bundled.

### ARM64 build is slow

This is expected when building ARM64 wheels on x86_64 hosts (QEMU emulation). Consider:
- Using a native ARM64 host
- Using GitHub Actions (ubuntu-24.04-arm64 runners)
- Reducing the number of Python versions built

### CMake configuration fails

Check that:
- Python development headers are available (automatically installed in cibuildwheel environments)
- CMAKE_PRESET environment variable is set (default: release)
- PROJECT_VERSION environment variable is set (default: reads from VERSION file + dev timestamp)

### Visual Studio not found (Windows)

Ensure Visual Studio Build Tools 2022 with C++ workload is installed. See [WINDOWS_BUILD.md](WINDOWS_BUILD.md) for detailed instructions.

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
