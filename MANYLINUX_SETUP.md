# Manylinux Build System Setup

## Summary

Added comprehensive manylinux build support using pypa/manylinux Docker images and cibuildwheel for building portable Linux wheels.

## What was added

### 1. GitHub Actions Workflow
- **File**: `.github/workflows/build-wheels.yml`
- Automated wheel building for Linux:
  - x86_64 architecture
  - aarch64/ARM64 architecture
  - Uses manylinux2014 base image
- Python versions: 3.9, 3.10, 3.11, 3.12, 3.13
- Automatic PyPI publishing on tagged releases
- Wheel testing after build

### 2. Local Build Scripts
- **build-manylinux.sh**: Build x86_64 manylinux wheels using Docker
- **build-manylinux-aarch64.sh**: Build ARM64 manylinux wheels using Docker
- **test-wheels.py**: Test built wheels in clean Docker environments (Linux) or virtual environments (macOS)
- **test_wheel_validate.py**: Validation script that runs inside test environments
- All scripts are self-contained and include help text

### 3. Build Configuration
- **pyproject.toml**: Added `[tool.cibuildwheel]` configuration
  - Specifies Python versions to build
  - Platform-specific settings
  - Pre-build dependency installation
  - Test commands
- **Makefile**: Added convenience targets with CLI parameter control:
  - `make build-local VERSION=3.6.0` - Local wheel build
  - `make build-manylinux VERSION=3.6.0` - Manylinux x86_64 build
  - `make build-manylinux-arm64 VERSION=3.6.0` - Manylinux ARM64 build
  - `make test` - Run comprehensive tests on built wheels
  - `make cibuildwheel VERSION=3.6.0 PYVER=312` - Build with cibuildwheel (ARCH auto-detects)
  - `make cibuildwheel VERSION=3.6.0 PYVER=312 ARCH=x86_64` - Build specific architecture
  - `make clean` - Clean build artifacts
  - Auto-reads version from VERSION file if not specified
  - ARCH defaults to auto-detection via `uname -m`

### 4. Documentation
- **docs/MANYLINUX_BUILD.md**: Comprehensive guide covering:
  - Overview of manylinux system
  - Build process details
  - Configuration options
  - Troubleshooting
  - Publishing to PyPI
- **docs/MANYLINUX_QUICKREF.md**: Quick reference for developers
  - Common commands
  - Environment variables
  - Issue resolutions
- **README.md**: Added "Building from Source" section

### 5. Updated .gitignore
- Already contains `wheelhouse/` for manylinux build artifacts

## Usage

### For end users
No changes needed - wheels will be automatically published to PyPI on release.

### For developers (local builds)
```bash
# Set version in VERSION file (Maven-like)
echo "3.6.0" > VERSION

# Quick local build (reads VERSION file)
make build-local

# Or specify version explicitly
make build-local VERSION=3.6.0

# Manylinux build (requires Docker)
make build-manylinux

# Build and test in one command
./build-manylinux.sh 3.6.0 --test

# Test wheels separately
make test
```

### For CI/CD
Push a tag to trigger automatic builds and PyPI publishing:
```bash
git tag v3.6.0
git push origin v3.6.0
```

## Technical Details

### Version Management
- **VERSION file**: Single source of truth for version number (Maven-like)
- Auto-appends `.dev0+<timestamp>` for development builds
- Can be overridden with `PROJECT_VERSION` environment variable
- Example: `3.6.0` in VERSION → `3.6.0.dev0+1738281234` when building

### Manylinux Image
- Uses **manylinux2014** (PEP 599)
- Based on CentOS 7
- Compatible with most Linux distributions from ~2014+
- Requires glibc 2.17+

### Dependencies
Build dependencies are automatically installed in the manylinux container:
- clang - C compiler
- llvm-devel - LLVM development headers
- cmake - Build system
- Python build tools (pip, build, setuptools, wheel)

### Wheel Repair
Wheels are automatically repaired with `auditwheel`:
- Bundles required shared libraries into the wheel
- Adds proper manylinux platform tags
- Verifies ABI compatibility

### Wheel Naming Convention
```
pyvgx-3.6.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```
- Compatible with CPython 3.11
- Requires glibc 2.17 or higher
- x86_64 architecture

## Benefits

1. **Portability**: Wheels work on most Linux distributions without compilation
2. **Convenience**: Users can `pip install pyvgx` without build tools
3. **Consistency**: Same build environment for all builds
4. **Automation**: GitHub Actions handles building and publishing
5. **Testing**: Wheels are tested before release
6. **Multi-architecture**: Support for x86_64 and ARM64/aarch64

## Requirements for Contributors

- Docker (for local manylinux builds)
- Git (for version control)
- No special build tools needed on host system

## Future Improvements

Potential enhancements for future consideration:
- Add manylinux_2_28 support for newer systems
- Add support for musllinux (Alpine Linux)
- Create separate build profiles for different CPU targets
- Add benchmarking in CI/CD pipeline
- Support for cross-compilation

## References

- [PEP 599 - manylinux2014](https://peps.python.org/pep-0599/)
- [pypa/manylinux](https://github.com/pypa/manylinux)
- [cibuildwheel](https://cibuildwheel.readthedocs.io/)
- [auditwheel](https://github.com/pypa/auditwheel)
