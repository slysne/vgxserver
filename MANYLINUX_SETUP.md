# Multi-Platform Build System Setup

## Summary

Added comprehensive build support using cibuildwheel for building portable wheels for all major platforms (Linux, macOS, Windows).

## What was added

### 1. GitHub Actions Workflow
- **File**: `.github/workflows/build-wheels.yml`
- Automated wheel building for all platforms:
  - **Linux**: x86_64, aarch64 (manylinux2014)
  - **macOS**: arm64 only - Apple Silicon/M1+
  - **Windows**: AMD64
- Python versions: 3.9, 3.10, 3.11, 3.12, 3.13
- Automatic PyPI publishing on tagged releases
- Comprehensive wheel testing after build (all platforms)

### 2. Testing Script
- **test_pip_package.py**: Cross-platform wheel testing script
  - Tests module imports, version consistency, and script availability
  - Automatically run by cibuildwheel after building each wheel
  - Can be run manually for local testing

### 3. Build Configuration
- **pyproject.toml**: Added `[tool.cibuildwheel]` configuration
  - Specifies Python versions to build (3.9-3.13)
  - Platform-specific settings (Linux, macOS, Windows)
  - Pre-build dependency installation
  - Test commands for all platforms

### 4. Documentation
- **MANYLINUX_BUILD.md**: Comprehensive multi-platform build guide
  - cibuildwheel usage and configuration
  - Build process details
  - Testing and troubleshooting
  - Publishing to PyPI
- **MANYLINUX_QUICKREF.md**: Quick reference for developers
  - Common cibuildwheel commands
  - Environment variables
  - Issue resolutions
- **WINDOWS_BUILD.md**: Windows-specific build instructions
  - Visual Studio Build Tools setup
  - cibuildwheel on Windows
  - Windows-specific troubleshooting
- **README.md**: Updated "Building from Source" section

### 5. Updated .gitignore
- Already contains `wheelhouse/` for manylinux build artifacts

## Usage

### For end users
No changes needed - wheels are automatically published to PyPI on release.

### For developers (local builds)
```bash
# Install cibuildwheel
pip install cibuildwheel

# Build wheels using Makefile
make cibuildwheel                                        # Auto-read VERSION file, all Python versions
make cibuildwheel VERSION=3.7.0                          # Explicit version, all Python versions
make cibuildwheel VERSION=3.7.0 PYVER=312               # Python 3.12 only
make cibuildwheel VERSION=3.7.0 PYVER=312 ARCH=x86_64   # Python 3.12, x86_64 only

# Test a built wheel manually
python -m venv test-env
source test-env/bin/activate  # On Windows: test-env\Scripts\activate
pip install wheelhouse/pyvgx-*.whl
python test_pip_package.py
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
