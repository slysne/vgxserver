# Manylinux Build Quick Reference

## Prerequisites

- Git repository cloned
- Python 3.9+ installed
- For GitHub Actions: PYPI_API_TOKEN secret configured

## Building Wheels with cibuildwheel

### Install cibuildwheel
```bash
pip install cibuildwheel
```

### Build commands
```bash
# Auto-read VERSION file, all Python versions
make cibuildwheel

# Explicit version, all Python versions
make cibuildwheel VERSION=3.7.0

# Specific Python version (39, 310, 311, 312, 313, or all)
make cibuildwheel VERSION=3.7.0 PYVER=312

# Specific Python version and architecture
make cibuildwheel VERSION=3.7.0 PYVER=312 ARCH=x86_64

# Specific architecture (x86_64, aarch64 for Linux, arm64 for macOS)
make cibuildwheel VERSION=3.7.0 ARCH=aarch64
```

Wheels will be created in the `wheelhouse/` directory.

### Supported Platforms
- **Linux**: x86_64, aarch64 (manylinux2014)
- **macOS**: arm64 only - Apple Silicon/M1+ (macOS 11.0+)
- **Windows**: AMD64

## Testing Wheels

cibuildwheel automatically runs tests after building each wheel. To manually test a wheel:

```bash
# Create a test environment
python -m venv test-env
source test-env/bin/activate  # On Windows: test-env\Scripts\activate

# Install the wheel
pip install wheelhouse/pyvgx-*.whl

# Run tests
python test_pip_package.py
```

### What the test does
Runs 4 comprehensive tests per wheel:
1. Module version check (pyvgx module version matches package version)
2. Script availability (vgxadmin command is available in PATH)
3. Module imports (vgxadmin, vgxinstance modules import successfully)
4. vgxdemoservice functionality (Linux/macOS x86_64/amd64 only - automatically skipped on Windows and ARM)

## GitHub Actions (CI/CD)

### Automatic builds
GitHub Actions automatically builds wheels for all platforms on:
- Push to main branch
- Pull requests
- Tagged releases (v*)

### Manual workflow dispatch
```bash
# Trigger a manual build
gh workflow run build-wheels.yml

# With custom parameters
gh workflow run build-wheels.yml \
  -f version="3.6.0" \
  -f python_versions="cp311-* cp312-*" \
  -f architectures="x86_64"
```

### Release and publish to PyPI
```bash
# Tag a release
git tag v3.6.0
git push origin v3.6.0

# Automatically builds and publishes to PyPI (requires PYPI_API_TOKEN secret)
```

## Build Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `VERSION` | Package version | Read from VERSION file + `.dev0+<timestamp>` |
| `PYVER` | Python version: 39\|310\|311\|312\|313\|all | `all` |
| `ARCH` | Architecture: x86_64\|aarch64\|arm64 | Auto-detected from `uname -m` |
| `CMAKE_PRESET` | Build type: release\|debug\|relWithDebInfo | `release` |

**Note:** When VERSION is auto-detected, it appends a development timestamp (e.g., `3.7.0.dev0+1770090416`) for unique identification.

## Common Issues

### "No matching distribution found"
- Wheel was not built for your platform/Python version
- Check which wheels were built in `wheelhouse/` directory
- Build for your specific Python version: `export CIBW_BUILD="cp312-*"`

### "ImportError: undefined symbol" (Linux)
- cibuildwheel automatically runs `auditwheel repair` to bundle shared libraries
- Check build logs for auditwheel warnings

### Build is very slow on ARM64 (Linux)
- Using QEMU emulation on x86_64 hosts (expected)
- Use native ARM64 host or GitHub Actions ubuntu-24.04-arm64 runner

### Visual Studio not found (Windows)
- Install Visual Studio Build Tools 2022 with C++ workload
- See [WINDOWS_BUILD.md](WINDOWS_BUILD.md) for detailed instructions

### Tests failing
- cibuildwheel automatically runs `test_pip_package.py` after each build
- Check test logs for specific failures
- vgxdemoservice test is automatically skipped on Windows and ARM architectures

## File Structure

```
vgxserver/
├── .github/
│   └── workflows/
│       └── build-wheels.yml          # GitHub Actions CI/CD workflow
├── test_pip_package.py               # Cross-platform wheel testing script
├── pyproject.toml                    # Build configuration (cibuildwheel settings)
├── setup.py                          # Build logic (CMake integration)
├── VERSION                           # Version file
├── MANYLINUX_BUILD.md                # Detailed build documentation
├── MANYLINUX_QUICKREF.md             # This file (quick reference)
├── MANYLINUX_SETUP.md                # Setup documentation
└── WINDOWS_BUILD.md                  # Windows-specific build guide
```

## Useful Commands

```bash
# Check wheel contents
unzip -l wheelhouse/pyvgx-*.whl

# Check wheel tags
pip debug --verbose wheelhouse/pyvgx-*.whl

# Inspect wheel dependencies (Linux)
auditwheel show wheelhouse/pyvgx-*.whl

# Upload to PyPI
twine upload wheelhouse/*.whl

# Upload to Test PyPI
twine upload --repository testpypi wheelhouse/*.whl
```

## Additional Documentation

- [MANYLINUX_BUILD.md](MANYLINUX_BUILD.md) - Detailed multi-platform build guide
- [WINDOWS_BUILD.md](WINDOWS_BUILD.md) - Windows-specific build instructions
- [MANYLINUX_SETUP.md](MANYLINUX_SETUP.md) - Development environment setup
- [cibuildwheel documentation](https://cibuildwheel.readthedocs.io/)
- [PEP 599 - manylinux2014](https://peps.python.org/pep-0599/)
