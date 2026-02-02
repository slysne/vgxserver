# Manylinux Build Quick Reference

## Prerequisites

- Docker installed and running
- Git repository cloned
- For GitHub Actions: PYPI_API_TOKEN secret configured

## Local Development Builds

### Quick build for current platform
```bash
make build-local VERSION=3.6.0
# Or let it read from VERSION file:
make build-local
```

### Build manylinux wheels (x86_64)
```bash
make build-manylinux VERSION=3.6.0
# Or with VERSION file:
make build-manylinux

# Build and test:
./build-manylinux.sh 3.6.0 --test
```

### Build manylinux wheels (ARM64)
```bash
make build-manylinux-arm64 VERSION=3.6.0
./build-manylinux-aarch64.sh 3.6.0 --test
```

### Build all manylinux platforms
```bash
make build-all-manylinux VERSION=3.6.0
```

## Using cibuildwheel

### Install cibuildwheel
```bash
pip install cibuildwheel
```

### Build with Makefile (recommended)
```bash
# All Python versions, auto-detected architecture
make cibuildwheel VERSION=3.6.0

# Specific Python version (ARCH auto-detects from uname -m)
make cibuildwheel VERSION=3.6.0 PYVER=312

# Specific Python version and architecture
make cibuildwheel VERSION=3.6.0 PYVER=312 ARCH=x86_64
make cibuildwheel VERSION=3.6.0 PYVER=312 ARCH=aarch64

# All Python versions, specific architecture
make cibuildwheel VERSION=3.6.0 ARCH=x86_64
```

### Build directly with cibuildwheel
```bash
# Build all platforms
export PROJECT_VERSION=3.6.0
export CMAKE_PRESET=release
python -m cibuildwheel --output-dir wheelhouse

# Build specific architecture
export CIBW_ARCHS_LINUX="x86_64"
python -m cibuildwheel --platform linux --output-dir wheelhouse

# Build specific Python version
export CIBW_BUILD="cp311-*"
python -m cibuildwheel --output-dir wheelhouse
```

## Testing Wheels

### Test all wheels (auto-detects Python version from filename)
```bash
./test-wheels.py
# Or via Makefile:
make test
```

### Test specific wheelhouse
```bash
./test-wheels.py dist/
```

### What the test does
- Auto-detects Python version from wheel filename (cp312 → python:3.12-slim)
- Runs 6 comprehensive tests per wheel:
  1. Import pyvgx
  2. Version consistency
  3. vgxadmin command
  4. vgxadmin module
  5. vgxinstance module
  6. vgxdemoservice (start, verify, stop)
- Shows command output for debugging

### Manual test
```bash
docker run --rm -v $(pwd)/wheelhouse:/wheels python:3.11-slim bash -c \
  "pip install /wheels/pyvgx-*.whl && python -c 'import pyvgx; print(pyvgx)'"
```

## GitHub Actions (CI/CD)

### Trigger manual build
```bash
# Push to main/develop branch
git push origin main

# Or use workflow dispatch
gh workflow run build-wheels.yml
```

### Release and publish to PyPI
```bash
# Tag a release
git tag v3.6.0
git push origin v3.6.0

# Automatically builds and publishes to PyPI
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VERSION` | Package version | Read from VERSION file |
| `PROJECT_VERSION` | Package version (build) | `0.0.0.dev0` |
| `CMAKE_PRESET` | Build type | `release` |
| `PYVER` | Python version for cibuildwheel | `all` |
| `ARCH` | Architecture for cibuildwheel | Auto-detected via `uname -m` |
| `COMPILER_OPTION_MCPU` | CPU target (macOS) | `native` |
| `MACOSX_DEPLOYMENT_TARGET` | macOS minimum version | `11.0` |

## Common Issues

### "No matching distribution found"
- Wheel was not built for target platform/Python version
- Build more Python versions: `CIBW_BUILD="cp39-* cp310-* cp311-* cp312-*"`

### "ImportError: undefined symbol"
- Wheel needs repair with auditwheel
- Check that auditwheel ran successfully in build logs

### Build is very slow on ARM64
- Using QEMU emulation (expected)
- Use native ARM64 host or GitHub Actions

### CMake not found
- Install: `pip install cmake`
- Or in container: `yum install -y cmake3 && ln -s /usr/bin/cmake3 /usr/bin/cmake`

## File Structure

```
vgxserver/
├── .github/
│   └── workflows/
│       └── build-wheels.yml          # GitHub Actions workflow
├── build-manylinux.sh                # Local x86_64 build script
├── build-manylinux-aarch64.sh        # Local ARM64 build script
├── test-wheels.py                    # Wheel testing script (main)
├── test_wheel_validate.py            # Wheel validation script
├── pyproject.toml                    # Build configuration
├── setup.py                          # Build logic
├── Makefile                          # Common tasks
├── VERSION                           # Version file
└── docs/
    ├── MANYLINUX_BUILD.md            # Detailed documentation
    ├── MANYLINUX_QUICKREF.md         # This file
    └── MANYLINUX_SETUP.md            # Setup documentation
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

## References

- [Manylinux Documentation](docs/MANYLINUX_BUILD.md)
- [cibuildwheel docs](https://cibuildwheel.readthedocs.io/)
- [PEP 599 - manylinux2014](https://peps.python.org/pep-0599/)
