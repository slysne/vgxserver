.PHONY: help clean build-local build-manylinux build-manylinux-arm64 test cibuildwheel

# Read version from VERSION file if not specified
VERSION_FILE := VERSION
VERSION ?= $(shell [ -f $(VERSION_FILE) ] && cat $(VERSION_FILE) || echo "0.0.0.dev0")

# Track if VERSION was explicitly set by user
VERSION_EXPLICIT := $(filter VERSION=%,$(MAKEFLAGS))

help:
	@echo "Available targets:"
	@echo "  clean                - Remove build artifacts"
	@echo "  build-local          - Build wheel locally (current platform)"
	@echo "  build-manylinux      - Build manylinux wheels (x86_64) using Docker"
	@echo "  build-manylinux-arm64 - Build manylinux wheels (aarch64) using Docker"
	@echo "  build-macos-arm64    - Build macOS ARM64 wheels (requires Apple Silicon)"
	@echo "  test                 - Test wheels in wheelhouse/"
	@echo "  cibuildwheel         - Build wheels using cibuildwheel (auto-detects platform)"
	@echo ""
	@echo "Environment variables:"
	@echo "  VERSION              - Package version (default: read from VERSION file or 0.0.0.dev0)"
	@echo "  CMAKE_PRESET         - Build type: release|debug|relWithDebInfo (default: release)"
	@echo "  PYVER                - Python version for cibuildwheel: 39|310|311|312|313|all (default: all)"
	@echo "  ARCH                 - Architecture: x86_64|aarch64|arm64|'x86_64 aarch64' (default: auto)"
	@echo "  CIBW_PLATFORM        - Platform override: linux|macos|windows (default: auto-detect)"

# Read version from VERSION file if not specified
VERSION_FILE := VERSION
VERSION ?= $(shell [ -f $(VERSION_FILE) ] && cat $(VERSION_FILE) || echo "0.0.0.dev0")

CMAKE_PRESET ?= release
PYTHON ?= python3
PYVER ?= all
ARCH ?= x86_64 aarch64

clean:
	rm -rf build/ dist/ wheelhouse/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.so" -delete
	find . -type f -name "*.dylib" -delete
	find . -type f -name "*.dll" -delete
	@echo "Cleaned build artifacts"

build-local:
	@echo "Building wheel for version $(VERSION)"
	export PROJECT_VERSION=$(VERSION) && \
	export CMAKE_PRESET=$(CMAKE_PRESET) && \
	$(PYTHON) -m build --wheel --outdir dist/
	@echo "Wheel built in dist/"

build-manylinux:
	@echo "Building manylinux wheels for version $(VERSION)"
	./build-manylinux.sh $(VERSION)
	@echo "Wheels built in wheelhouse/"

build-manylinux-arm64:
	@echo "Building manylinux ARM64 wheels for version $(VERSION)"
	./build-manylinux-aarch64.sh $(VERSION)
	@echo "Wheels built in wheelhouse/"

build-macos-arm64:
	@echo "Building macOS ARM64 wheels for version $(VERSION)"
	./build-macos-arm64.sh $(VERSION)
	@echo "Wheels built in wheelhouse/"

build-all-manylinux: build-manylinux build-manylinux-arm64
	@echo "All manylinux wheels built"

test:
	@if [ ! -d "wheelhouse" ]; then \
		echo "ERROR: wheelhouse/ directory not found. Build wheels first:"; \
		echo "  make build-manylinux"; \
		exit 1; \
	fi
	@if [ -z "$$(ls -A wheelhouse/*.whl 2>/dev/null)" ]; then \
		echo "ERROR: No wheels found in wheelhouse/. Build wheels first:"; \
		echo "  make build-manylinux"; \
		exit 1; \
	fi
	@echo "Testing wheels in wheelhouse/"
	@./test-wheels.sh wheelhouse || { \
		echo ""; \
		echo "Tests failed. Check output above for details."; \
		exit 1; \
	}

# Build using cibuildwheel (auto-detects platform or use CIBW_PLATFORM)
cibuildwheel:
	@echo "Building with cibuildwheel"
	@command -v cibuildwheel > /dev/null 2>&1 || { \
		echo "ERROR: cibuildwheel not found. Please install it:"; \
		echo "  pip install cibuildwheel"; \
		echo "  or: pip3 install cibuildwheel"; \
		exit 1; \
	}
	@# Prevent x86_64 → aarch64 cross-compilation (QEMU is unreliable)
	@if [ "$(ARCH)" = "aarch64" ] && [ "$$(uname -m)" = "x86_64" ]; then \
		echo "ERROR: Cross-compilation aarch64 on x86_64 is not supported (QEMU is unreliable)."; \
		echo ""; \
		echo "For aarch64 builds, use either:"; \
		echo "  1. Build on ARM64 hardware (native ARM64 Linux server)"; \
		echo "  2. GitHub Actions (uses native ARM64 runners)"; \
		echo ""; \
		exit 1; \
	fi
	@if [ "$(PYVER)" != "all" ]; then \
		export CIBW_BUILD="cp$(PYVER)-*"; \
	fi && \
	if [ -n "$(ARCH)" ]; then \
		export CIBW_ARCHS="$(ARCH)"; \
	fi && \
	if [ -n "$(VERSION_EXPLICIT)" ]; then \
		export CIBW_ENVIRONMENT="PROJECT_VERSION=$(VERSION) CMAKE_PRESET=$(CMAKE_PRESET)"; \
	else \
		export CIBW_ENVIRONMENT="CMAKE_PRESET=$(CMAKE_PRESET)"; \
	fi && \
	cibuildwheel --output-dir wheelhouse
	@echo "Wheels built in wheelhouse/"
