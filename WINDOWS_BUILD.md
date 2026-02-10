# Windows Build Guide

This guide covers building the pyvgx Python package on Windows.

## Prerequisites

### 1. Visual Studio Build Tools 2022

The build requires Visual Studio Build Tools 2022 with C++ workload. This provides the MSVC compiler and tools needed by CMake.

#### Option A: Install via winget (Recommended)

Open PowerShell as Administrator and run:

```powershell
winget install Microsoft.VisualStudio.2022.BuildTools --force --override "--wait --quiet --add Microsoft.VisualStudio.Workload.VCTools --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 --includeRecommended"
```

This installs:
- Visual Studio Build Tools 2022
- C++ build tools workload
- MSVC v143 compiler and libraries
- Windows SDK
- CMake and other recommended components

#### Option B: Manual Installation

1. Download the installer: https://aka.ms/vs/17/release/vs_BuildTools.exe
2. Run the installer
3. Select **"Desktop development with C++"** workload
4. Ensure these components are checked:
   - MSVC v143 - VS 2022 C++ x64/x86 build tools
   - Windows 11 SDK (or Windows 10 SDK)
   - C++ CMake tools for Windows
5. Click Install (this may take 15-30 minutes)

#### Verify Installation

After installation completes, verify it's working:

```powershell
# Check if Visual Studio is detected
"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe" -latest -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64

# This should display information about your VS 2022 installation
```

Restart your terminal after installation.

### 2. Python 3.9+

Install Python 3.9 or later from https://www.python.org/downloads/ or via winget:

```powershell
winget install Python.Python.3.12
```

### 3. CMake

CMake is included with Visual Studio Build Tools, but you can also install it separately:

```powershell
winget install Kitware.CMake
```

## Building Locally

### Using cibuildwheel

The recommended way to build wheels is using cibuildwheel, which handles all Python versions:

```powershell
# Clone the repository
git clone https://github.com/yourusername/vgxserver.git
cd vgxserver

# Install cibuildwheel
pip install cibuildwheel

# Build wheels for all Python versions (3.9-3.13)
# Version is auto-read from VERSION file + dev timestamp
python -m cibuildwheel --platform windows --output-dir wheelhouse

# Build for specific Python version only
$env:CIBW_BUILD = "cp312-*"
python -m cibuildwheel --platform windows --output-dir wheelhouse

# Build with explicit version (for releases)
$env:PROJECT_VERSION = "3.7.0"
python -m cibuildwheel --platform windows --output-dir wheelhouse
```

Wheels will be created in the `wheelhouse/` directory.

**Wheel naming:**
- Development build: `pyvgx-3.7.0.dev0+1770090416-cp312-cp312-win_amd64.whl` (auto-read VERSION + timestamp)
- Release build: `pyvgx-3.7.0-cp312-cp312-win_amd64.whl` (explicit PROJECT_VERSION)

**Note**: cibuildwheel will automatically:
- Create isolated build environments for each Python version
- Install build dependencies
- Build and test the wheels
- Use the Visual Studio 2022 generator (via CMAKE_GENERATOR environment variable)

### Development Build

For development with editable install:

```powershell
# Install in editable mode
pip install -e .

# This builds the extension and allows you to modify Python code without reinstalling
```

## Build Configuration

### Environment Variables

You can customize the cibuildwheel build with these environment variables:

```powershell
# Set package version (for release builds)
# If not set, auto-reads from VERSION file and appends .dev0+<timestamp>
# Example: VERSION file contains "3.7.0" → wheel version becomes "3.7.0.dev0+1770090416"
$env:PROJECT_VERSION = "3.7.0"

# Build specific Python versions
$env:CIBW_BUILD = "cp311-* cp312-*"

# Set build type (release, debug, relWithDebInfo)
$env:CMAKE_PRESET = "release"

# Then build
cibuildwheel --platform windows
```

The CMAKE_GENERATOR is automatically set to "Visual Studio 17 2022" for Windows builds.

### Build Types

- **Release** (default): Optimized build with LTO enabled
- **Debug**: No optimization, debug symbols included
- **RelWithDebInfo**: Optimized with debug symbols

To build debug version:

```powershell
$env:CMAKE_PRESET = "debug"
python -m cibuildwheel --platform windows --output-dir wheelhouse
```

## Testing

cibuildwheel automatically runs tests after building each wheel using the test script configured in `pyproject.toml`.

### Manual Testing

To manually test a built wheel:

```powershell
# Create a test environment
python -m venv test-env
test-env\Scripts\Activate.ps1

# Install the built wheel
pip install wheelhouse\pyvgx-*-win_amd64.whl

# Run tests
python test_pip_package.py
```

Note: The vgxdemoservice test is automatically skipped on Windows as it requires different process management.

## Development Build

For local development with editable install (not using cibuildwheel):

```powershell
# Clone the repository
git clone https://github.com/yourusername/vgxserver.git
cd vgxserver

# Install in editable mode
pip install -e .
```

This builds the extension in-place and allows you to modify Python code without reinstalling. The CMAKE_GENERATOR will be auto-detected (Visual Studio 2022) or you can set it explicitly:

```powershell
$env:CMAKE_GENERATOR = "Visual Studio 17 2022"
pip install -e .
```

## Troubleshooting

### "Visual Studio 17 2022 could not find any instance"

**Problem**: CMake can't find Visual Studio even after installation.

**Solution**:
1. Verify the installation:
   ```powershell
   "C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe" -all -products *
   ```

2. Make sure the C++ workload is installed:
   ```powershell
   "C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe" -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64
   ```

3. If not found, reinstall with the correct workload:
   ```powershell
   winget install Microsoft.VisualStudio.2022.BuildTools --force --override "--wait --quiet --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
   ```

4. Restart your terminal after installation.

### "Could not find python312.lib"

**Problem**: CMake can't find Python development libraries.

**Solution**:
1. Make sure you're using an official Python installation from python.org
2. The dev libraries should be in: `C:\Users\<username>\AppData\Local\Programs\Python\Python312\libs\`
3. If missing, reinstall Python with "pip" and "tcl/tk" options enabled

### "Could not find Python.h"

**Problem**: Python header files not found.

**Solution**:
1. Headers should be in: `C:\Users\<username>\AppData\Local\Programs\Python\Python312\Include\`
2. Reinstall Python ensuring development files are installed
3. Check the installation includes the "py launcher" and "for all users" options

### MSYS2/MinGW Conflicts

**Problem**: Build picks up MinGW gcc instead of MSVC.

**Solution**:
The build system explicitly uses Visual Studio generator which doesn't need compilers in PATH. However, if you see MinGW errors:

1. cibuildwheel isolates the build environment, so this shouldn't happen
2. If it does, ensure CMAKE_GENERATOR is set correctly:
   ```powershell
   $env:CIBW_ENVIRONMENT_WINDOWS = "CMAKE_GENERATOR='Visual Studio 17 2022'"
   python -m cibuildwheel --platform windows --output-dir wheelhouse
   ```

### CMake Generator Conflicts

**Problem**: Error about generator mismatch (e.g., "Does not match the generator used previously").

**Solution**:
This is automatically handled by cibuildwheel, which creates fresh build environments. If you encounter this:

```powershell
# Clean build artifacts
Remove-Item -Recurse -Force build/
Remove-Item -Recurse -Force wheelhouse/

# Rebuild
python -m cibuildwheel --platform windows --output-dir wheelhouse
```

The setup.py automatically cleans CMake cache files to prevent generator conflicts.

### Permissions Errors

**Problem**: "Access denied" errors during installation or build.

**Solution**:
1. Close any Python processes or IDEs
2. Run PowerShell as Administrator for system-wide installations
3. Use virtual environments to avoid system-wide changes

## CI/CD with GitHub Actions

The project uses GitHub Actions to automatically build Windows wheels alongside Linux and macOS builds. The workflow provides flexible build options for different scenarios.

**Windows Build Configuration:**
1. Uses `windows-2022` runner (has Visual Studio 2022 pre-installed)
2. Sets up MSVC environment with `ilammy/msvc-dev-cmd@v1` action
3. Uses Visual Studio 17 2022 generator via `CMAKE_GENERATOR` environment variable
4. Builds wheels for Python 3.9-3.13 (configurable via workflow parameters)
5. Uses cibuildwheel for portable wheel generation

**Workflow Triggers:**
- **Tags (v*)**: Automatically builds release version from tag name
- **Pull Requests (to main)**: Test builds using VERSION file + dev timestamp
- **Releases (published/created)**: Builds release version from release tag
- **Manual (workflow_dispatch)**: Custom builds with optional parameters:
  - `version`: Override default versioning (e.g., "3.7.0")
  - `python_versions`: Select Python versions (e.g., "cp311-* cp312-*")
  - `architectures`: Choose platforms (all/windows/linux_x86_64/linux_aarch64/macos)

**Environment Variables (Windows builds):**
```yaml
CIBW_BUILD: "${{ github.event.inputs.python_versions || '' }}"  # Python versions
CIBW_ENVIRONMENT_WINDOWS: PROJECT_VERSION="..." CMAKE_PRESET=release CMAKE_GENERATOR="Visual Studio 17 2022"
```

**Version Handling:**
- Manual input version (if provided)
- Tag name (for tagged releases)
- VERSION file + .dev0+timestamp (for development builds)

See [.github/workflows/build-wheels.yml](.github/workflows/build-wheels.yml) for complete workflow configuration.

## Additional Resources

- [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022)
- [CMake Documentation](https://cmake.org/documentation/)
- [cibuildwheel Documentation](https://cibuildwheel.readthedocs.io/)
- [Python Packaging Guide](https://packaging.python.org/)

## Getting Help

If you encounter issues not covered here:

1. Check existing issues: https://github.com/yourusername/vgxserver/issues
2. Create a new issue with:
   - Your Windows version
   - Python version (`python --version`)
   - Visual Studio version (from vswhere)
   - Full error output
   - Build command used
