#!/usr/bin/env python3
"""
Test script to verify built wheels (manylinux, macOS, etc.)
Usage: python test-wheels.py [wheelhouse_dir]

- Linux wheels (manylinux): Tested in Docker containers
- macOS wheels: Tested natively on macOS, skipped on other platforms
- Windows wheels: Skipped (not supported by this script)
"""

import argparse
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional


# Architecture constants
ARCH_X86_64 = 'x86_64'
ARCH_AARCH64 = 'aarch64'
ARCH_ARM64 = 'arm64'
ARCH_I686 = 'i686'
ARCH_AMD64 = 'amd64'
ARCH_X64 = 'x64'
ARCH_WIN32 = 'win32'

# Platform constants
PLATFORM_LINUX = 'linux'
PLATFORM_MACOS = 'macos'
PLATFORM_DARWIN = 'darwin'
PLATFORM_WINDOWS = 'windows'
PLATFORM_UNKNOWN = 'unknown'


def detect_platform_from_wheel(wheel_name: str) -> str:
    """Detect platform from wheel filename."""
    if 'manylinux' in wheel_name:
        return PLATFORM_LINUX
    elif 'macosx' in wheel_name:
        return PLATFORM_MACOS
    elif 'win' in wheel_name:
        return PLATFORM_WINDOWS
    else:
        return PLATFORM_UNKNOWN


def extract_python_version(wheel_name: str) -> Optional[str]:
    """Extract Python version from wheel filename (e.g., cp312 -> 3.12)."""
    match = re.search(r'cp(\d)(\d+)', wheel_name)
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    return None


def get_host_platform() -> str:
    """Get the current host platform."""
    system = platform.system().lower()
    if system == PLATFORM_DARWIN:
        return PLATFORM_DARWIN
    return system


def get_host_arch() -> str:
    """Get the current host architecture."""
    machine = platform.machine().lower()
    # Normalize architecture names
    if machine in (ARCH_AMD64, ARCH_X86_64, ARCH_X64):
        return ARCH_X86_64
    elif machine in (ARCH_AARCH64, ARCH_ARM64):
        return ARCH_AARCH64
    return machine


def extract_wheel_arch(wheel_name: str) -> Optional[str]:
    """Extract architecture from wheel filename."""
    if ARCH_X86_64 in wheel_name or ARCH_AMD64 in wheel_name:
        return ARCH_X86_64
    elif ARCH_AARCH64 in wheel_name or ARCH_ARM64 in wheel_name:
        return ARCH_AARCH64
    elif ARCH_I686 in wheel_name or ARCH_WIN32 in wheel_name:
        return ARCH_I686
    return None


def test_wheel_macos(wheel_path: Path, python_version: Optional[str], validate_script: Path) -> bool:
    """Test a wheel natively on macOS."""
    if python_version:
        python_cmd = f"python{python_version}"
        print(f"  Testing natively on macOS (Python {python_version})")
        try:
            subprocess.run([python_cmd, '--version'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            python_cmd = 'python3'
            print(f"  Warning: python{python_version} not found, using python3")
    else:
        python_cmd = 'python3'
        print("  Testing natively on macOS (using system python3)")

    with tempfile.TemporaryDirectory() as temp_venv:
        venv_path = Path(temp_venv)

        try:
            subprocess.run([python_cmd, '-m', 'venv', str(venv_path)], check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"  Failed to create virtual environment: {e}")
            return False

        pip_path = venv_path / 'bin' / 'pip'
        python_path = venv_path / 'bin' / 'python'

        try:
            subprocess.run([str(pip_path), 'install', '-q', str(wheel_path)],
                         check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"  Failed to install wheel: {e}")
            return False

        try:
            subprocess.run([str(python_path), str(validate_script)], check=True)
            return True
        except subprocess.CalledProcessError:
            return False


def test_wheel_docker(wheel_path: Path, python_version: Optional[str],
                     validate_script: Path, wheelhouse_dir: Path) -> bool:
    """Test a wheel using Docker."""
    if python_version:
        test_image = f"python:{python_version}-slim"
    else:
        test_image = "python:3-slim"
        print("  Using generic Python 3 image")

    print(f"  Using Docker image: {test_image}")

    wheel_name = wheel_path.name
    docker_cmd = [
        'docker', 'run', '--rm',
        '-v', f"{wheelhouse_dir.absolute()}:/wheels:ro",
        '-v', f"{validate_script.absolute()}:/test_wheel.py:ro",
        test_image,
        'bash', '-c',
        f"pip install -q /wheels/{wheel_name} && python /test_wheel.py"
    ]

    try:
        subprocess.run(docker_cmd, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def test_wheels(wheelhouse_dir: Path) -> int:
    """
    Test all wheels in the wheelhouse directory.
    Returns exit code (0 for success, 1 for failure).
    """
    print(f"Testing wheels in {wheelhouse_dir}")

    if not wheelhouse_dir.is_dir():
        print(f"ERROR: Directory {wheelhouse_dir} does not exist")
        return 1

    wheels = list(wheelhouse_dir.glob('*.whl'))
    if not wheels:
        print(f"ERROR: No wheels found in {wheelhouse_dir}")
        return 1

    print(f"Found {len(wheels)} wheel(s) to test")

    # Get path to validation script
    script_dir = Path(__file__).parent
    validate_script = script_dir / 'test_wheel_validate.py'

    if not validate_script.exists():
        print(f"ERROR: Validation script not found at {validate_script}")
        return 1

    try:
        success = 0
        failed = 0
        skipped = 0

        host_platform = get_host_platform()
        host_arch = get_host_arch()

        for wheel_path in sorted(wheels):
            wheel_name = wheel_path.name
            print()
            print(f"Testing: {wheel_name}")

            wheel_platform = detect_platform_from_wheel(wheel_name)
            wheel_arch = extract_wheel_arch(wheel_name)
            python_version = extract_python_version(wheel_name)

            if not python_version:
                print("  Warning: Could not detect Python version from wheel name")

            # Check platform compatibility
            if wheel_platform == PLATFORM_MACOS and host_platform != PLATFORM_DARWIN:
                print("  ⊘ Skipping macOS wheel on non-macOS host")
                skipped += 1
                continue
            elif wheel_platform == PLATFORM_WINDOWS:
                print("  ⊘ Skipping Windows wheel (not supported by this test script)")
                skipped += 1
                continue
            elif wheel_platform == PLATFORM_UNKNOWN:
                print("  ⊘ Skipping wheel with unknown platform")
                skipped += 1
                continue

            # Check architecture compatibility for Linux wheels
            if wheel_platform == PLATFORM_LINUX and wheel_arch and wheel_arch != host_arch:
                print(f"  ⊘ Skipping {wheel_arch} wheel on {host_arch} host (cross-arch testing not supported)")
                skipped += 1
                continue

            if wheel_platform == PLATFORM_MACOS:
                test_passed = test_wheel_macos(wheel_path, python_version, validate_script)
            else:
                test_passed = test_wheel_docker(wheel_path, python_version, validate_script, wheelhouse_dir)

            if test_passed:
                print(f"✓ {wheel_name} passed all tests")
                success += 1
            else:
                print(f"✗ {wheel_name} failed")
                failed += 1

        print()
        print("=" * 58)
        print("Test Summary")
        print("=" * 58)
        print(f"Total wheels: {len(wheels)}")
        print(f"Passed: {success}")
        if skipped > 0:
            print(f"Skipped: {skipped}")
        if failed > 0:
            print(f"Failed: {failed}")
            return 1
        else:
            print("Failed: 0")
            print("\nAll testable wheels passed!")
            return 0
    except Exception as e:
        print(f"ERROR: Unexpected error during testing: {e}")
        return 1


def main():
    parser = argparse.ArgumentParser(
        description='Test built Python wheels (manylinux, macOS, etc.)'
    )
    parser.add_argument(
        'wheelhouse',
        nargs='?',
        default='wheelhouse',
        help='Directory containing wheel files (default: wheelhouse)'
    )

    args = parser.parse_args()
    wheelhouse_dir = Path(args.wheelhouse)

    exit_code = test_wheels(wheelhouse_dir)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
