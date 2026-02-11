#!/usr/bin/env python3
"""
Cross-platform test script for pyvgx package installation.
Tests module imports, scripts, and service functionality.
"""
import importlib.metadata
import os
import platform
import shutil
import subprocess
import sys
import time


def run_command(cmd, check=True, capture_output=True, timeout=None):
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            cmd,
            shell=isinstance(cmd, str),
            capture_output=capture_output,
            text=True,
            timeout=timeout,
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        return e
    except subprocess.TimeoutExpired as e:
        return e


def test_module_version():
    """Test that pyvgx module version matches package version."""
    print("Testing module and package versions...")

    try:
        import pyvgx
        module_version = pyvgx.version(0)
        package_version = importlib.metadata.version("pyvgx")

        expected = f"pyvgx v{package_version}"
        if module_version != expected:
            print(f"✗ Version mismatch: module reports '{module_version}', expected '{expected}'")
            return False

        print(f"✓ Version check passed: {module_version}")
        return True
    except Exception as e:
        print(f"✗ Failed to check version: {e}")
        return False


def test_script_availability():
    """Test that vgxadmin script is available."""
    print("\nTesting vgxadmin script availability...")

    if not shutil.which("vgxadmin"):
        print("✗ 'vgxadmin' script not found in PATH")
        return False

    result = run_command(["vgxadmin", "--help"], check=False)
    if isinstance(result, Exception) or result.returncode != 0:
        print("✗ 'vgxadmin --help' failed")
        return False

    print("✓ vgxadmin script is available")
    return True


def test_module_imports():
    """Test that required modules can be imported."""
    print("\nTesting module imports...")

    modules = ["vgxadmin", "vgxinstance"]
    for module_name in modules:
        try:
            __import__(module_name)
            print(f"✓ Successfully imported '{module_name}'")
        except ImportError as e:
            print(f"✗ Failed to import '{module_name}': {e}")
            return False

    return True


def test_vgxdemoservice():
    """Test vgxdemoservice functionality (Linux/macOS x86_64/amd64 only)."""
    print("\nTesting vgxdemoservice...")

    arch = platform.machine().lower()
    system = platform.system()
    print(f"Detected platform: {system} {arch}")

    # Skip on Windows (process management differs significantly)
    # if system == "Windows":
    #     print(f"⊘ Skipping vgxdemoservice test on Windows")
    #     return True

    # Skip on non-x86_64/amd64 architectures
    if arch not in ["x86_64", "amd64"]:
        print(f"⊘ Skipping vgxdemoservice test on {arch} architecture")
        return True

    # Check if vgxdemoservice is available
    if not shutil.which("vgxdemoservice"):
        print("✗ 'vgxdemoservice' script not found")
        return False

    demo_process = None
    try:
        # Start vgxdemoservice in background
        print("Starting vgxdemoservice in background...")
        demo_process = subprocess.Popen(
            ["vgxdemoservice", "multi"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # Wait for services to start
        print("Waiting for services to start (10 seconds)...")
        time.sleep(10)

        # Check instance status
        print("Checking instance status...")
        result = run_command(
            ["vgxadmin", "127.0.0.1:9001", "--status", "*"],
            check=False
        )

        if isinstance(result, Exception):
            print(f"✗ Failed to check status: {result}")
            return False

        # Count S-IN instances
        instance_count = result.stdout.count("S-IN") if result.stdout else 0

        if instance_count != 6:
            print(f"✗ Expected 6 instances, found {instance_count}")
            return False

        print(f"✓ Found {instance_count} running instances")
        return True

    except Exception as e:
        print(f"✗ vgxdemoservice test failed: {e}")
        return False

    finally:
        # Clean up: stop vgxdemoservice
        if demo_process:
            print("Stopping vgxdemoservice...")

            # Try graceful shutdown first
            stop_result = run_command(
                ["vgxdemoservice", "stop"],
                check=False,
                timeout=60
            )

            if isinstance(stop_result, subprocess.TimeoutExpired):
                print("Graceful shutdown timed out, force killing...")

            # Force kill if still running
            try:
                demo_process.kill()
                demo_process.wait(timeout=5)
            except:
                pass

            # Kill any remaining processes
            if sys.platform != "win32":
                # pkill not available on Windows
                subprocess.run(["pkill", "-9", "-f", "vgxdemoservice"],
                             capture_output=True, check=False)
                subprocess.run(["pkill", "-9", "-f", "vgxinstance"],
                             capture_output=True, check=False)
            else:
                # Windows: use taskkill
                subprocess.run(["taskkill", "/F", "/IM", "vgxdemoservice.exe"],
                             capture_output=True, check=False)
                subprocess.run(["taskkill", "/F", "/IM", "vgxinstance.exe"],
                             capture_output=True, check=False)


def main():
    """Run all tests."""
    print("=" * 60)
    print("PyVGX Package Installation Test")
    print("=" * 60)

    tests = [
        ("Module Version", test_module_version),
        ("Script Availability", test_script_availability),
        ("Module Imports", test_module_imports),
        ("VGX Demo Service", test_vgxdemoservice),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            passed = test_func()
            results.append((test_name, passed))
        except Exception as e:
            print(f"\n✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Print summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    all_passed = True
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:30s} {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    if all_passed:
        print("\n✓ All tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
