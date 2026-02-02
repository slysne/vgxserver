#!/usr/bin/env python3
import sys
import os
import subprocess

print("=" * 60)
print(f"Python: {sys.version}")
print(f"Platform: {sys.platform}")
print("=" * 60)

def run_test(test_name, func):
    try:
        func()
        print(f"✓ {test_name}")
        return True
    except Exception as e:
        print(f"✗ {test_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

all_passed = True

# Test 1: Import pyvgx
def test_import():
    import pyvgx

test_import()
all_passed &= run_test("Import pyvgx", test_import)

# Test 2: Check version consistency
def test_version():
    import pyvgx
    import importlib.metadata

    pyvgx_version = pyvgx.version(0)
    pip_version = importlib.metadata.version("pyvgx")

    if pyvgx_version != f"pyvgx v{pip_version}":
        raise Exception(f"Version mismatch: pyvgx.version()='{pyvgx_version}' vs pip version='{pip_version}'")

all_passed &= run_test("Version consistency", test_version)

# Test 3: Check vgxadmin command
def test_vgxadmin_cmd():
    result = subprocess.run(['vgxadmin', '--help'])
    if result.returncode != 0:
        raise Exception("vgxadmin command failed")

all_passed &= run_test("vgxadmin command", test_vgxadmin_cmd)

# Test 4: Import vgxadmin module
def test_vgxadmin_module():
    import vgxadmin

all_passed &= run_test("Import vgxadmin module", test_vgxadmin_module)

# Test 5: Import vgxinstance module
def test_vgxinstance_module():
    import vgxinstance

all_passed &= run_test("Import vgxinstance module", test_vgxinstance_module)

# Test 6: Start vgxdemoservice
def test_vgxdemoservice():
    print("  Starting vgxdemoservice...")
    result = subprocess.run(['vgxdemoservice', 'multi'], timeout=30)
    if result.returncode != 0:
        raise Exception("Failed to start vgxdemoservice")

    print("  Checking instance status...")
    result = subprocess.run(['vgxadmin', '127.0.0.1:9001', '--status', '*'],
                          capture_output=True, timeout=10)
    if result.returncode != 0:
        raise Exception("Failed to get vgxadmin status")

    output = result.stdout.decode()
    print(output)
    instance_count = output.count('S-IN')
    if instance_count != 6:
        raise Exception(f"Expected 6 instances, found {instance_count}")

    print("  Stopping vgxdemoservice...")
    try:
        result = subprocess.run(['vgxdemoservice', 'stop'], timeout=30, capture_output=False)
        # vgxdemoservice stop may return non-zero even on success, so we don't check returncode
    except subprocess.TimeoutExpired:
        print("  Graceful shutdown timed out, force killing processes...")
        subprocess.run(['pkill', '-9', '-f', 'vgxdemoservice'], capture_output=True)
        subprocess.run(['pkill', '-9', '-f', 'vgxinstance'], capture_output=True)

all_passed &= run_test("vgxdemoservice functionality", test_vgxdemoservice)

print("=" * 60)
if all_passed:
    print("✓ All tests passed!")
    sys.exit(0)
else:
    print("✗ Some tests failed")
    sys.exit(1)
