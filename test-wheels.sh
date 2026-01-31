#!/bin/bash
# Test script to verify manylinux wheels
# Usage: ./test-wheels.sh [wheelhouse_dir]

WHEELHOUSE=${1:-"wheelhouse"}

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Testing wheels in ${WHEELHOUSE}${NC}"

if [ ! -d "${WHEELHOUSE}" ] || [ -z "$(ls -A ${WHEELHOUSE}/*.whl 2>/dev/null)" ]; then
    echo -e "${RED}ERROR: No wheels found in ${WHEELHOUSE}${NC}"
    exit 1
fi

# Count wheels
WHEEL_COUNT=$(ls -1 ${WHEELHOUSE}/*.whl 2>/dev/null | wc -l)
echo -e "${GREEN}Found ${WHEEL_COUNT} wheel(s) to test${NC}"

# Test each wheel
SUCCESS=0
FAILED=0

for wheel in ${WHEELHOUSE}/*.whl; do
    WHEEL_NAME=$(basename "$wheel")
    echo ""
    echo -e "${YELLOW}Testing: ${WHEEL_NAME}${NC}"
    
    # Detect Python version from wheel filename (e.g., cp312 -> 3.12)
    if [[ $WHEEL_NAME =~ cp([0-9])([0-9]+) ]]; then
        PY_MAJOR="${BASH_REMATCH[1]}"
        PY_MINOR="${BASH_REMATCH[2]}"
        TEST_IMAGE="python:${PY_MAJOR}.${PY_MINOR}-slim"
        echo -e "  Using Docker image: ${TEST_IMAGE}"
    else
        TEST_IMAGE="python:3.11-slim"
        echo -e "  ${YELLOW}Warning: Could not detect Python version, using default ${TEST_IMAGE}${NC}"
    fi
    
    # Create comprehensive test script (merged from test_pip_package.sh)
    cat > /tmp/test_wheel.py << 'EOF'
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
EOF
    
    # Run comprehensive test in Docker container
    if docker run --rm \
        -v "$(pwd)/${WHEELHOUSE}:/wheels:ro" \
        -v "/tmp/test_wheel.py:/test_wheel.py:ro" \
        ${TEST_IMAGE} \
        bash -c "pip install -q /wheels/${WHEEL_NAME} && python /test_wheel.py"; then
        echo -e "${GREEN}✓ ${WHEEL_NAME} passed all tests${NC}"
        ((SUCCESS++))
    else
        echo -e "${RED}✗ ${WHEEL_NAME} failed${NC}"
        ((FAILED++))
    fi
done

# Summary
echo ""
echo "=========================================================="
echo -e "${GREEN}Test Summary${NC}"
echo "=========================================================="
echo -e "Total wheels: ${WHEEL_COUNT}"
echo -e "${GREEN}Passed: ${SUCCESS}${NC}"
if [ ${FAILED} -gt 0 ]; then
    echo -e "${RED}Failed: ${FAILED}${NC}"
    exit 1
else
    echo -e "Failed: 0"
    echo -e "\n${GREEN}All wheels passed tests!${NC}"
    exit 0
fi
