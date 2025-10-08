@echo off
setlocal enabledelayedexpansion


REM === Verify current directory ===
if not exist "setup.py" (
    echo ERROR: setup.py not found in current directory.
    echo Please run this script from the project root directory.
    exit /b 1
)

REM === Uninstall previous package ===
pip uninstall pyvgx -y
echo Cleanup done.

REM === Configuration Variables ===
set "VERSION=%~1"
set "PRESET=%~2"
set "TEST=%~3"
if not defined VERSION (
    echo ERROR: Must pass version as first argument
    exit /b 1
)
if not defined PRESET set "PRESET=release"
if not defined TEST set "TEST=none"

set "PROJECT_VERSION=%VERSION%"
set "CMAKE_PRESET=%PRESET%"
set "BUILD_DIR=..\build-pyvgx"
set "SENTINEL=_tmp-pyvgx-build-dir"

echo Build directory: %BUILD_DIR%

REM === Call directory cleaner ===
call :safe_clear_dir %BUILD_DIR% %SENTINEL%
if errorlevel 1 exit /b 1

REM === Make sure build dir exists
if not exist "%BUILD_DIR%" mkdir "%BUILD_DIR%"

REM === Create sentinel ===
type nul > "%BUILD_DIR%\%SENTINEL%"

REM === Copy source code ===
robocopy . "%BUILD_DIR%" /E /NFL /NDL /NJH /NJS /NC /NS /XD "%BUILD_DIR%" >nul

REM === Enter build dir ===
pushd "%BUILD_DIR%"

REM === Build extension ===
REM python setup.py build_ext
REM if errorlevel 1 exit /b 1

REM === Build wheel ===
python -m build --wheel
if errorlevel 1 exit /b 1

REM === Find built wheel ===
REM find the python version
for /f "delims=" %%w in (
    'python -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')"'
) do set "PY_VER=%%w"

REM find first wheel file inside "dist" matching python version
pushd dist
for %%f in (
    .\*%PY_VER%*.whl
) do set "WHEEL=%%f"

if not defined WHEEL (
    echo ERROR: Wheel file not found.
    exit /b 1
)

REM === Install new wheel ===
echo "Now running pip install %WHEEL%"
pip install "%WHEEL%" --force-reinstall
popd

python -c "from pyvgx import *; print( f'SUCCESS {version(1)}')"

REM === Run tests
if /i not "%TEST%"=="none" (
    if not exist test mkdir test
    xcopy /E pyvgx\test test
    pushd test
    REM === Quick test
    if /i "%TEST%" == "quick" (
        python test_pyvgx.py -x --quick=1
        if errorlevel 1 exit /b 1
    )
    REM === Full test
    if /i "%TEST%" == "full" (
        python test_pyvgx.py -x
        if errorlevel 1 exit /b 1
    )
    popd
)


REM === Back to original source repo
popd


exit /b 0

REM === Subroutine: Safely clear directory ===
:safe_clear_dir
set "dir=%~1"
set "sentinel=%~2"

if not defined dir goto safeclear_fail
if "%dir%"=="\" goto safeclear_fail
if "%dir%"=="." goto safeclear_fail
if "%dir%"==".." goto safeclear_fail
if not exist "%dir%\" goto safeclear_fail

set "is_safe=0"

REM Check for sentinel
if exist "%dir%\%sentinel%" set "is_safe=1"

REM Or empty directory
for /f %%i in ('dir /b /a "%dir%" 2^>nul ^| find /c /v ""') do (
    if %%i==0 set "is_safe=1"
)

if "%is_safe%"=="1" (
    echo Deleting directory: %dir%
    rd /s /q "%dir%"
    if errorlevel 1 (
        goto safeclear_fail
    )
    goto :eof
)

:safeclear_fail
echo. ERROR: Failed to delete directory: %dir%
echo.        Manual cleanup required
exit /b 1
