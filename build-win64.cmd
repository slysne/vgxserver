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
set "PRESET=%~1"
if not defined PRESET set "PRESET=release"

set "VERSION=3.6"
set "PROJECT_VERSION=%VERSION%"
set "CMAKE_PRESET=%PRESET%"
set "BUILT_BY=%COMPUTERNAME%"
set "BUILD_NUMBER=1"
set "BUILD_DIR=..\build-pyvgx"
set "SENTINEL=_tmp-pyvgx-build-dir"

echo Build directory: %BUILD_DIR%
if not exist "%BUILD_DIR%" mkdir "%BUILD_DIR%"

REM === Call directory cleaner ===
call :safe_clear_dir %BUILD_DIR% %SENTINEL%
if errorlevel 1 exit /b 1

REM === Create sentinel ===
type nul > "%BUILD_DIR%\%SENTINEL%"

echo "will robocopy!"
REM === Copy source code ===
robocopy . "%BUILD_DIR%" /E /NFL /NDL /NJH /NJS /NC /NS /XD "%BUILD_DIR%" >nul


echo "here!"
REM === Enter build dir ===
pushd "%BUILD_DIR%"

echo "there!"

REM === Build extension ===
python setup.py build_ext
if errorlevel 1 exit /b 1

REM === Build wheel ===
python -m build --wheel
if errorlevel 1 exit /b 1

REM === Find built wheel ===
for /f "delims=" %%w in ('python -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')"' ) do set "PY_VER=%%w"
for %%f in (dist\*%PY_VER%*.whl) do set "WHEEL=%%f"

if not defined WHEEL (
    echo ERROR: Wheel file not found.
    exit /b 1
)

REM === Install new wheel ===
pip install "%WHEEL%"

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
    echo Clearing contents of %dir%
    del /f /q "%dir%\*.*" >nul 2>&1
    for /d %%d in ("%dir%\*") do rd /s /q "%%d" >nul 2>&1
    del /f /q "%dir%\.*" >nul 2>&1
    goto :eof
)

:safeclear_fail
echo ERROR: Refusing to clear %dir% — no sentinel or not empty.
exit /b 1
