@echo off
setlocal enabledelayedexpansion

REM No OS check needed, as this is Windows-specific

REM Verify current directory is project root by checking for setup.py
if not exist "setup.py" (
    echo ERROR: setup.py not found in current directory.
    echo Please run this script from the project root directory.
    exit /b 1
)

pip uninstall pyvgx -y

echo Cleanup done.

set "PRESET=%~1"
if not defined PRESET set "PRESET=release"

set "VERSION=3.6"

set "PROJECT_VERSION=%VERSION%"
set "CMAKE_PRESET=%PRESET%"
set "BUILT_BY=%COMPUTERNAME%"
set "BUILD_NUMBER=1"
set "BUILD_DIR=..\build-pyvgx"

echo Build directory: %BUILD_DIR%
if not exist "%BUILD_DIR%" mkdir "%BUILD_DIR%"

set "SENTINEL=.tmp-pyvgx-build-dir"

REM Function to safely clear directory
:safe_clear_dir
set "dir=%~1"
set "sentinel=%~2"

if not defined dir goto :safeclear_fail
if "%dir%"=="\" goto :safeclear_fail
if "%dir%"=="." goto :safeclear_fail
if "%dir%"==".." goto :safeclear_fail
if not exist "%dir%\" goto :safeclear_fail

REM Check if sentinel exists or directory is empty
set "is_safe=0"
if exist "%dir%\%sentinel%" set "is_safe=1"
for /f %%i in ('dir /b /a "%dir%" 2^>nul ^| find /c /v ""') do if %%i==0 set "is_safe=1"

if %is_safe%==1 (
    echo Clearing contents of %dir%
    del /q "%dir%\*.*" >nul 2>&1
    for /d %%d in ("%dir%\*") do rd /s /q "%%d" >nul 2>&1
    del /q "%dir%\.*" >nul 2>&1
) else (
    :safeclear_fail
    echo Manually remove %BUILD_DIR% first
    exit /b 1
)

goto :eof

call :safe_clear_dir "%BUILD_DIR%" "%SENTINEL%"

REM Create sentinel
type nul > "%BUILD_DIR%\%SENTINEL%"

REM Copy project to build dir (recursive, preserve attributes)
xcopy /e /i /h /k /y /q "." "%BUILD_DIR%" >nul

pushd "%BUILD_DIR%"

python setup.py build_ext
if errorlevel 1 exit /b 1

python -m build --wheel
if errorlevel 1 exit /b 1

REM Find the wheel file
for /f "delims=" %%w in ('python -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')"' ) do set "PY_VER=%%w"
for %%f in (dist\*%PY_VER%*.whl) do set "WHEEL=%%f"

if not defined WHEEL (
    echo ERROR: Wheel file not found.
    exit /b 1
)

pip install "%WHEEL%"

popd
