@echo off

SET LOCALHOST="127.0.0.1"
SET ADMIN="%LOCALHOST%:9001"

SET "DIR=%~dp0"
SET "DEMOMODE=%~1"

SET "PYTHONPATH=%PYTHONPATH%;%DIR%"

if "%DEMOMODE%" == "" (
  echo.
  echo Usage:
  echo %~nx0 ^<single^|multi^|stop^>
  echo.
  exit /b
)

if /i "%DEMOMODE%" == "stop" (
    vgxadmin %ADMIN% --stop "*" --confirm
    exit /b 0
)

echo Preparing to start...

vgxadmin %ADMIN% >nul 2>&1
set RETCODE=%ERRORLEVEL%

if %RETCODE% EQU 0 (
    vgxadmin %ADMIN% --status "*"
    echo.
    echo Service already running
    exit /b 1
)


set ntarget=0

if /i "%DEMOMODE%" == "single" (
  set ntarget=1
  echo Starting single instance demo
  echo:

  echo G1
  @start /MIN python -m vgxdemoservice --demo single --instanceid G1
)

if /i "%DEMOMODE%" == "multi" (
  set ntarget=6
  echo Starting multi instance demo
  echo:

  echo A1
  @start /MIN python -m vgxdemoservice --demo multi --instanceid A1

  echo TD
  @start /MIN python -m vgxdemoservice --demo multi --instanceid TD

  echo B0.1
  @start /MIN python -m vgxdemoservice --demo multi --instanceid B0.1

  echo B0.2
  @start /MIN python -m vgxdemoservice --demo multi --instanceid B0.2

  echo S1.1
  @start /MIN python -m vgxdemoservice --demo multi --instanceid S1.1

  echo S1.2
  @start /MIN python -m vgxdemoservice --demo multi --instanceid S1.2
)



setlocal ENABLEDELAYEDEXPANSION

set retry=0
set nrunning=0

echo.
<nul set /p=*** Starting %ntarget% service instance(s)...

:wait_loop
if %retry% GEQ 30 goto done

set /a retry+=1

vgxadmin %ADMIN% >nul 2>&1
set retcode=%ERRORLEVEL%

timeout /t 1 >nul
<nul set /p=.

if %retcode% NEQ 0 goto wait_loop

rem Count lines with %COMPUTERNAME% in status output
set nrunning=0
for /f %%L in ('vgxadmin %ADMIN% --status "*" ^| find /i "0d 00:"') do (
    set /a nrunning+=1
)

<nul set /p=!nrunning!

if !nrunning! NEQ %ntarget% goto wait_loop

:done
echo.
endlocal


vgxadmin %ADMIN% >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo *** One or more instances failed to start
    exit /b 1
)



if /i "%DEMOMODE%"=="multi" (
    echo *** Attaching Builder instances
    echo.
    vgxadmin %ADMIN% --attach "B*"
    echo.
)


echo:
echo Sample code location: %DIR%
echo:
echo Show system status:
echo vgxadmin 127.0.0.1:9001 --status *
echo:
echo Display effective system descriptor:
echo vgxadmin 127.0.0.1:9001 --show
echo:
echo Write effective system descriptor to local file:
echo vgxadmin 127.0.0.1:9001 --show ^> vgx.cf

