@echo off

SET "DIR=%~dp0"
SET "DEMOMODE=%~1"

if "%DEMOMODE%" == "" (
  echo Specify demo mode 'single' or 'multi' as first argument
  exit /b
)

SET "PYTHONPATH=%PYTHONPATH%;%DIR%"

if "%DEMOMODE%" == "single" (
  echo Starting vgx service instance
  echo:

  echo G1
  @start /MIN python -m vgxdemoservice --demo single --instanceid G1
)

if "%DEMOMODE%" == "multi" (
  echo Starting all vgx service instances
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

