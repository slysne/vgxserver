import calendar
import glob
import os
import pathlib
import shutil
import subprocess
import sys
import sysconfig
import time
import platform
from pathlib import Path

from setuptools import Extension, setup, find_packages
from setuptools.command.build_ext import build_ext

PLAT = platform.system()

IS_MACOS = PLAT == "Darwin"
IS_LINUX = PLAT == "Linux"
IS_WINDOWS = PLAT == "Windows"

if IS_MACOS:
    package_data = {"": ["libcxlib.a", "libvgx.dylib", "pyvgx.so"]}
elif IS_LINUX:
    package_data = {"": ["libcxlib.a", "libvgx.so", "pyvgx.so"]}
elif IS_WINDOWS:
    package_data = {"": ["libcxlib.lib", "libvgx.dll", "pyvgx.pyd"]} 
else:
    raise Exception("Not supported: {}".format(PLAT))

PYVGX = "pyvgx"
VGXADMIN = "vgxadmin"
VGXINSTANCE = "vgxinstance"
PYVGX_SCRIPTS = "pyvgx_scripts"
PYTHON_EXECUTABLE = f"python{sys.version_info.major}.{sys.version_info.minor}"

preset = os.getenv("CMAKE_PRESET", "release")  # default to release

if preset not in ['release', 'debug', 'relWithDebInfo']:
    raise Exception(f"Unknown cmake preset {preset}")


if 'SNAPSHOT' in os.environ.get('PROJECT_VERSION'):
    package_version = os.environ.get('PROJECT_VERSION').replace("-SNAPSHOT",
                                                              f".dev0+{calendar.timegm(time.gmtime())}")
else:
    package_version = os.environ.get('PROJECT_VERSION')


class PyVGX_Extension(Extension):

    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.fspath(Path(sourcedir).resolve())


class CmakeBuild(build_ext):
    def build_extension(self, ext: PyVGX_Extension) -> None:


        #raise NotImplementedError( "Working on it" )

        pyvgx_src_dir = f"{ext.sourcedir}/pyvgx/src/py"

        # Final directory where the compiled extension (.so / .pyd) will be placed
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))

        # Source directory for the CMake project (where CMakeLists.txt lives)
        cmake_source_dir = os.path.abspath(ext.sourcedir)

        # Root build/temp directory used by setuptools for temporary build files
        build_temp = os.path.abspath(self.build_temp)
        
        # Dedicated CMake build directory (per extension, inside build/temp)
        cmake_build_dir = os.path.join(build_temp, ext.name)

        print(f"[BuildEnv] Platform:          {PLAT}")
        print(f"[BuildEnv] Python Executable: {find_executable(PYTHON_EXECUTABLE)}")
        print(f"[BuildEnv] self.build_lib:    {self.build_lib}")
        print(f"[BuildEnv] pyvgx_src_dir:     {pyvgx_src_dir}")
        print(f"[BuildEnv] extdir:            {extdir}")
        print(f"[BuildEnv] cmake_source_dir:  {cmake_source_dir}")
        print(f"[BuildEnv] build_temp:        {build_temp}")
        print(f"[BuildEnv] cmake_build_dir:   {cmake_build_dir}")


        # Make sure the build directory exists
        os.makedirs(cmake_build_dir, exist_ok=True)

        # CMake configuration step — sets up the build system
        cmake_configure_cmd = [
            "cmake",
            cmake_source_dir,  # Path to CMakeLists.txt
            f"--preset={preset}",
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}",  # Output directory for shared libraries
            f"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={extdir}",  # Output directory for static libraries
            f"-DVERSION={package_version}",
            f"-DBUILT_BY={os.environ.get('BUILT_BY')}",
            f"-DBUILD_NUMBER={os.environ.get('BUILD_NUMBER')}",
            f"-DPython3_EXECUTABLE={find_executable(PYTHON_EXECUTABLE)}",
        ]

        # CMake execute build
        cmake_execute_build = [
            "cmake",
            "--build",
            f"--preset={preset}",
            "--verbose"
        ]

        # create package directories
        for DIR in (PYVGX, VGXADMIN, VGXINSTANCE, PYVGX_SCRIPTS):
            pathlib.Path(f"{self.build_lib}/{DIR}").mkdir(parents=True, exist_ok=True)
            shutil.copy2(f"{pyvgx_src_dir}/__init__.py", f"{self.build_lib}/{DIR}")

        # copy "vgxadmin", "vgxinstance" files
        shutil.copy2(f"{pyvgx_src_dir}/vgxadmin.py", f"{self.build_lib}/{VGXADMIN}")
        shutil.copy2(f"{pyvgx_src_dir}/vgxinstance.py", f"{self.build_lib}/{VGXINSTANCE}")
        # copy "pyvgx_scripts" files
        copy_files(f"{pyvgx_src_dir}", f"{self.build_lib}/{PYVGX_SCRIPTS}", "py")
        shutil.copy2(f"{pyvgx_src_dir}/vgxdemoservice", f"{self.build_lib}/{PYVGX_SCRIPTS}")
        shutil.copy2(f"{pyvgx_src_dir}/vgxdemoservice", f"{self.build_lib}/{PYVGX_SCRIPTS}")

        # overwrite python package __init__.py files
        with open(f'{self.build_lib}/{PYVGX_SCRIPTS}/__init__.py', 'w') as f:
            f.write("import pkgutil\n__path__ = pkgutil.extend_path(__path__, __name__)")
        with open(f'{self.build_lib}/{VGXADMIN}/__init__.py', 'w') as f:
            f.write("import sys\nfrom . import vgxadmin as _vgxadmin\nsys.modules[__name__] = _vgxadmin")
        with open(f'{self.build_lib}/{VGXINSTANCE}/__init__.py', 'w') as f:
            f.write("import sys\nfrom . import vgxinstance as _vgxinstance\nsys.modules[__name__] = _vgxinstance")
        
        if IS_MACOS:
            cmake_configure_cmd.extend([
                f"-DCMAKE_OSX_DEPLOYMENT_TARGET=" + os.environ.get("MACOSX_DEPLOYMENT_TARGET", "14.0"),
                f"-DCMAKE_C_COMPILER={find_executable('clang')}",
                f"-DCMAKE_CXX_COMPILER={find_executable('clang++')}"
            ])
        elif IS_LINUX:
            cmake_configure_cmd.extend([
                f"-DCMAKE_C_COMPILER={find_executable('gcc')}",
                f"-DCMAKE_CXX_COMPILER={find_executable('g++')}"
            ])
        elif IS_WINDOWS:
            # Use native VS generator for .sln and MSBuild (overrides Ninja from presets)
            cmake_configure_cmd.extend(['-G', 'Visual Studio 17 2022', '-A', 'x64'])  # For VS 2022, x64 arch

            # For multi-config generators like VS, specify config in build
            config = 'Release' if preset == 'release' else 'Debug' if preset == 'debug' else 'RelWithDebInfo'
            cmake_execute_build.extend(['--config', config])

            # # Ensure MSVC compiler (CMake detects it automatically)
            # cmake_configure_cmd.extend([
            #     f"-DCMAKE_C_COMPILER=cl.exe",  # MSVC C compiler
            #     f"-DCMAKE_CXX_COMPILER=cl.exe"  # MSVC C++ compiler
            # ])

        # Configuration step (generate the build system)
        subprocess.run(
            cmake_configure_cmd,
            cwd=cmake_build_dir,
            check=True
        )

        # Execute build
        subprocess.run(
            cmake_execute_build,
            cwd=cmake_build_dir,
            check=True
        )

        # Target location where setuptools expects files for packaging
        target_dir = os.path.join(self.build_lib, "pyvgx")

        # Ensure target dir exists
        os.makedirs(target_dir, exist_ok=True)

        # File extensions to copy based on platform
        if IS_WINDOWS:
            extensions_to_copy = ["lib", "dll", "pyd"]
        else:
            extensions_to_copy = ["so", "a"]
            if IS_MACOS:
                extensions_to_copy.append("dylib")

        # Copy matching files
        for ext in extensions_to_copy:
            for file in glob.glob(os.path.join(extdir, f"**/*.{ext}"), recursive=True):
                #shutil.copy(file, target_dir)  # CHANGED: Actually copy (your original has print(file)—was that a debug leftover?)
                print(f"Copied: {file} to {target_dir}")



def copy_files(source_dir: str, destination_dir: str, type: str):
    files = glob.iglob(os.path.join(source_dir, "*" if type == '*' else f"*.{type}"))
    for file in files:
        if os.path.isfile(file):
            shutil.copy2(file, destination_dir)


def find_executable(executable, path=None):
    executable_path = shutil.which(executable, path=path)
    if executable_path is None:
        raise RuntimeError("Did not find executable '{}'".format(executable))

    return executable_path




setup(
    name="pyvgx",
    version=package_version,
    ext_modules = [PyVGX_Extension("pyvgx.pyvgx", ".")],
    cmdclass={
        "build_ext": CmakeBuild,
    },
    zip_safe=False,
    packages=find_packages(include=["pyvgx", "vgxadmin", "vgxinstance", "pyvgx_scripts"]),
    package_data=package_data,
)
