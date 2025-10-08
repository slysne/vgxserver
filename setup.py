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

PYVGX = "pyvgx"
VGXADMIN = "vgxadmin"
VGXINSTANCE = "vgxinstance"
PYVGX_SCRIPTS = "pyvgx_scripts"

PY_SRC_DIR = "pyvgx/src/py"

PY_MAJOR = sys.version_info.major
PY_MINOR = sys.version_info.minor

PYTHON_VERSION = f"{PY_MAJOR}{PY_MINOR}"


if IS_WINDOWS:
    PYTHON_EXECUTABLE = sys.executable
    package_data = {"pyvgx": ["*.pyd", "vgx.dll"]}
else:
    PYTHON_EXECUTABLE = f"python{PY_MAJOR}.{PY_MINOR}"
    if IS_MACOS:
        package_data = {"": ["libvgx.dylib", "pyvgx.so"]}
    elif IS_LINUX:
        package_data = {"": ["libvgx.so", "pyvgx.so"]}
    else:
        raise Exception("Not supported: {}".format(PLAT))

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

        pyvgx_src_dir = f"{ext.sourcedir}/{PY_SRC_DIR}"
        
        build_cmd = self.get_finalized_command('build')
        
        # Directories used by setuptools
        build_base = os.path.abspath(build_cmd.build_base)
        build_scripts = os.path.abspath(build_cmd.build_scripts)
        build_lib = os.path.abspath(build_cmd.build_lib)
        build_temp = os.path.abspath(build_cmd.build_temp)

        ext_name_path = self.get_ext_fullpath(ext.name)
        ext_name_dirpath =  os.path.dirname(ext_name_path)

        print(dir(ext))

        # Final directory where the compiled extension (.so / .pyd) will be placed
        extdir = os.path.abspath(ext_name_dirpath)

        # Source directory for the CMake project (where CMakeLists.txt lives)
        cmake_source_dir = os.path.abspath(ext.sourcedir)

        
        # Dedicated CMake build directory (per extension, inside build/temp)
        cmake_build_dir = os.path.join(build_temp, ext.name)

        print(f"[BuildEnv] Platform:            {PLAT}")
        print(f"[BuildEnv] Python Executable:   {find_executable(PYTHON_EXECUTABLE)}")
        print(f"[BuildEnv] ext.name:            {ext.name}")
        print(f"[BuildEnv] ext_name_path:       {ext_name_path}")
        print(f"[BuildEnv] extdir:              {extdir}")
        print(f"[BuildEnv] cmake_source_dir:    {cmake_source_dir}")
        print(f"[BuildEnv] cmake_build_dir:     {cmake_build_dir}")
        print(f"[BuildEnv] pyvgx_src_dir:       {pyvgx_src_dir}")
        print(f"[BuildEnv] build_base:          {build_base}")
        print(f"[BuildEnv] build_scripts:       {build_scripts}")
        print(f"[BuildEnv] build_lib:           {build_lib}")
        print(f"[BuildEnv] build_temp:          {build_temp}")


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
        copy_files(f"{pyvgx_src_dir}", f"{self.build_lib}/{PYVGX_SCRIPTS}", ext="py", recursive=False)
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
                f"-DCMAKE_CXX_COMPILER={find_executable('clang++')}",
                f"-DCLANG_OPTION_MCPU={os.environ.get('COMPILER_OPTION_MCPU','native')}"
            ])
        elif IS_LINUX:
            cmake_configure_cmd.extend([
                f"-DCMAKE_C_COMPILER={find_executable('gcc')}",
                f"-DCMAKE_CXX_COMPILER={find_executable('g++')}"
            ])
        elif IS_WINDOWS:

            # Fairly reliable
            include_dir1 = sysconfig.get_paths()["include"]
            include_dir2 = None

            # e.g. python312.lib
            python_lib = f"python{PY_MAJOR}{PY_MINOR}.lib"

            # Probably empty
            python_libdir = sysconfig.get_config_var("LIBDIR")
            
            # Will become full path to lib file
            python_library_path = None
            
            # Hint we're in a venv
            if sys.prefix != sys.base_prefix:
                home = getattr(sys, "_home", None)
                if home and os.path.isdir(home):
                    # Set the correct path containing lib file and verify
                    python_libdir = home
                    python_library_path = os.path.join(python_libdir, python_lib)
                    if not os.path.exists( python_library_path ):
                        raise EnvironmentError( f"Bad venv, check paths {sys.prefix} and {home}" )
                # include dirs
                if os.path.isdir(sys.base_prefix):
                    if include_dir1 is None:
                        include_dir1 = os.path.join(sys.base_prefix, "Include")
                    # decide where pyconfig.h is located
                    if os.path.exists( os.path.join(sys.base_prefix, "PC", "pyconfig.h") ):
                        include_dir2 = os.path.join(sys.base_prefix, "PC")
                    elif python_libdir and os.path.exists( os.path.join(python_libdir, "pyconfig.h") ):
                        include_dir2 = python_libdir

            # Verify include path 1 contains Python.h
            while not include_dir1 or not os.path.exists(include_dir1) or not os.path.exists(os.path.join(include_dir1,"Python.h")):
                include_dir1 = input( "Enter include directory containing Python.h: " )

            # Verify include path 2 contains pyconfig.h
            while not include_dir2 or not os.path.exists(include_dir2) or not os.path.exists(os.path.join(include_dir2,"pyconfig.h")):
                include_dir2 = input( "Enter include directory containing pyconfig.h: " )

            
            include_dirs = []
            include_dirs.append(include_dir1)
            if include_dir2 != include_dir1:
                include_dirs.append(include_dir2)

            include_paths = ";".join(include_dirs)

            # Find library path if not already found
            if python_library_path is None:
                while not python_libdir or not os.path.exists(python_libdir) or not os.path.exists(os.path.join(python_libdir,python_lib)):
                    python_libdir = input( f"Enter include directory containing {python_lib}: " )
                python_library_path = os.path.join(python_libdir, python_lib)


            cmake_configure_cmd.extend([
                f"-DPython3_INCLUDE_DIRS={include_paths}",
                f"-DPython3_LIBRARY={python_library_path}"
            ])

            # Use native VS generator for .sln and MSBuild (overrides Ninja from presets)
            cmake_configure_cmd.extend(['-G', 'Visual Studio 17 2022', '-A', 'x64'])  # For VS 2022, x64 arch

            # For multi-config generators like VS, specify config in build
            config = 'Release' if preset == 'release' else 'Debug' if preset == 'debug' else 'RelWithDebInfo'
            cmake_execute_build.extend(['--config', config])            

        # Configuration step (generate the build system)
        subprocess.run(
            cmake_configure_cmd,
            #cwd=cmake_build_dir,
            check=True
        )

        # Execute build
        subprocess.run(
            cmake_execute_build,
            #cwd=cmake_build_dir,
            check=True
        )

        # File extensions to copy based on platform
        if IS_WINDOWS:
            extensions_to_copy = ["dll", "pyd"]
        else:
            extensions_to_copy = ["so"]
            if IS_MACOS:
                extensions_to_copy.append("dylib")
        
        # Ensure target dir exists
        os.makedirs(extdir, exist_ok=True)

        # Copy matching files
        for ext in extensions_to_copy:
            copy_files(build_base, extdir, ext=ext, recursive=True)



def copy_files(source_dir: str, destination_dir: str, ext: str, recursive: bool):
    if recursive:
        match = f"**/*.{ext}"
    elif ext == "*":
        match = "*"
    else:
        match = f"*.{ext}"

    if not os.path.isabs(source_dir):
        source_dir = os.path.abspath(source_dir)
    if not os.path.isabs(destination_dir):
        destination_dir = os.path.abspath(destination_dir)

    pathname = os.path.join(source_dir, match)
    for filepath in glob.glob(pathname, recursive=recursive):
        if os.path.isfile(filepath):
            if os.path.dirname(filepath) == destination_dir:
                print(f"Already in destination dir: {filepath}")
                continue
            print(f"Will copy: {filepath} to {destination_dir}")
            shutil.copy2(filepath, destination_dir)


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
    packages=find_packages(include= [PYVGX, VGXADMIN, VGXINSTANCE, PYVGX_SCRIPTS]),
    package_data=package_data
)

