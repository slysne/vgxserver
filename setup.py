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


if IS_WINDOWS:
    PYTHON_EXECUTABLE = sys.executable
    package_data = {"pyvgx": ["*.pyd", "vgx.dll"]}
else:
    PYTHON_EXECUTABLE = f"python{sys.version_info.major}.{sys.version_info.minor}"
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
        include_dir = sysconfig.get_paths()["include"]
        plat_include_dir = sysconfig.get_paths()["platinclude"]
        python_lib = sysconfig.get_config_var("LIBRARY")
        python_libdir = sysconfig.get_config_var("LIBDIR")
        python_arch = sysconfig.get_platform()

        pyvgx_src_dir = f"{ext.sourcedir}/{PY_SRC_DIR}"
        
        build_cmd = self.get_finalized_command('build')
        
        # Directories used by setuptools
        build_base = os.path.abspath(build_cmd.build_base)
        build_scripts = os.path.abspath(build_cmd.build_scripts)
        build_lib = os.path.abspath(build_cmd.build_lib)
        build_temp = os.path.abspath(build_cmd.build_temp)

        ext_name_path = self.get_ext_fullpath(ext.name)
        ext_name_dirpath =  os.path.dirname(ext_name_path)

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
                f"-DCMAKE_CXX_COMPILER={find_executable('clang++')}"
            ])
        elif IS_LINUX:
            cmake_configure_cmd.extend([
                f"-DCMAKE_C_COMPILER={find_executable('gcc')}",
                f"-DCMAKE_CXX_COMPILER={find_executable('g++')}"
            ])
        elif IS_WINDOWS:

            # Full path to .lib file (e.g., python312.lib)
            python_library_path = os.path.join(python_libdir, python_lib) if python_lib and python_libdir else None

            # include path
            if not include_dir or not os.path.exists(include_dir):
                print(f"[WARN] Could not find sysconfig expected headers 'include': {include_dir}")
                include_dir = "C:\\Python312\\Python-3.12.3\\Include" # TODO FIX
                print(f"[WARN] Using hardcoded path: {include_dir}")
            
            # platinclude path
            if not plat_include_dir or not os.path.exists(plat_include_dir):
                print(f"[WARN] Could not find sysconfig expected headers 'platinclude': {plat_include_dir}")
                plat_include_dir = "C:\\Python312\\Python-3.12.3\\PC" # TODO FIX
                print(f"[WARN] Using hardcoded path: {plat_include_dir}")

            # library path
            if not python_library_path or not os.path.exists(python_library_path):
                print(f"[WARN] Could not find sysconfig expected library {python_library_path}")
                python_library_path = "C:\\Python312\\x64\\Release\\libs\\python312.lib" # TODO FIX
                print(f"[WARN] Using hardcoded path: {python_library_path}")

            include_dirs = f"{include_dir};{plat_include_dir}"

            cmake_configure_cmd.extend([
                f"-DPython3_INCLUDE_DIRS={include_dirs}",
                f"-DPython3_LIBRARIES={python_library_path}"
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
