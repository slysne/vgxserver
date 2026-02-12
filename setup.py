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

# Read version from VERSION file or environment variable
# Note: Version caching is used to prevent timestamp mismatches when build tools
# (like cibuildwheel) invoke setup.py multiple times during a single build.
version_file = os.path.join(os.path.dirname(__file__), 'VERSION')
cached_version_file = os.path.join(os.path.dirname(__file__), 'build', '.version_cache')
package_version = os.environ.get('PROJECT_VERSION')

if not package_version:
    # Check for cached version first (to ensure consistency across multiple setup.py invocations)
    if os.path.exists(cached_version_file):
        with open(cached_version_file, 'r') as f:
            package_version = f.read().strip()
    else:
        # Try to read from VERSION file
        if os.path.exists(version_file):
            with open(version_file, 'r') as f:
                base_version = f.read().strip()
                # Append dev timestamp to VERSION file content (Maven SNAPSHOT-like)
                package_version = f"{base_version}.dev0+{calendar.timegm(time.gmtime())}"
        else:
            # Fallback to dev version with timestamp
            package_version = f"0.0.0.dev0+{calendar.timegm(time.gmtime())}"

        # Cache the generated version for subsequent setup.py invocations
        os.makedirs(os.path.dirname(cached_version_file), exist_ok=True)
        with open(cached_version_file, 'w') as f:
            f.write(package_version)
            


def get_windows_default_paths():
    # Python.org installers (including NuGet Python used by cibuildwheel) have a standard structure
    #
    # {sys.base_prefix}/
    # ├── libs/
    # │   └── python312.lib
    # └── Include/
    #     ├── Python.h
    #     └── pyconfig.h

    python_lib = f"python{PY_MAJOR}{PY_MINOR}.lib"

    # Standard Python.org structure
    python_library_path = os.path.join(sys.base_prefix, "libs", python_lib)
    include_dir = os.path.join(sys.base_prefix, "Include")

    # Verify files exist
    if not os.path.exists(python_library_path):
        raise EnvironmentError(
            f"Could not find {python_lib} at {python_library_path}\n"
            f"sys.base_prefix: {sys.base_prefix}\n"
            f"Ensure Python was installed from python.org with development files included."
        )

    if not os.path.exists(os.path.join(include_dir, "Python.h")):
        raise EnvironmentError(
            f"Could not find Python.h at {include_dir}\n"
            f"sys.base_prefix: {sys.base_prefix}\n"
            f"Ensure Python was installed from python.org with development files included."
        )

    if not os.path.exists(os.path.join(include_dir, "pyconfig.h")):
        raise EnvironmentError(
            f"Could not find pyconfig.h at {include_dir}\n"
            f"sys.base_prefix: {sys.base_prefix}\n"
            f"Ensure Python was installed from python.org with development files included."
        )

    include_paths = include_dir

    return {
        'include_paths': include_paths,
        'python_library_path': python_library_path
    }



def get_windows_venv_paths():
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

    return {
        'include_paths': include_paths,
        'python_library_path': python_library_path
    }



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

        # Clean CMake cache to avoid generator conflicts
        cmake_cache = os.path.join(cmake_build_dir, "CMakeCache.txt")
        cmake_files_dir = os.path.join(cmake_build_dir, "CMakeFiles")
        if os.path.exists(cmake_cache):
            os.remove(cmake_cache)
            print(f"[BuildEnv] Removed CMake cache: {cmake_cache}")
        if os.path.exists(cmake_files_dir):
            shutil.rmtree(cmake_files_dir)
            print(f"[BuildEnv] Removed CMake files: {cmake_files_dir}")

        # Determine build type from preset
        build_type_map = {
            'release': 'Release',
            'debug': 'Debug',
            'relWithDebInfo': 'RelWithDebInfo'
        }
        build_type = build_type_map.get(preset, 'Release')

        # CMake configuration step — sets up the build system
        cmake_configure_cmd = [
            "cmake",
            cmake_source_dir,  # Path to CMakeLists.txt
            f"-DCMAKE_BUILD_TYPE={build_type}",
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}",  # Output directory for shared libraries
            f"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={extdir}",  # Output directory for static libraries
            f"-DVERSION={package_version}",
            f"-DPython3_EXECUTABLE={find_executable(PYTHON_EXECUTABLE)}",
        ]

        # Enable LTO for release builds
        if build_type == 'Release':
            cmake_configure_cmd.append("-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON")

        # CMake execute build
        cmake_execute_build = [
            "cmake",
            "--build",
            cmake_build_dir,
            "--config", build_type,
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
        shutil.copy2(f"{pyvgx_src_dir}/vgxdemoservice.cmd", f"{self.build_lib}/{PYVGX_SCRIPTS}")

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
            try:
                winpaths = get_windows_default_paths()
            except EnvironmentError as windefault_ex:
                try:
                    winpaths = get_windows_venv_paths()
                except Exception as winvenv_ex:
                    raise windefault_ex

            cmake_configure_cmd.extend([
                f"-DPython3_INCLUDE_DIRS={winpaths['include_paths']}",
                f"-DPython3_LIBRARY={winpaths['python_library_path']}"
            ])

            # Select CMake generator for Windows
            # Windows builds require Visual Studio Build Tools 2022
            cmake_generator = os.environ.get("CMAKE_GENERATOR", "Visual Studio 17 2022")

            cmake_configure_cmd.extend(['-G', cmake_generator, '-A', 'x64'])
            print(f"[BuildEnv] Using generator: {cmake_generator}")

            # Provide helpful message if using default generator
            if "Visual Studio 17 2022" in cmake_generator:
                print("[BuildEnv] Note: Visual Studio Build Tools 2022 with C++ workload is required")
                print("[BuildEnv] Install: winget install Microsoft.VisualStudio.2022.BuildTools --override \"--add Microsoft.VisualStudio.Workload.VCTools\"")

        # Configuration step (generate the build system)
        subprocess.run(
            cmake_configure_cmd,
            cwd=cmake_build_dir,
            check=True
        )

        # Execute build
        subprocess.run(
            cmake_execute_build,
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
    # On macOS, use xcrun to find Xcode toolchain executables
    if IS_MACOS and executable in ['clang', 'clang++']:
        try:
            result = subprocess.run(['xcrun', '--find', executable],
                                    capture_output=True, text=True, check=True)
            executable_path = result.stdout.strip()
            if executable_path and os.path.exists(executable_path):
                print(f"Found {executable} via xcrun: {executable_path}")
                return executable_path
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"xcrun failed for {executable}: {e}, falling back to shutil.which")

    executable_path = shutil.which(executable, path=path)
    if executable_path is None:
        raise RuntimeError("Did not find executable '{}'".format(executable))

    # Verify the executable actually exists and is not a broken symlink
    if not os.path.exists(executable_path):
        raise RuntimeError("Found '{}' at '{}' but file does not exist (broken symlink?)".format(
            executable, executable_path))

    print(f"Found {executable}: {executable_path}")
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
