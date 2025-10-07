cmake_minimum_required(VERSION 3.10)


if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Disable interprocedural optimization (LTO) globally
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE CACHE BOOL "Disable LTO" FORCE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_DEBUG FALSE CACHE BOOL "" FORCE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO FALSE CACHE BOOL "" FORCE)
endif()

 # Set project name and version
project(vgxserver)

# Remove thin LTO flags if they are set anywhere else
string(REPLACE "-flto=thin" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
string(REPLACE "-flto=thin" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

# Get full path to venv Python interpreter
execute_process(
  COMMAND python -c "import sys; print(sys.executable)"
  OUTPUT_VARIABLE Python3_EXECUTABLE
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get the Python version numbers
execute_process(
  COMMAND python -c "import sys; print(f'{sys.version_info.major}')"
  OUTPUT_VARIABLE Python3_VERSION_MAJOR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
  COMMAND python -c "import sys; print(f'{sys.version_info.minor}')"
  OUTPUT_VARIABLE Python3_VERSION_MINOR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get the Python library directory
execute_process(
  COMMAND python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"
  OUTPUT_VARIABLE Python3_LIBDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Find python
if(APPLE)
  # Enable RPATH handling on macOS
  set(CMAKE_MACOSX_RPATH ON)
  set(CMAKE_SKIP_RPATH FALSE)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
  set(CMAKE_INSTALL_RPATH "@loader_path")
  set(CMAKE_C_COMPILER "/opt/homebrew/opt/llvm/bin/clang")
  set(CMAKE_CXX_COMPILER "/opt/homebrew/opt/llvm/bin/clang++")

  # Build the path to the libpython .dylib
  set(Python3_LIBRARY "${Python3_LIBDIR}/libpython${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}.dylib")

elseif(CMAKE_HOST_SYSTEM_NAME MATCHES "Linux")
  set(Python3_LIBRARY "${Python3_LIBDIR}/libpython${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}.so")

elseif(CMAKE_HOST_SYSTEM_NAME MATCHES "Windows")
  #set(Python3_LIBRARY "${Python3_LIBDIR}/python${Python3_VERSION_}${Python3_VERSION_MINOR}.lib")
  #    ^^^^^^^^^^^^^^^
  #    Passed in from setup.py
  get_filename_component(Python3_LIBDIR ${Python3_LIBRARY} DIRECTORY)
endif()
  

if(CMAKE_HOST_SYSTEM_NAME MATCHES "Windows")
  # we have passed in the variables
  set(Python3_FOUND TRUE CACHE BOOL "")
else()
  find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
endif()

set(VERSION                     CACHE STRING "Sets the CAP version")
set(ADDITIONAL_LIB_PATHS        CACHE STRING "Additional library paths")
set(ADDITIONAL_INCLUDE_PATHS    CACHE STRING "Additional include directories")

message(STATUS "VGX Server Version: ${VERSION}")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "Archiver for creating static libraries: ${CMAKE_AR}")
message(STATUS "Tool to index archives: ${CMAKE_RANLIB}")
message(STATUS "Linker path: ${CMAKE_LINKER}")
message(STATUS "Archiver for C compiler: ${CMAKE_C_COMPILER_AR}")
message(STATUS "Archiver for C++ compiler: ${CMAKE_CXX_COMPILER_AR}")

message(STATUS "Python Executable: ${Python3_EXECUTABLE}")
message(STATUS "Python Include Dir: ${Python3_INCLUDE_DIRS}")
message(STATUS "Python Library Dir: ${Python3_LIBDIR}")
message(STATUS "Python Library File: ${Python3_LIBRARY}")
message(STATUS "Python Version: ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")

set(LIBS)
set(LIBPATH)
set(LINKFLAGS)
# Define platform-specific settings
set(C_FLAGS)
set(C_FLAGS_DEBUG)
set(C_FLAGS_RELEASE)

set(CC_FLAGS)
set(CC_FLAGS_DEBUG)
set(CC_FLAGS_RELEASE)

set(CXX_FLAGS)
set(CXX_FLAGS_DEBUG)
set(CXX_FLAGS_RELEASE)

set(CPP_DEFINES)
set(CPP_DEFINES_DEBUG)
set(CPP_DEFINES_RELEASE)

# Setup platform-specific settings
if(CMAKE_HOST_SYSTEM_NAME MATCHES "Linux")
    # Define compiler flags
    set(C_FLAGS "-fno-strict-aliasing;-Wstrict-prototypes;-Wno-format")

    set(CC_FLAGS "-pthread;-fPIC;-mavx2;-m64;-mfma")
    # set(CC_FLAGS_DEBUG "-g")
    # set(CC_FLAGS_RELEASE "-O3")

    set(CXX_FLAGS "-std=c++11")

    # Define platform-specific preprocessor symbols
    set(CPP_DEFINES_RELEASE "NDEBUG")

    # Define default libs
    set(LIBS "pthread;rt")

    # Defined linkflags
    set(LINKFLAGS "-Wl,-rpath,\$ORIGIN")
elseif(CMAKE_HOST_SYSTEM_NAME MATCHES "Windows")
    # Define compiler flags
    set(C_FLAGS "/TC")

    set(CC_FLAGS "/W3;/Gm-;/Oi;/Ot;/Gy;/fp:precise;/Zc:wchar_t;/Zc:forScope;/Gd;/errorReport:prompt;/sdl-;/WX-;/Oy-;/nologo;/arch:AVX2;/utf-8;/MP")
    set(CC_FLAGS_DEBUG "/Od;/MDd;/RTC1")
    set(CC_FLAGS_RELEASE "/O2;/Ob2;/GL;/MD")
    
    set(CXX_FLAGS "/EHsc;/GR")

    # Define platform-specific preprocessor symbols
    set(CPP_DEFINES "_WINDOWS;_USRDLL;_CRT_SECURE_NO_WARNINGS;_WIN64;_AMD64_;BOOST_ALL_NO_LIB")
    set(CPP_DEFINES_RELEASE "NDEBUG")

    # Define default libs
    set(LIBS "dbghelp")

    # Defined linkflags
    set(LINKFLAGS "/LTCG")
 elseif(APPLE)
    message(STATUS "Configuring for Apple Silicon (macOS arm64)")

    # Compiler flags for Clang on Apple Silicon
    set(C_FLAGS "-fno-strict-aliasing -Wstrict-prototypes -Wno-format -fcolor-diagnostics -fansi-escape-codes")

    # General optimization and architecture flags
    set(CC_FLAGS "-fPIC -arch arm64 -mcpu=${CLANG_OPTION_MCPU}")  # or apple-m2, etc.
    set(CC_FLAGS_DEBUG "-O0 -g3 -gdwarf-4")

    # C++ standard and flags (note: macOS supports C++11+ by default)
    set(CXX_FLAGS "-std=c++11")

    # Preprocessor defines for macOS
    set(CPP_DEFINES "_DARWIN;APPLE")

    # Python linkage — macOS doesn’t need -lrt or pthread explicitly, just link Python
    #set(LIBS "python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")

    # Linker rpath handling for macOS
    #set(LINKFLAGS "-Wl,-rpath,@loader_path")

else()
    message(FATAl_ERROR "CMake is running on unsupported host: ${CMAKE_HOST_SYSTEM_NAME}")
endif()

# Append version info to cpp_defines
list(APPEND CPP_DEFINES "CXLIB_VERSION=${VERSION}"
                        "COMLIB_VERSION=${VERSION}"
                        "CXMALLOC_VERSION=${VERSION}"
                        "FRAMEHASH_VERSION=${VERSION}"
                        "PYFRAMEHASH_VERSION=${VERSION}"
                        "PYVGX_VERSION=${VERSION}"
                        "VGX_VERSION=${VERSION}")

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(C_FLAGS "${C_FLAGS};${C_FLAGS_DEBUG}")
    set(CC_FLAGS "${CC_FLAGS};${CC_FLAGS_DEBUG}")
    set(CXX_FLAGS "${CXX_FLAGS};${CXX_FLAGS_DEBUG}")
    set(CPP_DEFINES "${CPP_DEFINES};${CPP_DEFINES_DEBUG}")
else()
    set(C_FLAGS "${C_FLAGS};${C_FLAGS_RELEASE}")
    set(CC_FLAGS "${CC_FLAGS};${CC_FLAGS_RELEASE}")
    set(CXX_FLAGS "${CXX_FLAGS};${CXX_FLAGS_RELEASE}")
    set(CPP_DEFINES "${CPP_DEFINES};${CPP_DEFINES_RELEASE}")
endif()

# Define the include & lib paths
set(LIBPATH)
#set(CPPPATH "${Python3_INCLUDE_DIRS}")

if(DEFINED ADDITIONAL_INCLUDE_PATHS AND NOT "${ADDITIONAL_INCLUDE_PATHS}" STREQUAL "")
  list(APPEND CPPPATH "${ADDITIONAL_INCLUDE_PATHS}")
endif()

if(DEFINED ADDITIONAL_LIB_PATHS AND NOT "${ADDITIONAL_LIB_PATHS}" STREQUAL "")
  list(APPEND LIBPATH "${ADDITIONAL_LIB_PATHS}")
endif()

# Set C standard
set(CMAKE_C_STANDARD 99)
# Set C++ standard
set(CMAKE_CXX_STANDARD 11)

# Enable LTO globally
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# Compiler flags
string(JOIN " " C_FLAGS ${C_FLAGS})
string(JOIN " " CC_FLAGS ${CC_FLAGS})
string(JOIN " " CXX_FLAGS ${CXX_FLAGS})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS} ${CC_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS} ${CC_FLAGS}")


# Compiler definitions
add_compile_definitions(${CPP_DEFINES})
# specify linker-specific flags for a target
add_link_options(${LINKFLAGS})
# libraries or flags to use when linking a target
link_libraries(${LIBS})
# Add directories to the linker's search path
link_directories(${LIBPATH})
# Make header files accessible to users of this target
include_directories(${CPPPATH})

# Add the static library from the adjacent directory
add_subdirectory(cxlib)
add_subdirectory(vgx)
add_subdirectory(pyvgx)


if(CMAKE_BUILD_TYPE STREQUAL "Release")
    foreach(target cxlib vgx)
        if(MSVC) 
            # MSVC: /GL (compiler) and /LTCG (linker)
            target_compile_options(${target} PRIVATE /GL)
            target_link_options(${target} PRIVATE /LTCG)
        elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            # GCC/Clang: -flto
            target_compile_options(${target} PRIVATE -flto)
            target_link_options(${target} PRIVATE -flto)
        endif()
    endforeach()
endif()

if(APPLE)
    target_link_options(pyvgx PRIVATE "-undefined" "dynamic_lookup")
endif()




