set(LIB_NAME cxlib)

# Collect all .cpp and .h files in this directory and sub-directories
file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/include/*.h"
)

# Create the static library
add_library(${LIB_NAME} STATIC ${SRC_FILES})

# Make headers accessible to users of this library
target_include_directories(${LIB_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/include)

