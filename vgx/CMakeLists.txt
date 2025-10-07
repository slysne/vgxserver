set(LIB_NAME vgx)
set(WEB_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/src/resources/WEB-ROOT)

# Collect all .c and .h files in this directory and sub-directories
file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/include/*.h"
)

# Add the shared library
add_library(${LIB_NAME}
  SHARED ${SRC_FILES}
)

add_custom_target(VGX_GENERATE_HEADER_FILE
  COMMAND ${Python3_EXECUTABLE} ${WEB_ROOT}/tools/convert.py ${WEB_ROOT}/artifacts ${CMAKE_CURRENT_SOURCE_DIR}/src/include/generated
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating VGX XHTML header file"
)

# Link static library to the shared library
target_link_libraries(${LIB_NAME}
  PRIVATE
    cxlib
)

# Ensure main target depends on the pre-build task
add_dependencies(${LIB_NAME} VGX_GENERATE_HEADER_FILE)

# Make headers accessible to users of this library
target_include_directories(${LIB_NAME}
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/include
)
