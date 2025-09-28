set(LIB_NAME pyvgx)
set(PY_PLUGINS ${CMAKE_CURRENT_SOURCE_DIR}/resources/plugins)

# Find python
#find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
#get_filename_component(Python3_LIB_DIR ${Python3_LIBRARIES} DIRECTORY)

# Collect all .c and .h files in this directory and sub-directories
file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/include/*.h"
)

# Add the module library
add_library(${LIB_NAME} MODULE ${SRC_FILES})
set_target_properties(${LIB_NAME} PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION FALSE
)

# Remove the 'lib' prefix
#set_target_properties(${LIB_NAME} PROPERTIES PREFIX "")

execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"
    OUTPUT_VARIABLE PYTHON_ABI_SUFFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Ensure the correct library name
set_target_properties(${LIB_NAME} PROPERTIES 
    OUTPUT_NAME "pyvgx"
    PREFIX ""
    SUFFIX "${PYTHON_ABI_SUFFIX}"
)

add_custom_target(PYVGX_GENERATE_HEADER_FILE
  COMMAND ${Python3_EXECUTABLE} ${PY_PLUGINS}/tools/convert.py ${PY_PLUGINS}/py ${CMAKE_CURRENT_SOURCE_DIR}/src/include/generated
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating VGX XHTML header file"
)

if(APPLE)
    target_link_libraries(${LIB_NAME} PRIVATE cxlib vgx)

    # Ensure main target depends on the pre-build task
    add_dependencies(${LIB_NAME} PYVGX_GENERATE_HEADER_FILE)

    # Make headers accessible to users of this library
    target_include_directories(${LIB_NAME}
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/include
            ${Python3_INCLUDE_DIRS}
    )
else()
    target_link_libraries(${LIB_NAME} PRIVATE cxlib vgx ${Python3_LIBRARIES})

    # Add directories to the linker's search path
    target_link_directories(${LIB_NAME} PRIVATE ${Python3_LIB_DIR})
    
    # Ensure main target depends on the pre-build task
    add_dependencies(${LIB_NAME} PYVGX_GENERATE_HEADER_FILE)

    # Make headers accessible to users of this library
    target_include_directories(${LIB_NAME}
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/include)
endif()
