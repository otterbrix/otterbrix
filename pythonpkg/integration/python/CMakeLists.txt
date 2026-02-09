
if(POLICY CMP0148) # Disallow FindPythonLibs
  cmake_policy(SET CMP0148 NEW)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(external_LIBS
  otterbrix_string_util

  otterbrix::otterbrix
  otterbrix_vector
  otterbrix_table
  magic_enum::magic_enum
  utf8proc::utf8proc
)

set(ALL_OBJECT_FILES)

add_subdirectory(components)
add_subdirectory(pybind11)
# add_subdirectory(common)
add_subdirectory(connection_environment)
add_subdirectory(native)
add_subdirectory(numpy)
add_subdirectory(pandas)
add_subdirectory(scan)
# add_subdirectory(arrow)
add_subdirectory(otterbrix_wrapper)
add_subdirectory(pyconnection)
add_subdirectory(util)


set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} main2.cpp)
pybind11_add_module(otterbrix ${ALL_OBJECT_FILES})
target_link_libraries(otterbrix PUBLIC 
  ${external_LIBS}
)

install(TARGETS otterbrix DESTINATION .)
