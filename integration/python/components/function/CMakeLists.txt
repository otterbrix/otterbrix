project(function CXX)

set(ALL_OBJECT_FILES_FUNCTION
    ${ALL_OBJECT_FILES}
    ${CMAKE_CURRENT_SOURCE_DIR}/function.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/table_function.cpp
    PARENT_SCOPE
)

