project(c_otterbrix C CXX)


set( ${PROJECT_NAME}_headers
        otterbrix.h
)

set(${PROJECT_NAME}_sources
        main.cpp
)

include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_library(${PROJECT_NAME} SHARED
        ${${PROJECT_NAME}_headers}
        ${${PROJECT_NAME}_sources})

# the whole engine, not only what the C API reaches: C++ consumers link libotterbrix too
target_link_libraries(${PROJECT_NAME} PRIVATE $<LINK_LIBRARY:WHOLE_ARCHIVE,otterbrix::otterbrix>)

set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "otterbrix")

if (DEV_MODE)
    add_subdirectory(test)
endif()
