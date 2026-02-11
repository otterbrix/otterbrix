project(b_plus_tree)

set(header_${PROJECT_NAME}
        block.hpp
        segment_tree.hpp
        b_plus_tree.hpp
        msgpack_reader/msgpack_reader.hpp
        )

set(source_${PROJECT_NAME}
        block.cpp
        segment_tree.cpp
        b_plus_tree.cpp
        )

add_library(otterbrix_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        absl::crc32c
        msgpackc-cxx
        otterbrix::types
        otterbrix::file
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()