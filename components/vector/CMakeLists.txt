project(vector)

set(CMAKE_CXX_STANDARD 17)

set(SOURCE_${PROJECT_NAME}
        vector.cpp
        vector_buffer.cpp
        indexing_vector.cpp
        vector_operations.cpp
        data_chunk.cpp
        validation.cpp

        arrow/appender/bool_data.cpp
        arrow/appender/fixed_size_list_data.cpp
        arrow/appender/struct_data.cpp
        arrow/scaner/arrow_type.cpp
        arrow/scaner/arrow_type_info.cpp
        arrow/scaner/arrow_type_extension.cpp
        arrow/scaner/arrow_conversion.cpp
        arrow/arrow_appender.cpp
        arrow/arrow_converter.cpp
        arrow/schema_metadata.cpp
        arrow/arrow_wrapper.cpp
)
include_directories(${CMAKE_SOURCE_DIR})

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::types
        otterbrix::string_heap
        boost::boost
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()