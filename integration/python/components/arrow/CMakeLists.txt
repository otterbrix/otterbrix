project(arrow)

set(${PROJECT_NAME}_SOURCES
    appender/bool_data.cpp
    appender/fixed_size_list_data.cpp
    appender/struct_data.cpp
    arrow_appender.cpp
    arrow_converter.cpp
    schema_metadata.cpp
    arrow_wrapper.cpp
    PARENT_SCOPE
)
