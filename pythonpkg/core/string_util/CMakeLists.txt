project(string_util)

set(header_${PROJECT_NAME}
        string_util.hpp
        case_insensitive.hpp
        )

set(source_${PROJECT_NAME}
        string_util.cpp
        )

add_library(otterbrix_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

