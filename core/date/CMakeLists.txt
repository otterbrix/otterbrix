project(date)

set(header_${PROJECT_NAME}
        date_types.hpp
        date_cast.hpp
        date_parse.hpp
        timezones.hpp
        )

set(source_${PROJECT_NAME}
        timezones.cpp
        )

add_library(otterbrix_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        ${CMAKE_SOURCE_DIR}
)
