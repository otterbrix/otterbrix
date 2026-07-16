project(regex)

set(header_${PROJECT_NAME}
        regex.hpp
        )

set(source_${PROJECT_NAME}
        regex.cpp
        )

add_library(otterbrix_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        re2::re2
        )

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        ${CMAKE_SOURCE_DIR}
)
