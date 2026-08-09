project(execution_context)

add_library(otterbrix_${PROJECT_NAME} INTERFACE)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} INTERFACE
        otterbrix::types
        otterbrix::date
)

target_include_directories(otterbrix_${PROJECT_NAME} INTERFACE)