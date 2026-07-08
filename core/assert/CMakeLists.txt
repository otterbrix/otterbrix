project(assert)

set(source_${PROJECT_NAME}
        assert.cpp
        trace_full_exception.cpp
        )

add_library(otterbrix_${PROJECT_NAME}
        ${source_${PROJECT_NAME}}
        )


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::log
        Boost::boost
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()

