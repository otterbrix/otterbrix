project(index)

set(${PROJECT_NAME}_SOURCES
        index.cpp
        index_engine.cpp
        single_field_index.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document
        otterbrix::cursor
        otterbrix::context
        otterbrix::logical_plan
        otterbrix::log
        dl
        Boost::boost
        magic_enum::magic_enum
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC

)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
