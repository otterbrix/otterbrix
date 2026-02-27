project(expressions CXX)

set(SOURCE_${PROJECT_NAME}
        expression.cpp
        compare_expression.cpp
        aggregate_expression.cpp
        scalar_expression.cpp
        sort_expression.cpp
        update_expression.cpp
        function_expression.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::compute
        otterbrix::vector
        otterbrix::serialization
        Boost::boost
        absl::int128
        magic_enum::magic_enum
        msgpackc-cxx
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
