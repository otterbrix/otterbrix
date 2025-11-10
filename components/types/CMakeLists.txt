project(types CXX)

set(SOURCE_${PROJECT_NAME}
        types.cpp
        physical_value.cpp
        logical_value.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

target_link_libraries(
        otterbrix_${PROJECT_NAME}
        otterbrix::serialization
        absl::int128
        Boost::boost
        msgpackc-cxx
        magic_enum::magic_enum
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()
