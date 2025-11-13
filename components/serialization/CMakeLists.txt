project(serialization CXX)

set(SOURCE_${PROJECT_NAME}
        serializer.cpp
        deserializer.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        magic_enum::magic_enum
        msgpackc-cxx
        absl::int128
        Boost::boost
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
