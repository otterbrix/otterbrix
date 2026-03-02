project(wal)

set( ${PROJECT_NAME}_HEADERS
)

set(${PROJECT_NAME}_SOURCES
        dto.cpp
        manager_wal_replicate.cpp
        wal.cpp
        wal_reader.cpp
)

add_library(otterbrix_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::logical_plan
        otterbrix::locks
        otterbrix::file
        otterbrix::serialization

        spdlog::spdlog
        absl::crc32c
        actor-zeta::actor-zeta
        absl::int128
        Boost::boost
        magic_enum::magic_enum
        msgpackc-cxx
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PRIVATE
        PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif()
