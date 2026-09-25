project(cpp_otterbrix)


set(${PROJECT_NAME}_sources
      base_spaces.cpp
      catalog_listing.cpp
      wrapper_dispatcher.cpp
      otterbrix.cpp
      connection.cpp
)

set(otterbrix_LIBS
        otterbrix::log
        otterbrix::cursor
        otterbrix::session
        otterbrix::services
        otterbrix::locks
        otterbrix::sql
        otterbrix::b_plus_tree
        otterbrix_catalog

        absl::crc32c
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        actor-zeta::actor-zeta
        Boost::boost
        spdlog::spdlog
)


include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_library(${PROJECT_NAME} ${${PROJECT_NAME}_sources})


target_link_libraries(
        ${PROJECT_NAME} PUBLIC
        ${otterbrix_LIBS}
)

# set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "otterbrix")

if (DEV_MODE)
    add_subdirectory(test)
endif()
