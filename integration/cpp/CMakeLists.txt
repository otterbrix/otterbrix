project(cpp_otterbrix)


set(${PROJECT_NAME}_sources
      base_spaces.cpp
      wrapper_dispatcher.cpp
      otterbrix.cpp
      connection.cpp
)

set(otterbrix_LIBS
        otterbrix::log
        otterbrix::collection
        otterbrix::cursor
        otterbrix::session
        otterbrix::document
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::disk
        otterbrix::locks
        otterbrix::sql
        otterbrix::b_plus_tree
        otterbrix_loader
        otterbrix_catalog
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

add_library(${PROJECT_NAME}_shared SHARED ${${PROJECT_NAME}_sources})

target_link_libraries(
        ${PROJECT_NAME}_shared PRIVATE
        ${otterbrix_LIBS}
        spdlog::spdlog
        pybind11::pybind11
        msgpackc-cxx
        #CONAN_PKG::date
        absl::crc32c
        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        #CONAN_PKG::libcurl
        ZLIB::ZLIB
        BZip2::BZip2
        #libbacktrace::libbacktrace
        #CONAN_PKG::openssl
)

#set_target_properties({PROJECT_NAME}_shared PROPERTIES OUTPUT_NAME "cpp_otterbrix_shared")


if (DEV_MODE OR ALLOW_BENCHMARK)
    set(${PROJECT_NAME}_headers
            wrapper_dispatcher.hpp
    )

    set(${PROJECT_NAME}_sources
            wrapper_dispatcher.cpp
    )

    add_library(otterbrix_${PROJECT_NAME}
            ${${PROJECT_NAME}_headers}
            ${${PROJECT_NAME}_sources}
    )

    add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

    set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

    target_link_libraries(
            otterbrix_${PROJECT_NAME} PUBLIC

            ${otterbrix_LIBS}

            spdlog::spdlog
            msgpackc-cxx
            #CONAN_PKG::date
            absl::crc32c
            absl::int128
            actor-zeta::actor-zeta
            Boost::boost
            fmt::fmt
            #CONAN_PKG::libcurl
            ZLIB::ZLIB
            BZip2::BZip2
            #libbacktrace::libbacktrace
            #CONAN_PKG::openssl
    )
endif()

if (DEV_MODE)
    add_subdirectory(test)
endif()

if (ALLOW_BENCHMARK)
    add_subdirectory(benchmark)
endif()
