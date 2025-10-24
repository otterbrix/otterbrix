project(test_c_otterbrix)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES

)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE

        cpp_otterbrix

        otterbrix::test_generaty
        otterbrix::log
        otterbrix::memory_storage
        otterbrix::collection
        otterbrix::cursor
        otterbrix::session
        otterbrix::document
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::disk
        otterbrix::locks

        spdlog::spdlog
        msgpackc-cxx
        #CONAN_PKG::date
        Catch2::Catch2
        absl::crc32c
        absl::int128
        benchmark::benchmark
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        #CONAN_PKG::libcurl
        ZLIB::ZLIB
        BZip2::BZip2
        #libbacktrace::libbacktrace

)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
