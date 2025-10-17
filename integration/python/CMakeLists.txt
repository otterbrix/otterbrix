project(otterbrix)

set(${PROJECT_NAME}_sql_sources
        sql/convert.cpp
        sql/spaces.cpp
        sql/wrapper_client.cpp
        sql/wrapper_connection.cpp
        sql/wrapper_collection.cpp
        sql/wrapper_database.cpp
        sql/wrapper_cursor.cpp
        sql/wrapper_document.cpp
        sql/wrapper_document_id.cpp
)

set(otterbrix_LIBS
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
        otterbrix::b_plus_tree
)

list(APPEND CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}")
set(PYBIND11_FINDPYTHON ON)
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_sql_sources})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${otterbrix_LIBS}
        cpp_otterbrix
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

pybind11_extension(${PROJECT_NAME})

if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(${PROJECT_NAME})
endif()

set_target_properties(${PROJECT_NAME} PROPERTIES CXX_VISIBILITY_PRESET "hidden")

#target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

if(SKBUILD)
    install(DIRECTORY otterbrix DESTINATION  . )
    install(TARGETS ${PROJECT_NAME} DESTINATION  ./otterbrix )
    set(CMAKE_TOOLCHAIN_FILE ="conan_toolchain.cmake")
endif()

if (DEV_MODE)
    file(GLOB PY_TESTS
        "tests/*.py"
    )
    file(COPY ${PY_TESTS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()
