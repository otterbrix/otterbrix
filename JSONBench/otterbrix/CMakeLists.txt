project(jsonbench_otterbrix)

add_executable(jsonbench_otterbrix jsonbench.cpp)

target_compile_definitions(jsonbench_otterbrix PRIVATE
    JSONBENCH_DATA_FILE="${CMAKE_SOURCE_DIR}/JSONBench/file_0001_filtered.json"
)

target_link_libraries(jsonbench_otterbrix PRIVATE
    cpp_otterbrix
    otterbrix::log
    otterbrix::collection
    otterbrix::cursor
    otterbrix::session
    otterbrix::dispatcher
    otterbrix::wal
    otterbrix::disk
    otterbrix::locks
    spdlog::spdlog
    msgpackc-cxx
    actor-zeta::actor-zeta
    Boost::boost
    fmt::fmt
    ZLIB::ZLIB
    BZip2::BZip2
)
