set(project profile_write_path)

cmake_policy(SET CMP0048 NEW)
PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

add_executable(${PROJECT_NAME} profile_write_path.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_otterbrix
)
