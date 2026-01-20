project(compute)

set(${PROJECT_NAME}_SOURCES
        compute_kernel.cpp
        compute_result.cpp
        function.cpp
        kernel_executor.cpp
        kernel_signature.cpp
        kernel_utils.cpp

        kernels/aggregate.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::vector
        absl::int128
        msgpackc-cxx
)

target_include_directories( otterbrix_${PROJECT_NAME} PUBLIC )

if (DEV_MODE)
    add_subdirectory(tests)
endif ()
