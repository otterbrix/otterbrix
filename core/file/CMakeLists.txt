project(file)

set(header_${PROJECT_NAME}
        file_handle.hpp
        local_file_system.hpp
        virtual_file_system.hpp
        path_utils.hpp
        file_system.hpp
        list_dir.hpp
        )

set(source_${PROJECT_NAME}
        file_handle.cpp
        local_file_system.cpp
        virtual_file_system.cpp
        path_utils.cpp
        list_dir.cpp
        )

add_library(otterbrix_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        ${Boost_LIBRARIES}
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()
