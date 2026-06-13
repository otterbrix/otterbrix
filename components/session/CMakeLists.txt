project(session)

set( ${PROJECT_NAME}_HEADERS
        session.hpp
)

set(${PROJECT_NAME}_SOURCES
        session.cpp

)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)



add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC)

if (DEV_MODE)
    add_subdirectory(tests)
endif()
