project(context)

set( ${PROJECT_NAME}_HEADERS
        context.hpp
)

set(${PROJECT_NAME}_SOURCES
        context.cpp

)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::session
        otterbrix::logical_plan
        actor-zeta::actor-zeta

        absl::int128
        Boost::headers
        msgpackc-cxx
)

target_include_directories( otterbrix_${PROJECT_NAME} PUBLIC )
