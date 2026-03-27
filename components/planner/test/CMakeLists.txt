project(test_translator)

set(${PROJECT_NAME}_SOURCES
        test_logical_plan.cpp
        test_optimizer.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::expressions
        otterbrix::logical_plan
        otterbrix::test_generaty
        otterbrix::planner
        otterbrix::vector
        otterbrix::physical_plan_generator

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
