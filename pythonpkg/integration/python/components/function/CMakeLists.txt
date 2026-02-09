project(function CXX)

set(ALL_OBJECT_FILES_FUNCTION
    ${ALL_OBJECT_FILES}
    ${CMAKE_CURRENT_SOURCE_DIR}/function.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/table_function.cpp
#     table/arrow/arrow_type_info.cpp
#     table/arrow/arrow_otterbrix_schema.cpp
    PARENT_SCOPE
)

# add_library(otterbrix_${PROJECT_NAME}
#         ${SOURCE_${PROJECT_NAME}}
# )

# add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

# set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

# target_include_directories(
#         otterbrix_${PROJECT_NAME}
#         PUBLIC
# )

#target_link_libraries(otterbrix_${PROJECT_NAME}
#    PRIVATE magic_enum::magic_enum)

