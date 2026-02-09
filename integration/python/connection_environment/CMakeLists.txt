set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    ${CMAKE_CURRENT_SOURCE_DIR}/import_cache/importer.cpp  
    ${CMAKE_CURRENT_SOURCE_DIR}/import_cache/python_import_cache.cpp  
    ${CMAKE_CURRENT_SOURCE_DIR}/python_abstract.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/framework_object_detection.cpp
    
    ${CMAKE_CURRENT_SOURCE_DIR}/expression/expression_factory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/relation/relation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/relation/relation_factory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/connection_environment.cpp
    PARENT_SCOPE
)