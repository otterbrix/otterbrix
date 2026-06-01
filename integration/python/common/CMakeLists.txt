set(ALL_OBJECT_FILES 
    ${ALL_OBJECT_FILES} ${CMAKE_CURRENT_SOURCE_DIR}/exceptions.cpp  
    PARENT_SCOPE
)

#set(PYBIND11_FINDPYTHON ON)
#find_package(pybind11 CONFIG REQUIRED)

#pybind11_add_module(exception main.cpp exceptions.cpp)

#target_link_libraries(exception PUBLIC otterbrix::core_types)
