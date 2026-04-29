set(CURRENT_TARGET log)

list(APPEND CURRENT_${TARGET_HEADERS}
        log.hpp
)

list(APPEND CURRENT_${TARGET_HEADERS}
        log.cpp
)

add_library(otterbrix_${CURRENT_TARGET}
            ${CURRENT_${TARGET_HEADERS}}
            ${CURRENT_${TARGET_HEADERS}}
)


add_library(otterbrix::${CURRENT_TARGET} ALIAS otterbrix_${CURRENT_TARGET})

set_property(TARGET otterbrix_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        otterbrix_${CURRENT_TARGET} PUBLIC
        spdlog::spdlog
        Boost::headers
        dl
        fmt::fmt
)

target_include_directories(
        otterbrix_${CURRENT_TARGET}
        PUBLIC
)

if(DEV_MODE)
    #add_subdirectory(test)
endif()

install(TARGETS
        otterbrix_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
