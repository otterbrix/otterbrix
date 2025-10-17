cmake_minimum_required(VERSION 3.23...3.28)	# target_link_options()

# Highlight this module has been loaded.
set(Sanitizers_FOUND TRUE)

set(FIND_QUIETLY_FLAG "")
if (DEFINED Sanitizers_FIND_QUIETLY)
    set(FIND_QUIETLY_FLAG "QUIET")
endif ()

find_package(ASan ${FIND_QUIETLY_FLAG})
#find_package(TSan ${FIND_QUIETLY_FLAG})
#find_package(MSan ${FIND_QUIETLY_FLAG})
#find_package(UBSan ${FIND_QUIETLY_FLAG})

