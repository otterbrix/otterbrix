# rule 9: no throw on the operator-build path.
#
# create_plan_* must return nullptr (and predicate builders return error_t) so the
# executor surfaces failures through the error channel instead of an exception — the
# executor's #522 plan->execute flow is built on that contract. This lint fails the
# build (run as a CTest) if a real `throw` statement slips into
# components/physical_plan{,_generator}. Comment lines (including the "no throw"
# contract notes) are ignored.
#
# Invoked via: cmake -DSRC=<source-root> -P cmake/lint_no_throw.cmake

file(GLOB_RECURSE _files
     "${SRC}/components/physical_plan/*.cpp"
     "${SRC}/components/physical_plan/*.hpp"
     "${SRC}/components/physical_plan_generator/*.cpp"
     "${SRC}/components/physical_plan_generator/*.hpp")

set(_violations "")
foreach(_f ${_files})
    # lines where `throw` appears as a keyword (preceded by a non-identifier char,
    # followed by space/'('/';'): catches `throw e;` / `throw foo(...)` but not `do_not_throw`.
    file(STRINGS "${_f}" _hits REGEX "[^A-Za-z0-9_]throw[ (;]")
    foreach(_l ${_hits})
        if(NOT _l MATCHES "//") # skip comment lines
            list(APPEND _violations "${_f}:${_l}")
        endif()
    endforeach()
endforeach()

if(_violations)
    string(REPLACE ";" "\n  " _msg "${_violations}")
    message(FATAL_ERROR
            "rule 9 violated — throw on the operator-build path "
            "(must return nullptr / error_t instead):\n  ${_msg}")
endif()

message(STATUS "no-throw lint OK: 0 throws under physical_plan{,_generator}")
