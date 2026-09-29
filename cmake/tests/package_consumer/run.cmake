# cmake -P driver: packages the current build with `conan export-pkg` under a throwaway reference, builds
# the consumer project against it through CMakeDeps, runs it, and removes the reference again.
#
# Inputs: SOURCE_DIR, BINARY_DIR, BUILD_TYPE, PROFILE, WORK_DIR, CONAN.

foreach(input SOURCE_DIR BINARY_DIR BUILD_TYPE PROFILE WORK_DIR CONAN)
    if(NOT DEFINED ${input} OR "${${input}}" STREQUAL "")
        message(FATAL_ERROR "package_consumer: ${input} is not set")
    endif()
endforeach()

set(reference "otterbrix/0.0.0-package-test@package_test/tmp")

function(run_step name)
    execute_process(COMMAND ${ARGN}
                    WORKING_DIRECTORY "${SOURCE_DIR}"
                    RESULT_VARIABLE result
                    OUTPUT_VARIABLE output
                    ERROR_VARIABLE output)
    if(NOT result EQUAL 0)
        execute_process(COMMAND "${CONAN}" remove "${reference}" -c OUTPUT_QUIET ERROR_QUIET)
        message(FATAL_ERROR "package_consumer: ${name} failed (${result}):\n${output}")
    endif()
endfunction()

file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")
file(COPY "${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt" "${CMAKE_CURRENT_LIST_DIR}/main.cpp" DESTINATION "${WORK_DIR}")
foreach(file demo_ast.hpp demo_extension.cpp demo_extension.hpp demo_gram.y demo_scan.l)
    file(COPY "${SOURCE_DIR}/components/sql/demo_extension/${file}" DESTINATION "${WORK_DIR}")
endforeach()
file(WRITE "${WORK_DIR}/conanfile.txt" "[requires]\n${reference}\n\n[generators]\nCMakeDeps\nCMakeToolchain\n")

string(REGEX MATCH "^otterbrix/([^@]+)@([^/]+)/(.+)$" parsed "${reference}")
run_step("conan export-pkg"
         "${CONAN}" export-pkg "${SOURCE_DIR}" -pr:h "${PROFILE}" -pr:b default
         --version "${CMAKE_MATCH_1}" --user "${CMAKE_MATCH_2}" --channel "${CMAKE_MATCH_3}"
         -of "${BINARY_DIR}" --test-folder=)
run_step("conan install" "${CONAN}" install "${WORK_DIR}" -pr:h "${PROFILE}" -pr:b default -of "${WORK_DIR}/build")
# The package's own bison/flex wrappers: a prebuilt bison carries the m4 path of the machine that built it.
run_step("consumer configure"
         "${CMAKE_COMMAND}" -S "${WORK_DIR}" -B "${WORK_DIR}/build" -G Ninja
         "-DCMAKE_TOOLCHAIN_FILE=${WORK_DIR}/build/conan_toolchain.cmake"
         "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
         "-DBISON_EXECUTABLE=${BINARY_DIR}/generators/bison"
         "-DFLEX_EXECUTABLE=${BINARY_DIR}/generators/flex")
run_step("consumer build" "${CMAKE_COMMAND}" --build "${WORK_DIR}/build")
run_step("consumer run" "${WORK_DIR}/build/consumer")
execute_process(COMMAND "${CONAN}" remove "${reference}" -c OUTPUT_QUIET ERROR_QUIET)
file(REMOVE_RECURSE "${WORK_DIR}")
