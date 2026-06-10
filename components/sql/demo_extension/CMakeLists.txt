project(demo_extension CXX)

include(otterbrix_parser_extension)

# use to simplify bison/flex generation
otterbrix_add_parser_extension(demo_extension
        PREFIX demo
        SOURCES demo_extension.cpp)
