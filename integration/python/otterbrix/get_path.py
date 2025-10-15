import skbuild.constants

build_dir = skbuild.constants.CMAKE_BUILD_DIR() + "/build/Release/generators"

if __name__ == '__main__':  # pragma: no cover
    print(build_dir)