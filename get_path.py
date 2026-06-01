import skbuild.constants

build_dir = skbuild.constants.CMAKE_BUILD_DIR()

if __name__ == '__main__':  # pragma: no cover
    print(build_dir)