from skbuild import setup
import skbuild.constants
from pathlib import Path


# Get the absolute path to the toolchain file

file_name = "conan_toolchain.cmake"
found_files = list(Path(skbuild.constants.CMAKE_BUILD_DIR()).rglob(file_name))

path_to_generator = None

if (len(found_files) == 1):
    path_to_generator = found_files[0].resolve()
else:
    raise NotImplementedError("Current setup.py file can't find out conan_toolchain.cmake")

setup(
    name="otterbrix",
    version="1.0.1a9",
    description="""Otterbrix: computation framework for Semi-structured data processing """,
    author=" ",
    license=" ",
    packages=['otterbrix'],
    # package_dir={'': 'integration/python'},
    # package_data={"": []},
    # cmake_install_dir='integration/python',
    python_requires='>=3.7',
    # cmake_source_dir=".",
    include_package_data=True,
    extras_require={"test": ["pytest"]},
    cmake_args=[
        f"-DCMAKE_TOOLCHAIN_FILE={path_to_generator}",  # Pass toolchain file here
        # "-DCMAKE_TOOLCHAIN_FILE=_skbuild/linux-x86_64-3.8/cmake-build/build/Release/generators",
        "-DCMAKE_BUILD_TYPE=Release",
    ],
)
