from skbuild import setup
from pathlib import Path


# Get the absolute path to the toolchain file
toolchain_file = Path("./cmake-build/build/Release/generators/conan_toolchain.cmake").resolve()

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
        f"-DCMAKE_TOOLCHAIN_FILE={toolchain_file}",  # Pass toolchain file here
        # "-DCMAKE_TOOLCHAIN_FILE=_skbuild/linux-x86_64-3.8/cmake-build/build/Release/generators",
        "-DCMAKE_BUILD_TYPE=Release",
    ],
)
