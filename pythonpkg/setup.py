import sys
from pathlib import Path

    
try:
    from skbuild import setup
except ImportError:
    print(
        "Please update pip, you need pip 10 or greater,\n"
        " or you need to install the PEP 518 requirements in pyproject.toml yourself",
        file=sys.stderr,
    )
    raise

from setuptools import find_packages

import subprocess

subprocess.run(["conan", "install", "conanfile.py", "--build=missing", "--output-folder=conan_build", "-s", "build_type=Release"], check=True)

file_name = "conan_toolchain.cmake"
found_files = list(Path("conan_build").rglob(file_name))

path_to_generator = None

if (len(found_files) == 1): 
    path_to_generator = found_files[0].resolve() 
else:
    raise NotImplementedError("Current setup.py file can't find out conan_toolchain.cmake")


setup(
    name="otterbrix",
    version="0.0.1",
    description="""Otterbrix: computation framework for Semi-structured data processing """,
    author="",
    license="",
    packages=find_packages("integration/python"),
    package_dir={"":"integration/python"},
    cmake_install_dir="integration/python/otterbrix",
    include_package_data=True,
    cmake_source_dir='.',
    cmake_args=[
        f"-DCMAKE_TOOLCHAIN_FILE={path_to_generator}",  # Pass toolchain file here
        "-DCMAKE_BUILD_TYPE=Release",
    ]
)
