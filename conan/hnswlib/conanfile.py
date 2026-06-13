import os

from conan import ConanFile
from conan.tools.files import copy, get
from conan.tools.layout import basic_layout


class HnswlibConan(ConanFile):
    name = "hnswlib"
    version = "0.8.0"
    license = "Apache-2.0"
    homepage = "https://github.com/nmslib/hnswlib"
    description = "Header-only C++ HNSW approximate nearest neighbor search"
    settings = "os", "arch", "compiler", "build_type"
    no_copy_source = True

    def source(self):
        get(self,
            "https://github.com/nmslib/hnswlib/archive/refs/tags/v0.8.0.tar.gz",
            strip_root=True)

    def layout(self):
        basic_layout(self, src_folder="src")

    def package(self):
        copy(self, "*.h",
             src=os.path.join(self.source_folder, "hnswlib"),
             dst=os.path.join(self.package_folder, "include", "hnswlib"))

    def package_info(self):
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
