from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout


class Pythonpkg(ConanFile):
    name = "pythonpkg"
    version = "1.0.0"

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
        # Setting options for packages
        self.options["otterbrix/*"].shared = True

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        #self.requires("arrow/19.0.1")
        self.requires("boost/1.86.0", override=True)
        self.requires("fmt/11.1.3")
        self.requires("spdlog/1.15.1")
        self.requires("msgpack-cxx/4.1.1")
        self.requires("catch2/2.13.7")
        self.requires("abseil/20230802.1")
        #self.requires("benchmark/1.6.1")
        self.requires("zlib/1.2.12")
        self.requires("bzip2/1.0.8")
        self.requires("otterbrix/1.0.0a10-rc-3")
        self.requires("magic_enum/0.8.1")
        self.requires("actor-zeta/1.0.0a12")
        self.requires("pybind11/2.10.0")

        self.requires("utf8proc/2.9.0")
        self.requires("tabulate/1.5")


    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

