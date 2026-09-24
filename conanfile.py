import os
import textwrap

from conan import ConanFile
from conan.errors import ConanException
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain
from conan.tools.env import VirtualBuildEnv
from conan.tools.files import rm, save


class OtterbrixConan(ConanFile):
    name = "otterbrix"
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "build_python": [True, False],
    }
    default_options = {
        "shared": False,
        "build_python": False,
        "boost/*:header_only": True,
        "actor-zeta/*:cxx_standard": 20,
        "actor-zeta/*:fPIC": True,
        "actor-zeta/*:exceptions_disable": False,
        "actor-zeta/*:rtti_disable": False,
    }
    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "core/*",
        "components/*",
        "services/*",
        "integration/CMakeLists.txt",
        "integration/cpp/*",
        "integration/c/*",
    )

    def requirements(self):
        self.requires("boost/1.88.0")
        self.requires("fmt/11.1.3")
        self.requires("spdlog/1.15.1")
        if self.options.build_python:
            self.requires("pybind11/2.13.6")
        self.requires("catch2/3.15.1")
        # force: re2's recipe pins an older abseil range; override it so re2 and the
        # rest of the tree share the single abseil binary we already depend on.
        self.requires("abseil/20260107.1", force=True)
        self.requires("re2/20240702")
        self.requires("benchmark/1.6.1")
        self.requires("actor-zeta/1.2.0")

    def build_requirements(self):
        self.tool_requires("bison/3.8.2")
        self.tool_requires("flex/2.6.4")

    def validate(self):
        check_min_cppstd(self, 20)

    def layout(self):
        self.folders.generators = "generators"

    def generate(self):
        tc = CMakeToolchain(self)
        tc.user_presets_path = False
        # the shared option selects what package() ships; BUILD_SHARED_LIBS would turn every component into a .so
        tc.blocks.remove("shared")
        tc.variables["BUILD_PYTHON"] = bool(self.options.build_python)

        # A prebuilt bison carries the m4 path of the machine that built it, and flex looks "m4" up on PATH,
        # so both run through wrappers that export the m4 of the build environment.
        env = VirtualBuildEnv(self, auto_generate=True).vars()
        m4 = env.get("M4")
        if not m4:
            raise ConanException("M4 is not defined by the build environment of bison/flex")
        for tool, variable in (("bison", "BISON_EXECUTABLE"), ("flex", "FLEX_EXECUTABLE")):
            wrapper = os.path.join(self.generators_folder, tool)
            executable = os.path.join(self.dependencies.build[tool].package_folder, "bin", tool)
            save(self, wrapper, textwrap.dedent(f"""\
                #!/bin/sh
                export M4="{m4}"
                export BISON_PKGDATADIR="{env.get("BISON_PKGDATADIR", "")}"
                exec "{executable}" "$@"
                """))
            os.chmod(wrapper, 0o755)
            tc.variables[variable] = wrapper
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        CMake(self).install()
        lib = os.path.join(self.package_folder, "lib")
        if self.options.shared:
            rm(self, "*.a", lib)
        else:
            rm(self, "*.so*", lib)
            rm(self, "*.dylib", lib)

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "otterbrix")
        self.cpp_info.set_property("cmake_target_name", "otterbrix::otterbrix")
        self.cpp_info.libs = ["otterbrix"]
        if self.settings.os in ("Linux", "FreeBSD"):
            self.cpp_info.system_libs = ["dl", "pthread"]
