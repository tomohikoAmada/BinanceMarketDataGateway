from typing import ClassVar

from conan import ConanFile
from conan.tools.cmake import CMakeConfigDeps, CMakeToolchain


class BinanceMarketDataGatewayRuntimeDependencies(ConanFile):
    name = "binance-market-data-gateway-runtime-deps"
    version = "0.1.0"
    package_type = "application"
    settings = "os", "arch", "compiler", "build_type"
    options: ClassVar[dict[str, list[bool]]] = {"with_g7_grpc": [True, False]}
    default_options: ClassVar[dict[str, bool]] = {
        "with_g7_grpc": False,
        "binance-market-data-contracts-cpp/*:shared": False,
        "binance-market-data-contracts-grpc-cpp/*:shared": False,
        "binance-market-data-projection/*:proto_adapter": True,
        "binance-market-data-projection/*:shared": False,
        "boost/*:header_only": True,
    }

    def requirements(self):
        self.requires(
            "binance-market-data-contracts-cpp/0.1.0#9739b82137e9d6437ef66bbbf81a9ddd"
        )
        self.requires(
            "binance-market-data-projection/0.1.0#04cc16f91ddd2d214a1dd0efccac0eb3"
        )
        self.requires("boost/1.91.0")
        self.requires("nlohmann_json/3.12.0")
        self.requires("openssl/3.6.3")
        if self.options.with_g7_grpc:
            self.requires(
                "binance-market-data-contracts-grpc-cpp/0.1.0#40c3986aa8e13d2f8ebb137adfea931d"
            )

    def generate(self):
        dependencies = CMakeConfigDeps(self)
        # The installed Contracts config asks CMake for the canonical Protobuf package name.
        dependencies.set_property("protobuf", "cmake_file_name", "Protobuf")
        dependencies.generate()

        # Keep Conan-generated presets inside the caller's output directory.
        toolchain = CMakeToolchain(self)
        toolchain.user_presets_path = False
        toolchain.generate()
