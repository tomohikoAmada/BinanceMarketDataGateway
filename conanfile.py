from conan import ConanFile
from conan.tools.cmake import CMakeConfigDeps, CMakeToolchain


class BinanceMarketDataGatewayRuntimeDependencies(ConanFile):
    name = "binance-market-data-gateway-runtime-deps"
    version = "0.1.0"
    package_type = "application"
    settings = "os", "arch", "compiler", "build_type"

    requires = (
        "binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006",
        "binance-market-data-projection/0.1.0#d95fa71d6dca8d931e72fbb5b74114a9",
        "boost/1.91.0",
        "nlohmann_json/3.12.0",
        "openssl/3.6.3",
    )

    default_options = {
        "binance-market-data-contracts-cpp/*:shared": False,
        "binance-market-data-projection/*:proto_adapter": True,
        "binance-market-data-projection/*:shared": False,
        "boost/*:header_only": True,
    }

    def generate(self):
        dependencies = CMakeConfigDeps(self)
        # The installed Contracts config asks CMake for the canonical Protobuf package name.
        dependencies.set_property("protobuf", "cmake_file_name", "Protobuf")
        dependencies.generate()

        # Keep Conan-generated presets inside the caller's output directory.
        toolchain = CMakeToolchain(self)
        toolchain.user_presets_path = False
        toolchain.generate()
