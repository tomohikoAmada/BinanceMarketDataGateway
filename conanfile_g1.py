from conan import ConanFile
from conan.tools.cmake import CMakeConfigDeps, CMakeToolchain


class BinanceMarketDataGatewayG1Consumer(ConanFile):
    name = "binance-market-data-gateway-g1-consumer"
    version = "0.1.0"
    package_type = "application"
    settings = "os", "arch", "compiler", "build_type"

    requires = (
        "binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006",
        "binance-market-data-contracts-grpc-cpp/0.1.0#c3661aa735e1eb6450c1a93f635dbc20",
        "binance-market-data-projection/0.1.0#58ab66309450bdc2035b3e5935220348",
    )

    default_options = {
        "binance-market-data-contracts-cpp/*:shared": False,
        "binance-market-data-contracts-grpc-cpp/*:shared": False,
        "binance-market-data-projection/*:proto_adapter": True,
        "binance-market-data-projection/*:shared": False,
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
