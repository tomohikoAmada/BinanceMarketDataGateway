from conan import ConanFile
from conan.tools.cmake import CMakeToolchain


class BinanceMarketDataGatewayG1Consumer(ConanFile):
    name = "binance-market-data-gateway-g1-consumer"
    version = "0.1.0"
    package_type = "application"
    settings = "os", "arch", "compiler", "build_type"

    requires = (
        "binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006",
        "binance-market-data-contracts-grpc-cpp/0.1.0#c3661aa735e1eb6450c1a93f635dbc20",
        "binance-market-data-projection/0.1.0#5c8ee9626b652fad1075fa44e480182b",
    )

    default_options = {
        "binance-market-data-contracts-cpp/*:shared": False,
        "binance-market-data-contracts-grpc-cpp/*:shared": False,
        "binance-market-data-projection/*:proto_adapter": True,
        "binance-market-data-projection/*:shared": False,
    }

    generators = "CMakeDeps"

    def generate(self):
        # Keep Conan-generated presets inside the caller's output directory.
        toolchain = CMakeToolchain(self)
        toolchain.user_presets_path = False
        toolchain.generate()
