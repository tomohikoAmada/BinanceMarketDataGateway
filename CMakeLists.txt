cmake_minimum_required(VERSION 3.24)

project(
    BinanceMarketDataGateway
    VERSION 0.1.0
    LANGUAGES CXX
)

include(CTest)

option(BMD_GATEWAY_BUILD_TESTS "Build the offline Gateway foundation tests" ${BUILD_TESTING})
option(BMD_GATEWAY_BUILD_G2_SYNTHETIC_HOST
       "Build the deterministic G2 synthetic Spot host and focused tests" OFF)
option(BMD_GATEWAY_BUILD_G3_RUNTIME
       "Build the serialized G3 MarketRuntime and focused tests" OFF)
option(BMD_GATEWAY_BUILD_G4_SPOT_TRANSPORT
       "Build the real G4 Binance Spot BTCUSDT transport and offline tests" OFF)
option(BMD_GATEWAY_BUILD_G5_RECOVERY
       "Build bounded G5 Spot reconnect/resync recovery and focused tests" OFF)
option(BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE
       "Build the opt-in exact candidate Contracts/Projection consumer link smoke" OFF)
option(BMD_GATEWAY_ENABLE_WARNINGS "Enable GCC/Clang warnings" ON)
option(BMD_GATEWAY_WARNINGS_AS_ERRORS "Treat project warnings as errors" ON)
option(BMD_GATEWAY_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(BMD_GATEWAY_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(BMD_GATEWAY_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(BMD_GATEWAY_BUILD_G4_SPOT_TRANSPORT)
    set(BMD_GATEWAY_BUILD_G3_RUNTIME ON)
endif()

if(BMD_GATEWAY_BUILD_G5_RECOVERY)
    set(BMD_GATEWAY_BUILD_G4_SPOT_TRANSPORT ON)
    set(BMD_GATEWAY_BUILD_G3_RUNTIME ON)
endif()

if(BMD_GATEWAY_ENABLE_ASAN AND BMD_GATEWAY_ENABLE_TSAN)
    message(FATAL_ERROR "AddressSanitizer and ThreadSanitizer cannot be enabled together")
endif()

function(bmd_gateway_apply_warnings target)
    if(NOT BMD_GATEWAY_ENABLE_WARNINGS)
        return()
    endif()

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(
            ${target}
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
                -Wconversion
                -Wsign-conversion
                -Wshadow
        )
        if(BMD_GATEWAY_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    elseif(MSVC)
        target_compile_options(${target} PRIVATE /W4)
        if(BMD_GATEWAY_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    endif()
endfunction()

function(bmd_gateway_apply_sanitizers target)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        if(BMD_GATEWAY_ENABLE_ASAN OR BMD_GATEWAY_ENABLE_UBSAN OR BMD_GATEWAY_ENABLE_TSAN)
            message(FATAL_ERROR "Sanitizers require GCC, Clang, or AppleClang")
        endif()
        return()
    endif()

    set(sanitizer_flags)
    if(BMD_GATEWAY_ENABLE_ASAN)
        list(APPEND sanitizer_flags -fsanitize=address -fno-omit-frame-pointer)
    endif()
    if(BMD_GATEWAY_ENABLE_UBSAN)
        list(APPEND sanitizer_flags -fsanitize=undefined -fno-omit-frame-pointer)
    endif()
    if(BMD_GATEWAY_ENABLE_TSAN)
        list(APPEND sanitizer_flags -fsanitize=thread -fno-omit-frame-pointer)
    endif()
    if(sanitizer_flags)
        target_compile_options(${target} PRIVATE ${sanitizer_flags})
        target_link_options(${target} PRIVATE ${sanitizer_flags})
    endif()
endfunction()

add_library(bmd_gateway_foundation
    src/config.cpp
    src/lifecycle.cpp
)
add_library(BinanceMarketDataGateway::Foundation ALIAS bmd_gateway_foundation)

set_target_properties(
    bmd_gateway_foundation
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO
        POSITION_INDEPENDENT_CODE YES
)
target_compile_features(bmd_gateway_foundation PUBLIC cxx_std_20)
target_include_directories(
    bmd_gateway_foundation
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
bmd_gateway_apply_warnings(bmd_gateway_foundation)
bmd_gateway_apply_sanitizers(bmd_gateway_foundation)

add_executable(bmd-gatewayd app/gatewayd/main.cpp)
set_target_properties(
    bmd-gatewayd
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO
)
target_link_libraries(bmd-gatewayd PRIVATE BinanceMarketDataGateway::Foundation)
bmd_gateway_apply_warnings(bmd-gatewayd)
bmd_gateway_apply_sanitizers(bmd-gatewayd)

if(BMD_GATEWAY_BUILD_TESTS)
    enable_testing()
    add_executable(bmd_gateway_foundation_tests tests/foundation_tests.cpp)
    set_target_properties(
        bmd_gateway_foundation_tests
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_link_libraries(bmd_gateway_foundation_tests PRIVATE BinanceMarketDataGateway::Foundation)
    bmd_gateway_apply_warnings(bmd_gateway_foundation_tests)
    bmd_gateway_apply_sanitizers(bmd_gateway_foundation_tests)
    add_test(NAME gateway_foundation_tests COMMAND bmd_gateway_foundation_tests)
endif()

if(BMD_GATEWAY_BUILD_G2_SYNTHETIC_HOST)
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)

    add_library(bmd_gateway_g2_synthetic
        src/g2/synthetic_spot_host.cpp
    )
    set_target_properties(
        bmd_gateway_g2_synthetic
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_include_directories(
        bmd_gateway_g2_synthetic
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/g2
    )
    target_link_libraries(
        bmd_gateway_g2_synthetic
        PUBLIC
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
            BinanceMarketDataProjection::ProtoAdapter
    )
    bmd_gateway_apply_warnings(bmd_gateway_g2_synthetic)
    bmd_gateway_apply_sanitizers(bmd_gateway_g2_synthetic)

    add_executable(bmd-gateway-g2-synthetic app/g2_synthetic/main.cpp)
    set_target_properties(
        bmd-gateway-g2-synthetic
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_link_libraries(bmd-gateway-g2-synthetic PRIVATE bmd_gateway_g2_synthetic)
    bmd_gateway_apply_warnings(bmd-gateway-g2-synthetic)
    bmd_gateway_apply_sanitizers(bmd-gateway-g2-synthetic)

    if(BMD_GATEWAY_BUILD_TESTS)
        add_executable(bmd_gateway_g2_synthetic_tests tests/g2_synthetic_host_tests.cpp)
        set_target_properties(
            bmd_gateway_g2_synthetic_tests
            PROPERTIES
                CXX_STANDARD 20
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
        )
        target_link_libraries(bmd_gateway_g2_synthetic_tests PRIVATE bmd_gateway_g2_synthetic)
        bmd_gateway_apply_warnings(bmd_gateway_g2_synthetic_tests)
        bmd_gateway_apply_sanitizers(bmd_gateway_g2_synthetic_tests)
        add_test(NAME gateway_g2_synthetic_host_tests COMMAND bmd_gateway_g2_synthetic_tests)
    endif()
endif()

if(BMD_GATEWAY_BUILD_G3_RUNTIME)
    find_package(Threads REQUIRED)
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)

    add_library(bmd_gateway_g3_runtime
        src/g3/market_runtime.cpp
    )
    set_target_properties(
        bmd_gateway_g3_runtime
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_include_directories(
        bmd_gateway_g3_runtime
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/g3
    )
    target_link_libraries(
        bmd_gateway_g3_runtime
        PUBLIC
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
            BinanceMarketDataProjection::ProtoAdapter
            Threads::Threads
    )
    bmd_gateway_apply_warnings(bmd_gateway_g3_runtime)
    bmd_gateway_apply_sanitizers(bmd_gateway_g3_runtime)

    if(BMD_GATEWAY_BUILD_TESTS)
        add_executable(bmd_gateway_g3_runtime_tests tests/g3_market_runtime_tests.cpp)
        set_target_properties(
            bmd_gateway_g3_runtime_tests
            PROPERTIES
                CXX_STANDARD 20
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
        )
        target_link_libraries(bmd_gateway_g3_runtime_tests PRIVATE bmd_gateway_g3_runtime)
        bmd_gateway_apply_warnings(bmd_gateway_g3_runtime_tests)
        bmd_gateway_apply_sanitizers(bmd_gateway_g3_runtime_tests)
        add_test(NAME gateway_g3_market_runtime_tests COMMAND bmd_gateway_g3_runtime_tests)
    endif()
endif()

if(BMD_GATEWAY_BUILD_G4_SPOT_TRANSPORT)
    find_package(Boost 1.91.0 EXACT CONFIG REQUIRED)
    find_package(OpenSSL 3.6.3 EXACT CONFIG REQUIRED)
    find_package(nlohmann_json 3.12.0 EXACT CONFIG REQUIRED)

    add_library(bmd_gateway_g4_spot_protocol
        src/g4/spot_protocol.cpp
    )
    set_target_properties(
        bmd_gateway_g4_spot_protocol
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_include_directories(
        bmd_gateway_g4_spot_protocol
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/g4
    )
    target_link_libraries(
        bmd_gateway_g4_spot_protocol
        PUBLIC
            bmd_gateway_g3_runtime
            nlohmann_json::nlohmann_json
    )
    bmd_gateway_apply_warnings(bmd_gateway_g4_spot_protocol)
    bmd_gateway_apply_sanitizers(bmd_gateway_g4_spot_protocol)

    add_library(bmd_gateway_g4_spot_transport
        src/g4/spot_transport.cpp
    )
    set_target_properties(
        bmd_gateway_g4_spot_transport
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_include_directories(
        bmd_gateway_g4_spot_transport
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/g4
    )
    target_link_libraries(
        bmd_gateway_g4_spot_transport
        PUBLIC
            bmd_gateway_g4_spot_protocol
            boost::boost
            openssl::openssl
    )
    bmd_gateway_apply_warnings(bmd_gateway_g4_spot_transport)
    bmd_gateway_apply_sanitizers(bmd_gateway_g4_spot_transport)

    add_executable(bmd-gateway-g4-spot-live app/g4_spot_live/main.cpp)
    set_target_properties(
        bmd-gateway-g4-spot-live
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_link_libraries(
        bmd-gateway-g4-spot-live
        PRIVATE
            bmd_gateway_g4_spot_transport
    )
    bmd_gateway_apply_warnings(bmd-gateway-g4-spot-live)
    bmd_gateway_apply_sanitizers(bmd-gateway-g4-spot-live)

    if(BMD_GATEWAY_BUILD_TESTS)
        add_executable(bmd_gateway_g4_spot_protocol_tests
            tests/g4_spot_protocol_tests.cpp
        )
        set_target_properties(
            bmd_gateway_g4_spot_protocol_tests
            PROPERTIES
                CXX_STANDARD 20
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
        )
        target_link_libraries(
            bmd_gateway_g4_spot_protocol_tests
            PRIVATE
                bmd_gateway_g4_spot_protocol
        )
        bmd_gateway_apply_warnings(bmd_gateway_g4_spot_protocol_tests)
        bmd_gateway_apply_sanitizers(bmd_gateway_g4_spot_protocol_tests)
        add_test(NAME gateway_g4_spot_protocol_tests
                 COMMAND bmd_gateway_g4_spot_protocol_tests)

        add_executable(bmd_gateway_g4_spot_transport_tests
            tests/g4_spot_transport_tests.cpp
        )
        set_target_properties(
            bmd_gateway_g4_spot_transport_tests
            PROPERTIES
                CXX_STANDARD 20
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
        )
        target_link_libraries(
            bmd_gateway_g4_spot_transport_tests
            PRIVATE
                bmd_gateway_g4_spot_transport
        )
        bmd_gateway_apply_warnings(bmd_gateway_g4_spot_transport_tests)
        bmd_gateway_apply_sanitizers(bmd_gateway_g4_spot_transport_tests)
        add_test(NAME gateway_g4_spot_transport_tests
                 COMMAND bmd_gateway_g4_spot_transport_tests)
    endif()
endif()

if(BMD_GATEWAY_BUILD_G5_RECOVERY)
    add_library(bmd_gateway_g5_recovery
        src/g5/spot_recovery.cpp
    )
    set_target_properties(
        bmd_gateway_g5_recovery
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_include_directories(
        bmd_gateway_g5_recovery
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/g5
    )
    target_link_libraries(
        bmd_gateway_g5_recovery
        PUBLIC
            bmd_gateway_g4_spot_transport
    )
    bmd_gateway_apply_warnings(bmd_gateway_g5_recovery)
    bmd_gateway_apply_sanitizers(bmd_gateway_g5_recovery)

    add_executable(bmd-gateway-g5-spot-recovery-live
        app/g5_spot_recovery_live/main.cpp
    )
    set_target_properties(
        bmd-gateway-g5-spot-recovery-live
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_link_libraries(
        bmd-gateway-g5-spot-recovery-live
        PRIVATE
            bmd_gateway_g5_recovery
    )
    bmd_gateway_apply_warnings(bmd-gateway-g5-spot-recovery-live)
    bmd_gateway_apply_sanitizers(bmd-gateway-g5-spot-recovery-live)

    if(BMD_GATEWAY_BUILD_TESTS)
        add_executable(bmd_gateway_g5_recovery_tests
            tests/g5_spot_recovery_tests.cpp
        )
        set_target_properties(
            bmd_gateway_g5_recovery_tests
            PROPERTIES
                CXX_STANDARD 20
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
        )
        target_link_libraries(
            bmd_gateway_g5_recovery_tests
            PRIVATE
                bmd_gateway_g5_recovery
        )
        bmd_gateway_apply_warnings(bmd_gateway_g5_recovery_tests)
        bmd_gateway_apply_sanitizers(bmd_gateway_g5_recovery_tests)
        add_test(NAME gateway_g5_recovery_tests
                 COMMAND bmd_gateway_g5_recovery_tests)
    endif()
endif()

if(BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE)
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataContractsGrpc 0.1.0 EXACT CONFIG REQUIRED)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)

    foreach(bmd_gateway_package IN ITEMS
            BinanceMarketDataContracts
            BinanceMarketDataContractsGrpc
            BinanceMarketDataProjection)
        if(NOT DEFINED ${bmd_gateway_package}_DIR)
            message(FATAL_ERROR "G1 package directory was not resolved: ${bmd_gateway_package}_DIR")
        endif()
        set(bmd_gateway_config
            "${${bmd_gateway_package}_DIR}/${bmd_gateway_package}Config.cmake")
        if(NOT EXISTS "${bmd_gateway_config}")
            message(FATAL_ERROR "G1 canonical package config is missing: ${bmd_gateway_config}")
        endif()
        message(STATUS "G1 canonical config: ${bmd_gateway_package}=${bmd_gateway_config}")
    endforeach()

    add_executable(bmd_gateway_upstream_link_smoke tests/upstream_link_smoke.cpp)
    set_target_properties(
        bmd_gateway_upstream_link_smoke
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_link_libraries(
        bmd_gateway_upstream_link_smoke
        PRIVATE
            BinanceMarketDataGateway::Foundation
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataContracts::Grpc
            BinanceMarketDataProjection::Core
            BinanceMarketDataProjection::ProtoAdapter
    )
    bmd_gateway_apply_warnings(bmd_gateway_upstream_link_smoke)
    bmd_gateway_apply_sanitizers(bmd_gateway_upstream_link_smoke)
    if(NOT BMD_GATEWAY_BUILD_TESTS)
        enable_testing()
    endif()
    add_test(NAME gateway_upstream_link_smoke COMMAND bmd_gateway_upstream_link_smoke)
endif()
