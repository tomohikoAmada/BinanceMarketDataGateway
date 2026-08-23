cmake_minimum_required(VERSION 3.24)

project(
    BinanceMarketDataGateway
    VERSION 0.1.0
    LANGUAGES CXX
)

include(CTest)

option(BMD_GATEWAY_BUILD_TESTS "Build the offline Gateway foundation tests" ${BUILD_TESTING})
option(BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE
       "Build the opt-in Contracts/Projection consumer link smoke" OFF)
option(BMD_GATEWAY_ENABLE_WARNINGS "Enable GCC/Clang warnings" ON)
option(BMD_GATEWAY_WARNINGS_AS_ERRORS "Treat project warnings as errors" ON)
option(BMD_GATEWAY_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(BMD_GATEWAY_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(BMD_GATEWAY_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

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

if(BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE)
    set(BMD_GATEWAY_UPSTREAM_CMAKE_PREFIX_PATH "" CACHE STRING
        "Semicolon-separated staged prefixes for the exact Contracts/Projection packages")
    if(NOT BMD_GATEWAY_UPSTREAM_CMAKE_PREFIX_PATH)
        message(FATAL_ERROR
            "Link smoke requires an explicit staged package prefix; final package pins remain "
            "pending Contracts #14 and Projection #45")
    endif()

    list(PREPEND CMAKE_PREFIX_PATH ${BMD_GATEWAY_UPSTREAM_CMAKE_PREFIX_PATH})
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataContractsGrpc 0.1.0 EXACT CONFIG REQUIRED)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)

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
