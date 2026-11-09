cmake_minimum_required(VERSION 3.24)

project(
    BinanceMarketDataGateway
    VERSION 0.1.0
    LANGUAGES CXX
)

include(CTest)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

option(BMD_GATEWAY_BUILD_TESTS
       "Build offline tests (Foundation always; domain tests require the pinned graph)"
       ${BUILD_TESTING})
option(BMD_GATEWAY_BUILD_DOMAIN_TESTS
       "Build domain tests that require pinned Contracts/Projection/Boost/OpenSSL"
       OFF)
option(BMD_GATEWAY_BUILD_PRODUCTION_DAEMON
       "Build the configured-product production bmd-gatewayd"
       OFF)
option(BMD_GATEWAY_BUILD_ACCEPTANCE_TOOLS
       "Build historical live/acceptance executables and the synthetic Spot host"
       OFF)
option(BMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION
       "Build the opt-in performance trace/delivery instrumentation"
       OFF)
option(BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE
       "Build the opt-in exact Contracts/Projection consumer link smoke"
       OFF)
option(BMD_GATEWAY_ENABLE_WARNINGS "Enable GCC/Clang warnings" ON)
option(BMD_GATEWAY_WARNINGS_AS_ERRORS "Treat project warnings as errors" ON)
option(BMD_GATEWAY_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(BMD_GATEWAY_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(BMD_GATEWAY_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(BMD_GATEWAY_ENABLE_ASAN AND BMD_GATEWAY_ENABLE_TSAN)
    message(FATAL_ERROR "AddressSanitizer and ThreadSanitizer cannot be enabled together")
endif()

if(BMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION AND
   NOT BMD_GATEWAY_BUILD_PRODUCTION_DAEMON)
    message(FATAL_ERROR
            "Performance baseline instrumentation requires the production daemon graph")
endif()

# A production or acceptance build implies the full domain graph and, when
# tests are enabled, the domain tests. Foundation remains independently
# buildable with no Conan packages.
if(BMD_GATEWAY_BUILD_PRODUCTION_DAEMON OR BMD_GATEWAY_BUILD_ACCEPTANCE_TOOLS)
    set(BMD_GATEWAY_BUILD_DOMAIN_GRAPH ON)
endif()
if(BMD_GATEWAY_BUILD_DOMAIN_TESTS)
    set(BMD_GATEWAY_BUILD_DOMAIN_GRAPH ON)
endif()
if(BMD_GATEWAY_BUILD_TESTS AND BMD_GATEWAY_BUILD_DOMAIN_GRAPH)
    set(BMD_GATEWAY_BUILD_DOMAIN_TESTS ON)
endif()

# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

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

function(bmd_gateway_configure_target target)
    set_target_properties(
        ${target}
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_compile_features(${target} PRIVATE cxx_std_20)
    bmd_gateway_apply_warnings(${target})
    bmd_gateway_apply_sanitizers(${target})
endfunction()

function(bmd_gateway_configure_library target)
    bmd_gateway_configure_target(${target})
    target_compile_features(${target} PUBLIC cxx_std_20)
    target_include_directories(
        ${target}
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    )
endfunction()

function(bmd_gateway_add_test name source)
    add_executable(${name} ${source})
    target_include_directories(
        ${name}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/app
    )
    target_link_libraries(${name} PRIVATE ${ARGN})
    bmd_gateway_configure_target(${name})
    add_test(NAME ${name} COMMAND ${name})
endfunction()

function(bmd_gateway_add_acceptance name source)
    add_executable(${name} ${source})
    target_include_directories(
        ${name}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/app
    )
    target_link_libraries(${name} PRIVATE ${ARGN})
    bmd_gateway_configure_target(${name})
endfunction()

# ---------------------------------------------------------------------------
# Foundation: no network, no threads, no third-party runtime dependency.
# ---------------------------------------------------------------------------

add_library(bmd_gateway_foundation
    src/foundation/config.cpp
    src/foundation/lifecycle.cpp
)
add_library(BinanceMarketDataGateway::Foundation ALIAS bmd_gateway_foundation)
bmd_gateway_configure_library(bmd_gateway_foundation)
target_include_directories(
    bmd_gateway_foundation
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

if(BMD_GATEWAY_BUILD_TESTS)
    enable_testing()
    bmd_gateway_add_test(
        bmd_gateway_foundation_tests
        tests/foundation/foundation_tests.cpp
        BinanceMarketDataGateway::Foundation
    )
endif()

# ---------------------------------------------------------------------------
# Domain graph: protocol -> publication -> runtime -> transport -> recovery
#               -> composition -> status/grpc -> production
# ---------------------------------------------------------------------------

if(BMD_GATEWAY_BUILD_DOMAIN_GRAPH)
    find_package(Threads REQUIRED)
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)
    find_package(Boost 1.91.0 EXACT CONFIG REQUIRED)
    find_package(OpenSSL 3.6.3 EXACT CONFIG REQUIRED)
    find_package(nlohmann_json 3.12.0 EXACT CONFIG REQUIRED)
    find_package(BinanceMarketDataContractsGrpc 0.1.0 EXACT CONFIG REQUIRED)

    if(BMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION)
        add_compile_definitions(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED=1)
    endif()

    # The domain graph is the complete V1 surface. These macros only preserve
    # the historical compile-time split inside the gRPC/status sources.
    add_compile_definitions(
        BMD_GATEWAY_G9_ENABLED=1
        BMD_GATEWAY_G10_ENABLED=1
        BMD_GATEWAY_G11_ENABLED=1
    )

    add_library(bmd_gateway_protocol
        src/protocol/spot_protocol.cpp
        src/protocol/usdm_protocol.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_protocol)
    target_link_libraries(
        bmd_gateway_protocol
        PUBLIC
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
            nlohmann_json::nlohmann_json
    )

    add_library(bmd_gateway_publication
        src/publication/order_book_publication.cpp
        src/publication/event_publication.cpp
    )
    if(BMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION)
        target_sources(
            bmd_gateway_publication
            PRIVATE
                src/performance/performance_baseline.cpp
        )
    endif()
    bmd_gateway_configure_library(bmd_gateway_publication)
    target_link_libraries(
        bmd_gateway_publication
        PUBLIC
            bmd_gateway_protocol
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
    )

    add_library(bmd_gateway_runtime
        src/runtime/market_runtime.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_runtime)
    target_link_libraries(
        bmd_gateway_runtime
        PUBLIC
            bmd_gateway_publication
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
            BinanceMarketDataProjection::ProtoAdapter
            Threads::Threads
    )

    add_library(bmd_gateway_transport
        src/transport/spot_transport.cpp
        src/transport/usdm_transport.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_transport)
    target_link_libraries(
        bmd_gateway_transport
        PUBLIC
            bmd_gateway_runtime
            bmd_gateway_protocol
            boost::boost
            openssl::openssl
    )

    add_library(bmd_gateway_recovery
        src/recovery/spot_recovery.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_recovery)
    target_link_libraries(
        bmd_gateway_recovery
        PUBLIC
            bmd_gateway_transport
    )

    add_library(bmd_gateway_composition
        src/composition/market_registry.cpp
        src/composition/multi_market_runtime.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_composition)
    target_link_libraries(
        bmd_gateway_composition
        PUBLIC
            bmd_gateway_runtime
            bmd_gateway_transport
            bmd_gateway_recovery
            bmd_gateway_publication
    )

    add_library(bmd_gateway_status
        src/status/gateway_status.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_status)
    target_link_libraries(
        bmd_gateway_status
        PUBLIC
            bmd_gateway_composition
            bmd_gateway_runtime
            bmd_gateway_recovery
            bmd_gateway_publication
    )

    add_library(bmd_gateway_grpc
        src/grpc/grpc_service.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_grpc)
    target_link_libraries(
        bmd_gateway_grpc
        PUBLIC
            bmd_gateway_status
            bmd_gateway_composition
            bmd_gateway_runtime
            bmd_gateway_recovery
            bmd_gateway_publication
            BinanceMarketDataContracts::Grpc
    )

    add_library(bmd_gateway_production
        src/production/daemon_config.cpp
        src/production/daemon_runtime.cpp
        src/production/production_gateway.cpp
        src/production/production_metadata.cpp
        src/production/termination_signals.cpp
    )
    bmd_gateway_configure_library(bmd_gateway_production)
    target_link_libraries(
        bmd_gateway_production
        PUBLIC
            BinanceMarketDataGateway::Foundation
            bmd_gateway_grpc
            bmd_gateway_composition
            bmd_gateway_transport
            bmd_gateway_protocol
            nlohmann_json::nlohmann_json
            Threads::Threads
    )

    if(BMD_GATEWAY_BUILD_PRODUCTION_DAEMON)
        add_executable(bmd-gatewayd app/gatewayd/main.cpp)
        target_link_libraries(bmd-gatewayd PRIVATE bmd_gateway_production)
        bmd_gateway_configure_target(bmd-gatewayd)
        install(TARGETS bmd-gatewayd RUNTIME DESTINATION bin)
    endif()

    # -----------------------------------------------------------------------
    # Test support and domain tests
    # -----------------------------------------------------------------------

    if(BMD_GATEWAY_BUILD_DOMAIN_TESTS OR BMD_GATEWAY_BUILD_ACCEPTANCE_TOOLS)
        add_library(bmd_gateway_synthetic_host
            tests/support/synthetic_spot_host.cpp
        )
        bmd_gateway_configure_library(bmd_gateway_synthetic_host)
        target_include_directories(
            bmd_gateway_synthetic_host
            PUBLIC
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
        )
        target_link_libraries(
            bmd_gateway_synthetic_host
            PUBLIC
                BinanceMarketDataContracts::Protobuf
                BinanceMarketDataProjection::Core
                BinanceMarketDataProjection::ProtoAdapter
        )
    endif()

    if(BMD_GATEWAY_BUILD_DOMAIN_TESTS)
        bmd_gateway_add_test(
            bmd_gateway_synthetic_host_tests
            tests/support/synthetic_spot_host_tests.cpp
            bmd_gateway_synthetic_host
        )

        bmd_gateway_add_test(
            bmd_gateway_protocol_tests
            tests/protocol/spot_protocol_tests.cpp
            bmd_gateway_protocol
        )

        bmd_gateway_add_test(
            bmd_gateway_usdm_protocol_tests
            tests/protocol/usdm_protocol_tests.cpp
            bmd_gateway_runtime
        )
        target_compile_definitions(
            bmd_gateway_usdm_protocol_tests
            PRIVATE
                BMD_GATEWAY_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
        )

        bmd_gateway_add_test(
            bmd_gateway_public_api_budget_tests
            tests/transport/public_api_budget_tests.cpp
            Threads::Threads
        )
        target_include_directories(
            bmd_gateway_public_api_budget_tests
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/src
        )

        bmd_gateway_add_test(
            bmd_gateway_spot_transport_tests
            tests/transport/spot_transport_tests.cpp
            bmd_gateway_transport
        )
        bmd_gateway_add_test(
            bmd_gateway_usdm_transport_tests
            tests/transport/usdm_transport_tests.cpp
            bmd_gateway_transport
        )

        bmd_gateway_add_test(
            bmd_gateway_market_runtime_tests
            tests/runtime/market_runtime_tests.cpp
            bmd_gateway_runtime
        )

        bmd_gateway_add_test(
            bmd_gateway_composition_tests
            tests/composition/multi_market_runtime_tests.cpp
            bmd_gateway_composition
        )

        bmd_gateway_add_test(
            bmd_gateway_recovery_tests
            tests/recovery/spot_recovery_tests.cpp
            bmd_gateway_recovery
        )
        bmd_gateway_add_test(
            bmd_gateway_rotation_tests
            tests/recovery/spot_rotation_tests.cpp
            bmd_gateway_recovery
        )

        bmd_gateway_add_test(
            bmd_gateway_publication_tests
            tests/publication/order_book_publication_tests.cpp
            bmd_gateway_runtime
        )
        bmd_gateway_add_test(
            bmd_gateway_event_publication_tests
            tests/publication/event_publication_tests.cpp
            bmd_gateway_publication
        )

        bmd_gateway_add_test(
            bmd_gateway_grpc_service_tests
            tests/grpc/grpc_service_tests.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_test(
            bmd_gateway_event_service_tests
            tests/grpc/event_service_tests.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_test(
            bmd_gateway_status_service_tests
            tests/grpc/status_service_tests.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_test(
            bmd_gateway_routing_tests
            tests/grpc/routing_tests.cpp
            bmd_gateway_grpc
        )

        bmd_gateway_add_test(
            bmd_gateway_status_tests
            tests/status/gateway_status_tests.cpp
            bmd_gateway_status
        )

        bmd_gateway_add_test(
            bmd_gateway_projection_integration_tests
            tests/integration/projection_integration_tests.cpp
            bmd_gateway_production
        )

        bmd_gateway_add_test(
            bmd_gateway_four_product_integration_tests
            tests/integration/four_product_integration_tests.cpp
            bmd_gateway_production
        )
        target_compile_definitions(
            bmd_gateway_four_product_integration_tests
            PRIVATE
                BMD_GATEWAY_FOUR_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway.json"
        )

        bmd_gateway_add_test(
            bmd_gateway_twenty_product_integration_tests
            tests/integration/four_product_integration_tests.cpp
            bmd_gateway_production
        )
        target_compile_definitions(
            bmd_gateway_twenty_product_integration_tests
            PRIVATE
                BMD_GATEWAY_TWENTY_INTEGRATION=1
                BMD_GATEWAY_TWENTY_PROFILE="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/projection-p1-twenty.json"
                BMD_GATEWAY_TWENTY_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway-twenty.json"
        )

        bmd_gateway_add_test(
            bmd_gateway_production_tests
            tests/production/production_gateway_tests.cpp
            bmd_gateway_production
        )
        target_compile_definitions(
            bmd_gateway_production_tests
            PRIVATE
                BMD_GATEWAY_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
        )

        bmd_gateway_add_test(
            bmd_gateway_production_config_metadata_tests
            tests/production/production_config_metadata_tests.cpp
            bmd_gateway_production
        )

        bmd_gateway_add_test(
            bmd_gateway_production_acceptance_config_tests
            tests/production/production_acceptance_config_tests.cpp
            bmd_gateway_production
        )

        add_executable(
            bmd_gateway_projection_p1_measurements
            tests/performance/projection_p1_measurements.cpp
        )
        target_include_directories(
            bmd_gateway_projection_p1_measurements
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/src
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
                ${CMAKE_CURRENT_SOURCE_DIR}/app
        )
        target_link_libraries(
            bmd_gateway_projection_p1_measurements
            PRIVATE
                bmd_gateway_production
        )
        bmd_gateway_configure_target(bmd_gateway_projection_p1_measurements)
        target_compile_definitions(
            bmd_gateway_projection_p1_measurements
            PRIVATE
                BMD_GATEWAY_FOUR_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway.json"
                BMD_GATEWAY_TWENTY_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway-twenty.json"
        )
        add_test(
            NAME bmd_gateway_projection_p1_smoke
            COMMAND bmd_gateway_projection_p1_measurements
                    8 1 8 1000 "${CMAKE_CURRENT_BINARY_DIR}/p1-smoke.json"
        )
        set_tests_properties(bmd_gateway_projection_p1_smoke PROPERTIES TIMEOUT 30)
        add_test(
            NAME bmd_gateway_projection_p1_twenty_smoke
            COMMAND bmd_gateway_projection_p1_measurements
                    --profile "${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/projection-p1-twenty.json"
                    smoke "${CMAKE_CURRENT_BINARY_DIR}/p1-twenty-smoke.json"
        )
        set_tests_properties(bmd_gateway_projection_p1_twenty_smoke PROPERTIES TIMEOUT 60)

        if(BMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION)
            bmd_gateway_add_test(
                bmd_gateway_performance_baseline_tests
                tests/performance/performance_baseline_tests.cpp
                bmd_gateway_production
            )
        endif()

        add_executable(
            bmd_gateway_production_daemon_fixture
            tests/production/production_daemon_fixture.cpp
        )
        target_include_directories(
            bmd_gateway_production_daemon_fixture
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/src
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
        )
        target_link_libraries(
            bmd_gateway_production_daemon_fixture
            PRIVATE
                bmd_gateway_production
        )
        bmd_gateway_configure_target(bmd_gateway_production_daemon_fixture)

        if(BMD_GATEWAY_BUILD_PRODUCTION_DAEMON)
            add_executable(
                bmd_gateway_production_daemon_process_tests
                tests/production/production_daemon_process_tests.cpp
            )
            target_include_directories(
                bmd_gateway_production_daemon_process_tests
                PRIVATE
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests
                    ${CMAKE_CURRENT_SOURCE_DIR}/app
            )
            target_link_libraries(
                bmd_gateway_production_daemon_process_tests
                PRIVATE
                    bmd_gateway_production
            )
            bmd_gateway_configure_target(
                bmd_gateway_production_daemon_process_tests)
            add_test(
                NAME bmd_gateway_production_daemon_process
                COMMAND bmd_gateway_production_daemon_process_tests
                        $<TARGET_FILE:bmd-gatewayd>
                        $<TARGET_FILE:bmd_gateway_production_daemon_fixture>
            )
            set_tests_properties(
                bmd_gateway_production_daemon_process
                PROPERTIES TIMEOUT 45)
        endif()
    endif()

    # -----------------------------------------------------------------------
    # Historical live/acceptance tools. They are intentionally outside the
    # production daemon target and are only built on request.
    # -----------------------------------------------------------------------

    if(BMD_GATEWAY_BUILD_ACCEPTANCE_TOOLS)
        bmd_gateway_add_acceptance(
            bmd-gateway-synthetic-spot-host
            app/acceptance/synthetic_spot_host/main.cpp
            bmd_gateway_synthetic_host
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-spot-live
            app/acceptance/spot_live/main.cpp
            bmd_gateway_transport
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-spot-recovery-live
            app/acceptance/spot_recovery_live/main.cpp
            bmd_gateway_recovery
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-spot-rotation-live
            app/acceptance/spot_rotation_live/main.cpp
            bmd_gateway_recovery
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-spot-grpc-live
            app/acceptance/spot_grpc_live/main.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-integration-live
            app/acceptance/integration_live/main.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-spot-events-live
            app/acceptance/spot_events_live/main.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-multi-market-live
            app/acceptance/multi_market_live/main.cpp
            bmd_gateway_grpc
        )
        bmd_gateway_add_acceptance(
            bmd-gateway-production-acceptance-client
            app/acceptance/production_daemon/main.cpp
            bmd_gateway_production
        )
    endif()
endif()

# ---------------------------------------------------------------------------
# Opt-in upstream link smoke (frozen dependency boundary proof).
# ---------------------------------------------------------------------------

if(BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE)
    if(NOT BMD_GATEWAY_BUILD_TESTS)
        enable_testing()
    endif()
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataContractsGrpc 0.1.0 EXACT CONFIG REQUIRED)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)

    foreach(bmd_gateway_package IN ITEMS
            BinanceMarketDataContracts
            BinanceMarketDataContractsGrpc
            BinanceMarketDataProjection)
        if(NOT DEFINED ${bmd_gateway_package}_DIR)
            message(FATAL_ERROR "Upstream package directory was not resolved: ${bmd_gateway_package}_DIR")
        endif()
        set(bmd_gateway_config
            "${${bmd_gateway_package}_DIR}/${bmd_gateway_package}Config.cmake")
        if(NOT EXISTS "${bmd_gateway_config}")
            message(FATAL_ERROR "Upstream canonical package config is missing: ${bmd_gateway_config}")
        endif()
        message(STATUS "Upstream canonical config: ${bmd_gateway_package}=${bmd_gateway_config}")
    endforeach()

    bmd_gateway_add_test(
        bmd_gateway_upstream_link_smoke
        tests/smoke/upstream_link_smoke.cpp
        BinanceMarketDataGateway::Foundation
        BinanceMarketDataContracts::Protobuf
        BinanceMarketDataContracts::Grpc
        BinanceMarketDataProjection::Core
        BinanceMarketDataProjection::ProtoAdapter
    )
endif()
