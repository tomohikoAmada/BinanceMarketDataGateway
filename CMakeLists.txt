cmake_minimum_required(VERSION 3.24)

project(
    BMDG
    VERSION 0.1.0
    LANGUAGES CXX
)

include(CTest)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

# New BMDG_* options. The old BMD_GATEWAY_* names remain accepted as
# deprecated aliases for one transition period.
function(bmdg_option name legacy default description)
    if(DEFINED ${legacy} AND NOT DEFINED ${name})
        set(${name} "${${legacy}}" CACHE BOOL
            "${description} (deprecated alias: ${legacy})" FORCE)
        message(DEPRECATION
                "${legacy} is deprecated; use ${name}")
    endif()
    option(${name} "${description}" ${default})
endfunction()

bmdg_option(BMDG_BUILD_TESTS BMD_GATEWAY_BUILD_TESTS "${BUILD_TESTING}"
            "Build offline tests (Foundation always; domain tests require the pinned graph)")
bmdg_option(BMDG_BUILD_DOMAIN_TESTS BMD_GATEWAY_BUILD_DOMAIN_TESTS OFF
            "Build domain tests that require pinned Contracts/Projection/Boost/OpenSSL")
bmdg_option(BMDG_BUILD_PRODUCTION_DAEMON BMD_GATEWAY_BUILD_PRODUCTION_DAEMON OFF
            "Build the configured-product production bmdg")
bmdg_option(BMDG_BUILD_ACCEPTANCE_TOOLS BMD_GATEWAY_BUILD_ACCEPTANCE_TOOLS OFF
            "Build historical live/acceptance executables and the synthetic Spot host")
bmdg_option(BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION BMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION OFF
            "Build the opt-in performance trace/delivery instrumentation")
bmdg_option(BMDG_BUILD_UPSTREAM_LINK_SMOKE BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE OFF
            "Build the opt-in exact Contracts/Projection consumer link smoke")
bmdg_option(BMDG_ENABLE_WARNINGS BMD_GATEWAY_ENABLE_WARNINGS ON
            "Enable GCC/Clang warnings")
bmdg_option(BMDG_WARNINGS_AS_ERRORS BMD_GATEWAY_WARNINGS_AS_ERRORS ON
            "Treat project warnings as errors")
bmdg_option(BMDG_ENABLE_ASAN BMD_GATEWAY_ENABLE_ASAN OFF
            "Enable AddressSanitizer")
bmdg_option(BMDG_ENABLE_UBSAN BMD_GATEWAY_ENABLE_UBSAN OFF
            "Enable UndefinedBehaviorSanitizer")
bmdg_option(BMDG_ENABLE_TSAN BMD_GATEWAY_ENABLE_TSAN OFF
            "Enable ThreadSanitizer")

if(BMDG_ENABLE_ASAN AND BMDG_ENABLE_TSAN)
    message(FATAL_ERROR "AddressSanitizer and ThreadSanitizer cannot be enabled together")
endif()

if(BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION AND
   NOT BMDG_BUILD_PRODUCTION_DAEMON)
    message(FATAL_ERROR
            "Performance baseline instrumentation requires the production daemon graph")
endif()

# A production or acceptance build implies the full domain graph and, when
# tests are enabled, the domain tests. Foundation remains independently
# buildable with no Conan packages.
if(BMDG_BUILD_PRODUCTION_DAEMON OR BMDG_BUILD_ACCEPTANCE_TOOLS)
    set(BMDG_BUILD_DOMAIN_GRAPH ON)
endif()
if(BMDG_BUILD_DOMAIN_TESTS)
    set(BMDG_BUILD_DOMAIN_GRAPH ON)
endif()
if(BMDG_BUILD_TESTS AND BMDG_BUILD_DOMAIN_GRAPH)
    set(BMDG_BUILD_DOMAIN_TESTS ON)
endif()

# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

function(bmdg_apply_warnings target)
    if(NOT BMDG_ENABLE_WARNINGS)
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
        if(BMDG_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    elseif(MSVC)
        target_compile_options(${target} PRIVATE /W4)
        if(BMDG_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    endif()
endfunction()

function(bmdg_apply_sanitizers target)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        if(BMDG_ENABLE_ASAN OR BMDG_ENABLE_UBSAN OR BMDG_ENABLE_TSAN)
            message(FATAL_ERROR "Sanitizers require GCC, Clang, or AppleClang")
        endif()
        return()
    endif()

    set(sanitizer_flags)
    if(BMDG_ENABLE_ASAN)
        list(APPEND sanitizer_flags -fsanitize=address -fno-omit-frame-pointer)
    endif()
    if(BMDG_ENABLE_UBSAN)
        list(APPEND sanitizer_flags -fsanitize=undefined -fno-omit-frame-pointer)
    endif()
    if(BMDG_ENABLE_TSAN)
        list(APPEND sanitizer_flags -fsanitize=thread -fno-omit-frame-pointer)
    endif()
    if(sanitizer_flags)
        target_compile_options(${target} PRIVATE ${sanitizer_flags})
        target_link_options(${target} PRIVATE ${sanitizer_flags})
    endif()
endfunction()

function(bmdg_configure_target target)
    set_target_properties(
        ${target}
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
    )
    target_compile_features(${target} PRIVATE cxx_std_20)
    bmdg_apply_warnings(${target})
    bmdg_apply_sanitizers(${target})
endfunction()

function(bmdg_configure_library target)
    bmdg_configure_target(${target})
    target_compile_features(${target} PUBLIC cxx_std_20)
    target_include_directories(
        ${target}
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    )
endfunction()

function(bmdg_add_test name source)
    add_executable(${name} ${source})
    target_include_directories(
        ${name}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/app
    )
    target_link_libraries(${name} PRIVATE ${ARGN})
    bmdg_configure_target(${name})
    add_test(NAME ${name} COMMAND ${name})
endfunction()

function(bmdg_add_acceptance name source)
    add_executable(${name} ${source})
    target_include_directories(
        ${name}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/app
    )
    target_link_libraries(${name} PRIVATE ${ARGN})
    bmdg_configure_target(${name})
endfunction()

# ---------------------------------------------------------------------------
# Foundation: no network, no threads, no third-party runtime dependency.
# ---------------------------------------------------------------------------

add_library(bmdg_foundation
    src/foundation/config.cpp
    src/foundation/lifecycle.cpp
)
add_library(BMDG::Foundation ALIAS bmdg_foundation)
add_library(BinanceMarketDataGateway::Foundation ALIAS bmdg_foundation)
add_library(bmd_gateway_foundation ALIAS bmdg_foundation)
bmdg_configure_library(bmdg_foundation)
target_include_directories(
    bmdg_foundation
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

if(BMDG_BUILD_TESTS)
    enable_testing()
    bmdg_add_test(
        bmdg_foundation_tests
        tests/foundation/foundation_tests.cpp
        BinanceMarketDataGateway::Foundation
    )
endif()

# ---------------------------------------------------------------------------
# Domain graph: protocol -> publication -> runtime -> transport -> recovery
#               -> composition -> status/grpc -> production
# ---------------------------------------------------------------------------

if(BMDG_BUILD_DOMAIN_GRAPH)
    find_package(Threads REQUIRED)
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)
    find_package(Boost 1.91.0 EXACT CONFIG REQUIRED)
    find_package(OpenSSL 3.6.3 EXACT CONFIG REQUIRED)
    find_package(nlohmann_json 3.12.0 EXACT CONFIG REQUIRED)
    find_package(BinanceMarketDataContractsGrpc 0.1.0 EXACT CONFIG REQUIRED)

    if(BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION)
        add_compile_definitions(BMDG_PERFORMANCE_BASELINE_ENABLED=1)
    endif()

    # The domain graph is the complete V1 surface. These macros only preserve
    # the historical compile-time split inside the gRPC/status sources.
    add_compile_definitions(
        BMDG_G9_ENABLED=1
        BMDG_G10_ENABLED=1
        BMDG_G11_ENABLED=1
    )

    add_library(bmdg_protocol
        src/protocol/spot_protocol.cpp
        src/protocol/usdm_protocol.cpp
    )
    bmdg_configure_library(bmdg_protocol)
    target_link_libraries(
        bmdg_protocol
        PUBLIC
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
            nlohmann_json::nlohmann_json
    )

    add_library(bmdg_publication
        src/publication/order_book_publication.cpp
        src/publication/event_publication.cpp
    )
    if(BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION)
        target_sources(
            bmdg_publication
            PRIVATE
                src/performance/performance_baseline.cpp
        )
    endif()
    bmdg_configure_library(bmdg_publication)
    target_link_libraries(
        bmdg_publication
        PUBLIC
            bmdg_protocol
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
    )

    add_library(bmdg_runtime
        src/runtime/market_runtime.cpp
    )
    bmdg_configure_library(bmdg_runtime)
    target_link_libraries(
        bmdg_runtime
        PUBLIC
            bmdg_publication
            BinanceMarketDataContracts::Protobuf
            BinanceMarketDataProjection::Core
            BinanceMarketDataProjection::ProtoAdapter
            Threads::Threads
    )

    add_library(bmdg_transport
        src/transport/spot_transport.cpp
        src/transport/usdm_transport.cpp
        src/transport/usdm_reference.cpp
    )
    bmdg_configure_library(bmdg_transport)
    target_link_libraries(
        bmdg_transport
        PUBLIC
            bmdg_runtime
            bmdg_protocol
            boost::boost
            openssl::openssl
    )

    add_library(bmdg_recovery
        src/recovery/spot_recovery.cpp
    )
    bmdg_configure_library(bmdg_recovery)
    target_link_libraries(
        bmdg_recovery
        PUBLIC
            bmdg_transport
    )

    add_library(bmdg_composition
        src/composition/market_registry.cpp
        src/composition/multi_market_runtime.cpp
    )
    bmdg_configure_library(bmdg_composition)
    target_link_libraries(
        bmdg_composition
        PUBLIC
            bmdg_runtime
            bmdg_transport
            bmdg_recovery
            bmdg_publication
    )

    add_library(bmdg_status
        src/status/gateway_status.cpp
    )
    bmdg_configure_library(bmdg_status)
    target_link_libraries(
        bmdg_status
        PUBLIC
            bmdg_composition
            bmdg_runtime
            bmdg_recovery
            bmdg_publication
    )

    add_library(bmdg_grpc
        src/grpc/grpc_service.cpp
    )
    bmdg_configure_library(bmdg_grpc)
    target_link_libraries(
        bmdg_grpc
        PUBLIC
            bmdg_status
            bmdg_composition
            bmdg_runtime
            bmdg_recovery
            bmdg_publication
            BinanceMarketDataContracts::Grpc
    )

    add_library(bmdg_production
        src/production/daemon_config.cpp
        src/production/daemon_runtime.cpp
        src/production/production_gateway.cpp
        src/production/production_metadata.cpp
        src/production/termination_signals.cpp
    )
    bmdg_configure_library(bmdg_production)
    target_link_libraries(
        bmdg_production
        PUBLIC
            BinanceMarketDataGateway::Foundation
            bmdg_grpc
            bmdg_composition
            bmdg_transport
            bmdg_protocol
            nlohmann_json::nlohmann_json
            Threads::Threads
    )

    if(BMDG_BUILD_PRODUCTION_DAEMON)
        add_executable(bmdg app/bmdg/main.cpp)
        target_link_libraries(bmdg PRIVATE bmdg_production)
        bmdg_configure_target(bmdg)

        # Transitional compatibility: old scripts and service units may still
        # look for bmd-gatewayd. The real binary is bmdg.
        add_custom_command(
            TARGET bmdg POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E create_symlink
                    $<TARGET_FILE_NAME:bmdg>
                    $<TARGET_FILE_DIR:bmdg>/bmd-gatewayd
        )
        add_custom_target(bmd-gatewayd DEPENDS bmdg)

        install(TARGETS bmdg RUNTIME DESTINATION bin)
        install(CODE [[
            execute_process(
                COMMAND "${CMAKE_COMMAND}" -E create_symlink
                        "bmdg"
                        "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/bmd-gatewayd"
                RESULT_VARIABLE _bmdg_symlink_result)
            if(NOT _bmdg_symlink_result EQUAL 0)
                message(WARNING "failed to create bmd-gatewayd compatibility symlink")
            endif()
        ]])
    endif()

    # -----------------------------------------------------------------------
    # Test support and domain tests
    # -----------------------------------------------------------------------

    # Old target names remain available as aliases for one transition period.
    foreach(_bmdg_pair IN ITEMS
            "bmd_gateway_protocol;bmdg_protocol"
            "bmd_gateway_publication;bmdg_publication"
            "bmd_gateway_runtime;bmdg_runtime"
            "bmd_gateway_transport;bmdg_transport"
            "bmd_gateway_recovery;bmdg_recovery"
            "bmd_gateway_composition;bmdg_composition"
            "bmd_gateway_status;bmdg_status"
            "bmd_gateway_grpc;bmdg_grpc"
            "bmd_gateway_production;bmdg_production")
        list(GET _bmdg_pair 0 _bmdg_old)
        list(GET _bmdg_pair 1 _bmdg_new)
        add_library(${_bmdg_old} ALIAS ${_bmdg_new})
    endforeach()

    if(BMDG_BUILD_DOMAIN_TESTS OR BMDG_BUILD_ACCEPTANCE_TOOLS)
        add_library(bmdg_synthetic_host
            tests/support/synthetic_spot_host.cpp
        )
        bmdg_configure_library(bmdg_synthetic_host)
        target_include_directories(
            bmdg_synthetic_host
            PUBLIC
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
        )
        target_link_libraries(
            bmdg_synthetic_host
            PUBLIC
                BinanceMarketDataContracts::Protobuf
                BinanceMarketDataProjection::Core
                BinanceMarketDataProjection::ProtoAdapter
        )
        add_library(bmd_gateway_synthetic_host ALIAS bmdg_synthetic_host)
    endif()

    if(BMDG_BUILD_DOMAIN_TESTS)
        bmdg_add_test(
            bmdg_synthetic_host_tests
            tests/support/synthetic_spot_host_tests.cpp
            bmdg_synthetic_host
        )

        bmdg_add_test(
            bmdg_protocol_tests
            tests/protocol/spot_protocol_tests.cpp
            bmdg_protocol
        )

        bmdg_add_test(
            bmdg_usdm_protocol_tests
            tests/protocol/usdm_protocol_tests.cpp
            bmdg_runtime
        )
        target_compile_definitions(
            bmdg_usdm_protocol_tests
            PRIVATE
                BMDG_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
        )

        bmdg_add_test(
            bmdg_public_api_budget_tests
            tests/transport/public_api_budget_tests.cpp
            Threads::Threads
        )
        target_include_directories(
            bmdg_public_api_budget_tests
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/src
        )

        bmdg_add_test(
            bmdg_spot_transport_tests
            tests/transport/spot_transport_tests.cpp
            bmdg_transport
        )
        bmdg_add_test(
            bmdg_usdm_transport_tests
            tests/transport/usdm_transport_tests.cpp
            bmdg_transport
        )
        bmdg_add_test(
            bmdg_usdm_reference_tests
            tests/transport/usdm_reference_tests.cpp
            bmdg_transport
        )

        bmdg_add_test(
            bmdg_market_runtime_tests
            tests/runtime/market_runtime_tests.cpp
            bmdg_runtime
        )

        bmdg_add_test(
            bmdg_composition_tests
            tests/composition/multi_market_runtime_tests.cpp
            bmdg_composition
        )

        bmdg_add_test(
            bmdg_recovery_tests
            tests/recovery/spot_recovery_tests.cpp
            bmdg_recovery
        )
        bmdg_add_test(
            bmdg_rotation_tests
            tests/recovery/spot_rotation_tests.cpp
            bmdg_recovery
        )

        bmdg_add_test(
            bmdg_publication_tests
            tests/publication/order_book_publication_tests.cpp
            bmdg_runtime
        )
        bmdg_add_test(
            bmdg_event_publication_tests
            tests/publication/event_publication_tests.cpp
            bmdg_publication
        )

        bmdg_add_test(
            bmdg_grpc_service_tests
            tests/grpc/grpc_service_tests.cpp
            bmdg_grpc
        )
        bmdg_add_test(
            bmdg_event_service_tests
            tests/grpc/event_service_tests.cpp
            bmdg_grpc
        )
        bmdg_add_test(
            bmdg_status_service_tests
            tests/grpc/status_service_tests.cpp
            bmdg_grpc
        )
        bmdg_add_test(
            bmdg_routing_tests
            tests/grpc/routing_tests.cpp
            bmdg_grpc
        )

        bmdg_add_test(
            bmdg_status_tests
            tests/status/gateway_status_tests.cpp
            bmdg_status
        )

        bmdg_add_test(
            bmdg_projection_integration_tests
            tests/integration/projection_integration_tests.cpp
            bmdg_production
        )

        bmdg_add_test(
            bmdg_four_product_integration_tests
            tests/integration/four_product_integration_tests.cpp
            bmdg_production
        )
        target_compile_definitions(
            bmdg_four_product_integration_tests
            PRIVATE
                BMDG_FOUR_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway.json"
        )

        bmdg_add_test(
            bmdg_twenty_product_integration_tests
            tests/integration/four_product_integration_tests.cpp
            bmdg_production
        )
        target_compile_definitions(
            bmdg_twenty_product_integration_tests
            PRIVATE
                BMDG_TWENTY_INTEGRATION=1
                BMDG_TWENTY_PROFILE="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/projection-p1-twenty.json"
                BMDG_TWENTY_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway-twenty.json"
        )

        bmdg_add_test(
            bmdg_production_tests
            tests/production/production_gateway_tests.cpp
            bmdg_production
        )
        target_compile_definitions(
            bmdg_production_tests
            PRIVATE
                BMDG_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
        )

        bmdg_add_test(
            bmdg_production_config_metadata_tests
            tests/production/production_config_metadata_tests.cpp
            bmdg_production
        )

        bmdg_add_test(
            bmdg_production_acceptance_config_tests
            tests/production/production_acceptance_config_tests.cpp
            bmdg_production
        )

        add_executable(
            bmdg_projection_p1_measurements
            tests/performance/projection_p1_measurements.cpp
        )
        target_include_directories(
            bmdg_projection_p1_measurements
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/src
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
                ${CMAKE_CURRENT_SOURCE_DIR}/app
        )
        target_link_libraries(
            bmdg_projection_p1_measurements
            PRIVATE
                bmdg_production
        )
        bmdg_configure_target(bmdg_projection_p1_measurements)
        target_compile_definitions(
            bmdg_projection_p1_measurements
            PRIVATE
                BMDG_FOUR_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway.json"
                BMDG_TWENTY_PRODUCT_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/examples/gateway-twenty.json"
        )
        add_test(
            NAME bmdg_projection_p1_smoke
            COMMAND bmdg_projection_p1_measurements
                    8 1 8 1000 "${CMAKE_CURRENT_BINARY_DIR}/p1-smoke.json"
        )
        set_tests_properties(bmdg_projection_p1_smoke PROPERTIES TIMEOUT 30)
        add_test(
            NAME bmdg_projection_p1_twenty_smoke
            COMMAND bmdg_projection_p1_measurements
                    --profile "${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/projection-p1-twenty.json"
                    smoke "${CMAKE_CURRENT_BINARY_DIR}/p1-twenty-smoke.json"
        )
        set_tests_properties(bmdg_projection_p1_twenty_smoke PROPERTIES TIMEOUT 60)

        if(BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION)
            bmdg_add_test(
                bmdg_performance_baseline_tests
                tests/performance/performance_baseline_tests.cpp
                bmdg_production
            )
        endif()

        add_executable(
            bmdg_production_daemon_fixture
            tests/production/production_daemon_fixture.cpp
        )
        target_include_directories(
            bmdg_production_daemon_fixture
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/src
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
        )
        target_link_libraries(
            bmdg_production_daemon_fixture
            PRIVATE
                bmdg_production
        )
        bmdg_configure_target(bmdg_production_daemon_fixture)

        if(BMDG_BUILD_PRODUCTION_DAEMON)
            add_executable(
                bmdg_production_daemon_process_tests
                tests/production/production_daemon_process_tests.cpp
            )
            target_include_directories(
                bmdg_production_daemon_process_tests
                PRIVATE
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests
                    ${CMAKE_CURRENT_SOURCE_DIR}/app
            )
            target_link_libraries(
                bmdg_production_daemon_process_tests
                PRIVATE
                    bmdg_production
            )
            bmdg_configure_target(
                bmdg_production_daemon_process_tests)
            add_test(
                NAME bmdg_production_daemon_process
                COMMAND bmdg_production_daemon_process_tests
                        $<TARGET_FILE:bmdg>
                        $<TARGET_FILE:bmdg_production_daemon_fixture>
            )
            set_tests_properties(
                bmdg_production_daemon_process
                PROPERTIES TIMEOUT 45)
        endif()
    endif()

    # -----------------------------------------------------------------------
    # Historical live/acceptance tools. They are intentionally outside the
    # production daemon target and are only built on request.
    # -----------------------------------------------------------------------

    if(BMDG_BUILD_ACCEPTANCE_TOOLS)
        bmdg_add_acceptance(
            bmdg-synthetic-spot-host
            app/acceptance/synthetic_spot_host/main.cpp
            bmdg_synthetic_host
        )
        bmdg_add_acceptance(
            bmdg-spot-live
            app/acceptance/spot_live/main.cpp
            bmdg_transport
        )
        bmdg_add_acceptance(
            bmdg-spot-recovery-live
            app/acceptance/spot_recovery_live/main.cpp
            bmdg_recovery
        )
        bmdg_add_acceptance(
            bmdg-spot-rotation-live
            app/acceptance/spot_rotation_live/main.cpp
            bmdg_recovery
        )
        bmdg_add_acceptance(
            bmdg-spot-grpc-live
            app/acceptance/spot_grpc_live/main.cpp
            bmdg_grpc
        )
        bmdg_add_acceptance(
            bmdg-integration-live
            app/acceptance/integration_live/main.cpp
            bmdg_grpc
        )
        bmdg_add_acceptance(
            bmdg-spot-events-live
            app/acceptance/spot_events_live/main.cpp
            bmdg_grpc
        )
        bmdg_add_acceptance(
            bmdg-multi-market-live
            app/acceptance/multi_market_live/main.cpp
            bmdg_grpc
        )
        bmdg_add_acceptance(
            bmdg-production-acceptance-client
            app/acceptance/production_daemon/main.cpp
            bmdg_production
        )
    endif()
endif()

# ---------------------------------------------------------------------------
# Opt-in upstream link smoke (frozen dependency boundary proof).
# ---------------------------------------------------------------------------

if(BMDG_BUILD_UPSTREAM_LINK_SMOKE)
    if(NOT BMDG_BUILD_TESTS)
        enable_testing()
    endif()
    find_package(BinanceMarketDataContracts 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Protobuf)
    find_package(BinanceMarketDataContractsGrpc 0.1.0 EXACT CONFIG REQUIRED)
    find_package(BinanceMarketDataProjection 0.1.0 EXACT CONFIG REQUIRED COMPONENTS Core ProtoAdapter)

    foreach(bmdg_package IN ITEMS
            BinanceMarketDataContracts
            BinanceMarketDataContractsGrpc
            BinanceMarketDataProjection)
        if(NOT DEFINED ${bmdg_package}_DIR)
            message(FATAL_ERROR "Upstream package directory was not resolved: ${bmdg_package}_DIR")
        endif()
        set(bmdg_config
            "${${bmdg_package}_DIR}/${bmdg_package}Config.cmake")
        if(NOT EXISTS "${bmdg_config}")
            message(FATAL_ERROR "Upstream canonical package config is missing: ${bmdg_config}")
        endif()
        message(STATUS "Upstream canonical config: ${bmdg_package}=${bmdg_config}")
    endforeach()

    bmdg_add_test(
        bmdg_upstream_link_smoke
        tests/smoke/upstream_link_smoke.cpp
        BinanceMarketDataGateway::Foundation
        BinanceMarketDataContracts::Protobuf
        BinanceMarketDataContracts::Grpc
        BinanceMarketDataProjection::Core
        BinanceMarketDataProjection::ProtoAdapter
    )
endif()

# ---------------------------------------------------------------------------
# Transitional target aliases (old bmd_gateway_* / bmd-gateway-* names).
# These are aliases, not build outputs; the real targets are bmdg_*.
# ---------------------------------------------------------------------------

function(bmdg_alias_executable old_name new_name)
    if(TARGET ${new_name})
        add_executable(${old_name} ALIAS ${new_name})
    endif()
endfunction()

foreach(_bmdg_pair IN ITEMS
        "bmd_gateway_foundation_tests;bmdg_foundation_tests"
        "bmd_gateway_synthetic_host_tests;bmdg_synthetic_host_tests"
        "bmd_gateway_protocol_tests;bmdg_protocol_tests"
        "bmd_gateway_usdm_protocol_tests;bmdg_usdm_protocol_tests"
        "bmd_gateway_public_api_budget_tests;bmdg_public_api_budget_tests"
        "bmd_gateway_spot_transport_tests;bmdg_spot_transport_tests"
        "bmd_gateway_usdm_transport_tests;bmdg_usdm_transport_tests"
        "bmd_gateway_usdm_reference_tests;bmdg_usdm_reference_tests"
        "bmd_gateway_market_runtime_tests;bmdg_market_runtime_tests"
        "bmd_gateway_composition_tests;bmdg_composition_tests"
        "bmd_gateway_recovery_tests;bmdg_recovery_tests"
        "bmd_gateway_rotation_tests;bmdg_rotation_tests"
        "bmd_gateway_publication_tests;bmdg_publication_tests"
        "bmd_gateway_event_publication_tests;bmdg_event_publication_tests"
        "bmd_gateway_grpc_service_tests;bmdg_grpc_service_tests"
        "bmd_gateway_event_service_tests;bmdg_event_service_tests"
        "bmd_gateway_status_service_tests;bmdg_status_service_tests"
        "bmd_gateway_routing_tests;bmdg_routing_tests"
        "bmd_gateway_status_tests;bmdg_status_tests"
        "bmd_gateway_projection_integration_tests;bmdg_projection_integration_tests"
        "bmd_gateway_four_product_integration_tests;bmdg_four_product_integration_tests"
        "bmd_gateway_twenty_product_integration_tests;bmdg_twenty_product_integration_tests"
        "bmd_gateway_production_tests;bmdg_production_tests"
        "bmd_gateway_production_config_metadata_tests;bmdg_production_config_metadata_tests"
        "bmd_gateway_production_acceptance_config_tests;bmdg_production_acceptance_config_tests"
        "bmd_gateway_projection_p1_measurements;bmdg_projection_p1_measurements"
        "bmd_gateway_performance_baseline_tests;bmdg_performance_baseline_tests"
        "bmd_gateway_production_daemon_fixture;bmdg_production_daemon_fixture"
        "bmd_gateway_production_daemon_process_tests;bmdg_production_daemon_process_tests"
        "bmd_gateway_upstream_link_smoke;bmdg_upstream_link_smoke"
        "bmd-gateway-synthetic-spot-host;bmdg-synthetic-spot-host"
        "bmd-gateway-spot-live;bmdg-spot-live"
        "bmd-gateway-spot-recovery-live;bmdg-spot-recovery-live"
        "bmd-gateway-spot-rotation-live;bmdg-spot-rotation-live"
        "bmd-gateway-spot-grpc-live;bmdg-spot-grpc-live"
        "bmd-gateway-integration-live;bmdg-integration-live"
        "bmd-gateway-spot-events-live;bmdg-spot-events-live"
        "bmd-gateway-multi-market-live;bmdg-multi-market-live"
        "bmd-gateway-production-acceptance-client;bmdg-production-acceptance-client")
    list(GET _bmdg_pair 0 _bmdg_old)
    list(GET _bmdg_pair 1 _bmdg_new)
    bmdg_alias_executable(${_bmdg_old} ${_bmdg_new})
endforeach()
