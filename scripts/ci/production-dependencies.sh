#!/usr/bin/env bash
set -euo pipefail

# The ordinary PR path only consumes binaries. Preparing a missing cache is an
# explicit workflow_dispatch action; upstream source work never runs in PRs.
mode="${1:-consume}"
profile=profiles/production-ci
args=(--lockfile=conan.lock -pr:h="$profile" -pr:b="$profile" -o with_g7_grpc=True)

if [[ "$mode" == prepare ]]; then
    mkdir -p build/ci-upstream
    for item in \
        'BinanceMarketDataContracts 518880bdfa60948c3b65b6b3525d024526995166' \
        'BinanceMarketDataProjection 555dd1f4d846e19cfd6fbe55676c86504559048e'; do
        read -r repository revision <<< "$item"
        directory="build/ci-upstream/$repository"
        if [[ ! -d "$directory/.git" ]]; then
            git clone --no-checkout "https://github.com/tomohikoAmada/$repository.git" "$directory"
        fi
        git -C "$directory" fetch origin "$revision"
        git -C "$directory" checkout --detach "$revision"
        test "$(git -C "$directory" rev-parse HEAD)" = "$revision"
    done
    contracts=build/ci-upstream/BinanceMarketDataContracts
    projection=build/ci-upstream/BinanceMarketDataProjection
    conan export "$contracts"
    conan export "$contracts/conanfile_grpc.py"
    conan export "$projection"
    # Exact RREVs in the lock and consumer recipe must exist after export.
    for ref in \
        'binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006' \
        'binance-market-data-contracts-grpc-cpp/0.1.0#c3661aa735e1eb6450c1a93f635dbc20' \
        'binance-market-data-projection/0.1.0#d95fa71d6dca8d931e72fbb5b74114a9'; do
        conan cache path "$ref"
    done
    # Only this explicitly requested preparation may build missing binaries.
    BMD_CONTRACTS_SOURCE_REVISION=518880bdfa60948c3b65b6b3525d024526995166 \
        conan install . "${args[@]}" --build=missing --output-folder=build/production-deps
    conan cache clean '*' --source --build --download --temp
elif [[ "$mode" != consume ]]; then
    echo "usage: $0 [consume|prepare]" >&2
    exit 2
fi

conan install . "${args[@]}" --build=never --no-remote \
    --output-folder=build/production-deps --format=json > build/production-deps-graph.json
