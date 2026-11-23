#!/usr/bin/env bash
set -euo pipefail

# The ordinary PR path only consumes binaries. Preparing a missing cache is an
# explicit workflow_dispatch action; upstream source work never runs in PRs.
mode="${1:-consume}"
mkdir -p build/production-deps
profile=profiles/production-ci
args=(--lockfile=conan.lock -pr:h="$profile" -pr:b=profiles/production-ci-build -o with_g7_grpc=True)

if [[ "$mode" == prepare ]]; then
    mkdir -p build/ci-upstream
    for item in \
        'BinanceMarketDataContracts 05911f863525dc341f063a1d5bf1aaa6b3f8cd25' \
        'BinanceMarketDataProjection b682d59a5ec6cd4b0d3f9a5da69665eb1ee397a9'; do
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
        'binance-market-data-contracts-cpp/0.1.0#0029d6a497ad77201966ef85bfaf228e' \
        'binance-market-data-contracts-grpc-cpp/0.1.0#c1da6283935080279e4ac59f69515e4a' \
        'binance-market-data-projection/0.1.0#f1dc6264e7820d9d968536b4839a7bb5'; do
        conan cache path "$ref"
    done
    # Only this explicitly requested preparation may build missing binaries.
    BMD_CONTRACTS_SOURCE_REVISION=05911f863525dc341f063a1d5bf1aaa6b3f8cd25 \
        conan install . "${args[@]}" --build=missing --output-folder=build/production-deps
    conan cache clean '*' --source --build --download --temp
elif [[ "$mode" != consume ]]; then
    echo "usage: $0 [consume|prepare]" >&2
    exit 2
fi

conan install . "${args[@]}" --build=never --no-remote \
    --output-folder=build/production-deps --format=json > build/production-deps-graph.json
