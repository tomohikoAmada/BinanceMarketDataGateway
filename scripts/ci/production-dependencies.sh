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
        'BinanceMarketDataContracts 0ecd024833c8b93ff95287e5fd7593060784b173' \
        'BinanceMarketDataProjection a4151d4e8ddaedcc8ed0c4c34f4ced6b14f2fd7e'; do
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
        'binance-market-data-contracts-cpp/0.1.0#e79d88e75cb72b2bb7dae75e6d0b5646' \
        'binance-market-data-contracts-grpc-cpp/0.1.0#0237321d3ab670daa101c3214f0778bd' \
        'binance-market-data-projection/0.1.0#ef04bfbc02bc79f6b31cb6e30588da76'; do
        conan cache path "$ref"
    done
    # Only this explicitly requested preparation may build missing binaries.
    BMD_CONTRACTS_SOURCE_REVISION=0ecd024833c8b93ff95287e5fd7593060784b173 \
        conan install . "${args[@]}" --build=missing --output-folder=build/production-deps
    conan cache clean '*' --source --build --download --temp
elif [[ "$mode" != consume ]]; then
    echo "usage: $0 [consume|prepare]" >&2
    exit 2
fi

conan install . "${args[@]}" --build=never --no-remote \
    --output-folder=build/production-deps --format=json > build/production-deps-graph.json
