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
        'BinanceMarketDataContracts 24a209b1a415c4f4dd56d864eee9132135bc7237' \
        'BinanceMarketDataProjection ebe86957e04dfed3bdda9ec58988d999419e4342'; do
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
        'binance-market-data-contracts-cpp/0.1.0#9739b82137e9d6437ef66bbbf81a9ddd' \
        'binance-market-data-contracts-grpc-cpp/0.1.0#40c3986aa8e13d2f8ebb137adfea931d' \
        'binance-market-data-projection/0.1.0#04cc16f91ddd2d214a1dd0efccac0eb3'; do
        conan cache path "$ref"
    done
    # Only this explicitly requested preparation may build missing binaries.
    BMD_CONTRACTS_SOURCE_REVISION=24a209b1a415c4f4dd56d864eee9132135bc7237 \
        conan install . "${args[@]}" --build=missing --output-folder=build/production-deps
    conan cache clean '*' --source --build --download --temp
elif [[ "$mode" != consume ]]; then
    echo "usage: $0 [consume|prepare]" >&2
    exit 2
fi

conan install . "${args[@]}" --build=never --no-remote \
    --output-folder=build/production-deps --format=json > build/production-deps-graph.json
