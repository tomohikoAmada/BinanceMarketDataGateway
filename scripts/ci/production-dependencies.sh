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
        'BinanceMarketDataContracts b4af96a29ade080bc92d65d5a7dc64184f139424' \
        'BinanceMarketDataProjection c335c33a10a347bf9575e3f3d25fcaf202ec8490'; do
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
        'binance-market-data-projection/0.1.0#a330ca46ddb45c1e17cdadcea250442c'; do
        conan cache path "$ref"
    done
    # Only this explicitly requested preparation may build missing binaries.
    BMD_CONTRACTS_SOURCE_REVISION=b4af96a29ade080bc92d65d5a7dc64184f139424 \
        conan install . "${args[@]}" --build=missing --output-folder=build/production-deps
    conan cache clean '*' --source --build --download --temp
elif [[ "$mode" != consume ]]; then
    echo "usage: $0 [consume|prepare]" >&2
    exit 2
fi

conan install . "${args[@]}" --build=never --no-remote \
    --output-folder=build/production-deps --format=json > build/production-deps-graph.json
