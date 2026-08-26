#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
conan_home="${BMD_GATEWAY_CONAN_HOME:-$repo_root/build/g1-conan-home}"
conan="${BMD_GATEWAY_CONAN:-conan}"
work_dir="$(mktemp -d "$repo_root/build/g1-candidate-proof.XXXXXX")"

cleanup() {
    case "$work_dir" in
        "$repo_root"/build/g1-candidate-proof.*) rm -r -- "$work_dir" ;;
        *) echo "refusing to remove unexpected path: $work_dir" >&2; exit 1 ;;
    esac
}
trap cleanup EXIT

export CONAN_HOME="$conan_home"
deps_dir="$work_dir/conan"
cmake_build="$work_dir/cmake"
graph_file="$work_dir/graph.json"
prefix_file="$work_dir/package-prefixes.txt"

"$conan" install "$repo_root" \
    --output-folder="$deps_dir" \
    --build=never \
    -s build_type=Release \
    -s compiler.cppstd=20 \
    --format=json \
    > "$graph_file"

"$repo_root/scripts/g1-verify-graph.py" "$graph_file" "$prefix_file"

contracts_prefix=""
grpc_prefix=""
projection_prefix=""
while IFS='=' read -r package_name package_prefix; do
    case "$package_name" in
        contracts) contracts_prefix="$package_prefix" ;;
        grpc) grpc_prefix="$package_prefix" ;;
        projection) projection_prefix="$package_prefix" ;;
    esac
done < "$prefix_file"

candidate_prefixes="$contracts_prefix;$grpc_prefix;$projection_prefix"

cmake \
    -S "$repo_root" \
    -B "$cmake_build" \
    -G "Unix Makefiles" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE="$deps_dir/conan_toolchain.cmake" \
    -DBMD_GATEWAY_BUILD_TESTS=ON \
    -DBMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE=ON \
    -DBMD_GATEWAY_UPSTREAM_CMAKE_PREFIX_PATH="$candidate_prefixes" \
    -DBMD_GATEWAY_ENABLE_WARNINGS=ON \
    -DBMD_GATEWAY_WARNINGS_AS_ERRORS=ON

cmake --build "$cmake_build" --parallel
ctest --test-dir "$cmake_build" --output-on-failure

"$cmake_build/bmd_gateway_upstream_link_smoke"

message_symbol='ExchangeDepthSnapshotD0Ev$'
message_owner_count="$(nm -g "$contracts_prefix/lib/libbinance_market_data_contracts_protobuf.a" 2>/dev/null \
    | awk '$2 ~ /^[TDBS]$/ {print $0}' \
    | grep -Ec "$message_symbol" || true)"
grpc_owner_count="$(nm -g "$grpc_prefix/lib/libbinance_market_data_contracts_grpc.a" 2>/dev/null \
    | awk '$2 ~ /^[TDBS]$/ {print $0}' \
    | grep -Ec "$message_symbol" || true)"
binary_owner_count="$(nm -g "$cmake_build/bmd_gateway_upstream_link_smoke" 2>/dev/null \
    | awk '$2 ~ /^[TDBS]$/ {print $0}' \
    | grep -Ec "$message_symbol" || true)"
if [[ "$message_owner_count" != "1" || "$grpc_owner_count" != "0" || "$binary_owner_count" != "1" ]]; then
    echo "unexpected Contracts message symbol ownership: message=$message_owner_count grpc=$grpc_owner_count binary=$binary_owner_count" >&2
    exit 1
fi

core_targets="$projection_prefix/lib/cmake/BinanceMarketDataProjection/BinanceMarketDataProjectionCoreTargets-release.cmake"
if rg -n -i 'contracts|protobuf' "$core_targets" >/dev/null; then
    echo "Projection Core target unexpectedly mentions Contracts/Protobuf" >&2
    exit 1
fi

if find "$repo_root" -path "$repo_root/build" -prune -o -name '*.proto' -print -quit | grep -q .; then
    echo "Gateway repository contains copied .proto files" >&2
    exit 1
fi
if rg -n -i 'recorder' CMakeLists.txt conanfile.py include src tests/upstream_link_smoke.cpp >/dev/null; then
    echo "Gateway G1 consumer unexpectedly depends on Recorder" >&2
    exit 1
fi

echo "G1 message symbol ownership and Core independence verified"
