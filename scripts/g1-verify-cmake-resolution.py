#!/usr/bin/env python3
import re
import sys
from pathlib import Path


PACKAGES = {
    "BinanceMarketDataContracts": "contracts",
    "BinanceMarketDataContractsGrpc": "grpc",
    "BinanceMarketDataProjection": "projection",
}


def read_package_prefixes(path: Path) -> dict[str, Path]:
    prefixes = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        name, separator, package_folder = line.partition("=")
        if not separator or name not in PACKAGES.values() or not package_folder:
            raise SystemExit(f"invalid package-prefix record: {line!r}")
        prefixes[name] = Path(package_folder).resolve()
    if set(prefixes) != set(PACKAGES.values()):
        raise SystemExit(f"missing package-prefix records: {sorted(set(PACKAGES.values()) - set(prefixes))}")
    return prefixes


def main() -> int:
    cache_path = Path(sys.argv[1])
    configure_log = Path(sys.argv[2])
    prefixes = read_package_prefixes(Path(sys.argv[3]))
    log = configure_log.read_text(encoding="utf-8")

    if "BMD_GATEWAY_UPSTREAM_CMAKE_PREFIX_PATH" in log:
        raise SystemExit("authoritative G1 configure mentioned the removed manual prefix option")

    resolved = {}
    pattern = re.compile(r"G1 canonical config: (\w+?)=(.+)$", re.MULTILINE)
    for package_name, package_config in pattern.findall(log):
        if package_name in PACKAGES:
            resolved[package_name] = Path(package_config).resolve()

    if set(resolved) != set(PACKAGES):
        raise SystemExit(f"missing canonical config evidence: {sorted(set(PACKAGES) - set(resolved))}")

    cache = cache_path.read_text(encoding="utf-8")
    if "BMD_GATEWAY_UPSTREAM_CMAKE_PREFIX_PATH" in cache:
        raise SystemExit("CMake cache contains the removed manual prefix option")

    for package_name, prefix_key in PACKAGES.items():
        expected = (
            prefixes[prefix_key]
            / "lib"
            / "cmake"
            / package_name
            / f"{package_name}Config.cmake"
        )
        if resolved[package_name] != expected:
            raise SystemExit(
                f"{package_name} did not resolve its installed canonical config: "
                f"{resolved[package_name]} != {expected}"
            )
        if not resolved[package_name].is_file():
            raise SystemExit(f"resolved package config is not a file: {resolved[package_name]}")
        print(f"G1 canonical config verified: {package_name}={resolved[package_name]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
