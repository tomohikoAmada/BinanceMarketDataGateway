#!/usr/bin/env python3
import json
import sys
from pathlib import Path


EXPECTED = {
    "binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006",
    "binance-market-data-contracts-grpc-cpp/0.1.0#c3661aa735e1eb6450c1a93f635dbc20",
    "binance-market-data-projection/0.1.0#5c8ee9626b652fad1075fa44e480182b",
}


def main() -> int:
    graph = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    nodes = graph["graph"]["nodes"]
    referenced_nodes = {node["ref"]: node for node in nodes.values() if "ref" in node}
    references = set(referenced_nodes)

    if not EXPECTED.issubset(references):
        missing = sorted(EXPECTED - references)
        raise SystemExit(f"G1 graph is missing exact candidate references: {missing}")

    message_revisions = {
        reference.split("#", 1)[1].split(":", 1)[0]
        for reference in references
        if reference.startswith("binance-market-data-contracts-cpp/0.1.0#")
    }
    if message_revisions != {"53713fb82c27cf6ea0395f6b8a853006"}:
        raise SystemExit(f"G1 graph resolved multiple Contracts message RREVs: {message_revisions}")

    if len(sys.argv) > 2:
        package_prefixes = {
            "contracts": referenced_nodes[
                "binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006"
            ]["package_folder"],
            "grpc": referenced_nodes[
                "binance-market-data-contracts-grpc-cpp/0.1.0#c3661aa735e1eb6450c1a93f635dbc20"
            ]["package_folder"],
            "projection": referenced_nodes[
                "binance-market-data-projection/0.1.0#5c8ee9626b652fad1075fa44e480182b"
            ]["package_folder"],
        }
        Path(sys.argv[2]).write_text(
            "".join(f"{name}={path}\n" for name, path in package_prefixes.items()),
            encoding="utf-8",
        )

    print("G1 exact candidate graph verified")
    print("  Contracts message RREV: 53713fb82c27cf6ea0395f6b8a853006")
    print("  Contracts gRPC RREV: c3661aa735e1eb6450c1a93f635dbc20")
    print("  Projection RREV: 5c8ee9626b652fad1075fa44e480182b")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
