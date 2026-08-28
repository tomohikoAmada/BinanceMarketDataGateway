#!/usr/bin/env python3
"""Verify the narrow normal Gateway dependency-lane invariant."""

import json
import sys
from pathlib import Path


CONTRACTS_MESSAGE = "binance-market-data-contracts-cpp"
CONTRACTS_MESSAGE_RREV = "53713fb82c27cf6ea0395f6b8a853006"
PROJECTION = "binance-market-data-projection"
PROJECTION_RREV = "d95fa71d6dca8d931e72fbb5b74114a9"
GRPC_PACKAGE_NAMES = {
    "binance-market-data-contracts-grpc-cpp",
    "grpc",
}


def fail(message: str) -> "None":
    raise SystemExit(message)


def main() -> int:
    if len(sys.argv) != 2:
        fail(f"usage: {Path(sys.argv[0]).name} GRAPH_JSON")

    document = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    nodes = document.get("graph", {}).get("nodes")
    if not isinstance(nodes, dict):
        fail("Conan graph JSON is missing graph.nodes")

    node_list = list(nodes.values())
    names = {str(node.get("name", "")).lower() for node in node_list}
    forbidden = sorted(names & GRPC_PACKAGE_NAMES)
    if forbidden:
        fail(f"normal Gateway graph contains forbidden gRPC package(s): {forbidden}")

    message_nodes = [node for node in node_list if node.get("name") == CONTRACTS_MESSAGE]
    message_rrevs = {
        node.get("rrev") for node in message_nodes if node.get("rrev") is not None
    }
    if message_rrevs != {CONTRACTS_MESSAGE_RREV}:
        fail(f"unexpected Contracts message RREVs: {sorted(message_rrevs)}")
    if len(message_nodes) != 1:
        fail(f"expected exactly one Contracts message node, found {len(message_nodes)}")

    projection_nodes = [node for node in node_list if node.get("name") == PROJECTION]
    projection_matches = [
        node for node in projection_nodes if node.get("rrev") == PROJECTION_RREV
    ]
    if len(projection_matches) != 1:
        fail(
            f"expected one Projection node at RREV {PROJECTION_RREV}, "
            f"found {len(projection_matches)}"
        )

    print("NORMAL_GRAPH_CONTRACTS_MESSAGE=YES")
    print("NORMAL_GRAPH_PROJECTION=YES")
    print("NORMAL_GRAPH_CONTRACTS_GRPC=NO")
    print("NORMAL_GRAPH_GRPC=NO")
    print("SINGLE_CONTRACTS_MESSAGE_LINEAGE=YES")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
