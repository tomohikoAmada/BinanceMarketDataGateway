#!/usr/bin/env python3
"""Verify the exact G7-enabled Conan dependency lane."""

import json
import sys
from pathlib import Path


CONTRACTS_MESSAGE = "binance-market-data-contracts-cpp"
CONTRACTS_MESSAGE_RREV = "53713fb82c27cf6ea0395f6b8a853006"
CONTRACTS_GRPC = "binance-market-data-contracts-grpc-cpp"
CONTRACTS_GRPC_RREV = "c3661aa735e1eb6450c1a93f635dbc20"
PROJECTION = "binance-market-data-projection"
PROJECTION_RREV = "d95fa71d6dca8d931e72fbb5b74114a9"


def fail(message: str) -> "None":
    raise SystemExit(message)


def exact_host_node(nodes: list[dict], name: str, rrev: str) -> dict:
    matches = [
        node
        for node in nodes
        if node.get("context") == "host" and node.get("name") == name
    ]
    if len(matches) != 1:
        fail(f"expected exactly one host {name} node, found {len(matches)}")
    if matches[0].get("rrev") != rrev:
        fail(f"unexpected {name} RREV: {matches[0].get('rrev')!r}")
    if not matches[0].get("package_id") or not matches[0].get("prev"):
        fail(f"{name} does not resolve to an exact binary package revision")
    return matches[0]


def main() -> int:
    if len(sys.argv) != 2:
        fail(f"usage: {Path(sys.argv[0]).name} GRAPH_JSON")

    document = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    node_map = document.get("graph", {}).get("nodes")
    if not isinstance(node_map, dict):
        fail("Conan graph JSON is missing graph.nodes")
    nodes = list(node_map.values())

    message = exact_host_node(nodes, CONTRACTS_MESSAGE, CONTRACTS_MESSAGE_RREV)
    grpc_contracts = exact_host_node(nodes, CONTRACTS_GRPC, CONTRACTS_GRPC_RREV)
    exact_host_node(nodes, PROJECTION, PROJECTION_RREV)
    grpc_runtime = [
        node
        for node in nodes
        if node.get("context") == "host" and node.get("name") == "grpc"
    ]
    if len(grpc_runtime) != 1:
        fail(f"expected exactly one host grpc node, found {len(grpc_runtime)}")

    grpc_dependencies = grpc_contracts.get("dependencies", {})
    grpc_dependency_names = {
        dependency.get("ref", "").split("/", maxsplit=1)[0]
        for dependency in grpc_dependencies.values()
    }
    if CONTRACTS_MESSAGE not in grpc_dependency_names or "grpc" not in grpc_dependency_names:
        fail("Contracts gRPC package lacks its required message/gRPC dependencies")

    host_message_nodes = [
        node
        for node in nodes
        if node.get("context") == "host"
        and node.get("name") == CONTRACTS_MESSAGE
    ]
    if len(host_message_nodes) != 1 or host_message_nodes[0]["id"] != message["id"]:
        fail("G7 graph contains multiple Contracts message lineages")

    print("NORMAL_G7_CONTRACTS_GRPC=YES")
    print("NORMAL_G7_GRPC=YES")
    print("NORMAL_G7_PROJECTION=YES")
    print("SINGLE_CONTRACTS_MESSAGE_LINEAGE=YES")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
