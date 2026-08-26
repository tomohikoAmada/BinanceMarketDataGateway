#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path


EXPECTED = {
    "contracts_message": {
        "name": "binance-market-data-contracts-cpp",
        "source": "9c34199f42467b92a4380c6e88617dde0e54ae13",
        "rrev": "53713fb82c27cf6ea0395f6b8a853006",
        "package_id": "a1a286da6ca09b590d78bcb14d8250c025131c29",
        "prev": "244330f80db8b6aaed1adbe6f90f825e",
    },
    "contracts_grpc": {
        "name": "binance-market-data-contracts-grpc-cpp",
        "source": "9c34199f42467b92a4380c6e88617dde0e54ae13",
        "rrev": "c3661aa735e1eb6450c1a93f635dbc20",
        "package_id": "3acd5bb035736877e698472136645086258451a7",
        "prev": "80a1c50b6d77fae2d6bcc2fb895ba7ae",
    },
    "projection": {
        "name": "binance-market-data-projection",
        "source": "87d05af38d7173ca2d5cae13e7592c38495ec895",
        "rrev": "58ab66309450bdc2035b3e5935220348",
        "package_id": "cb502a97da45967059ab2838b16d0fa48417c4a7",
        "prev": "1af4eac37f3e73001b6109f095fa1a54",
    },
}


def verify_document(path: Path, observed: dict[str, dict[str, str]]) -> None:
    document = path.read_text(encoding="utf-8")
    labels = {
        "contracts_message": "Contracts message",
        "contracts_grpc": "Contracts gRPC",
        "projection": "Projection",
    }
    for key, expected in EXPECTED.items():
        label = labels[key]
        source_match = re.search(rf"\| {re.escape(label)} \| `([^`]+)` \| `([^`]+)` \|", document)
        if source_match is None:
            raise SystemExit(f"G1 candidate document is missing {label} source/RREV row")
        if source_match.group(1) != expected["source"]:
            raise SystemExit(f"G1 candidate document has unexpected {label} source")
        if source_match.group(2) != expected["rrev"]:
            raise SystemExit(f"G1 candidate document has unexpected {label} RREV")

        identity_match = re.search(
            rf"\| {re.escape(label)} \| `([^`]+)` \| `([^`]+)` \|", document[document.find("The observed"):]
        )
        if identity_match is None:
            raise SystemExit(f"G1 candidate document is missing {label} package identity row")
        document_identity = {
            "package_id": identity_match.group(1),
            "prev": identity_match.group(2),
        }
        if document_identity != {
            "package_id": observed[key]["package_id"],
            "prev": observed[key]["prev"],
        }:
            raise SystemExit(f"G1 candidate document disagrees with the graph for {label}")


def main() -> int:
    graph = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    nodes = graph["graph"]["nodes"]
    observed = {}
    for key, expected in EXPECTED.items():
        matching_nodes = [
            node
            for node in nodes.values()
            if node.get("name") == expected["name"] and node.get("rrev") == expected["rrev"]
        ]
        if len(matching_nodes) != 1:
            raise SystemExit(
                f"G1 graph does not contain exactly one expected {expected['name']} node: "
                f"{len(matching_nodes)}"
            )
        node = matching_nodes[0]
        for identity_key in ("package_id", "prev"):
            if node.get(identity_key) != expected[identity_key]:
                raise SystemExit(
                    f"G1 graph has unexpected {expected['name']} {identity_key}: "
                    f"{node.get(identity_key)!r} != {expected[identity_key]!r}"
                )
        observed[key] = {
            "rrev": node["rrev"],
            "package_id": node["package_id"],
            "prev": node["prev"],
            "package_folder": node.get("package_folder"),
        }

    message_revisions = {
        node.get("rrev")
        for node in nodes.values()
        if node.get("name") == EXPECTED["contracts_message"]["name"]
        and node.get("rrev") is not None
    }
    if message_revisions != {EXPECTED["contracts_message"]["rrev"]}:
        raise SystemExit(f"G1 graph resolved multiple Contracts message RREVs: {message_revisions}")

    if len(sys.argv) > 3:
        verify_document(Path(sys.argv[3]), observed)

    if len(sys.argv) > 2:
        package_prefixes = {
            "contracts": observed["contracts_message"]["package_folder"],
            "grpc": observed["contracts_grpc"]["package_folder"],
            "projection": observed["projection"]["package_folder"],
        }
        if any(path is None for path in package_prefixes.values()):
            raise SystemExit("G1 graph is missing a package folder for an expected binary")
        Path(sys.argv[2]).write_text(
            "".join(f"{name}={path}\n" for name, path in package_prefixes.items()),
            encoding="utf-8",
        )

    print("G1 exact candidate graph verified")
    for key in ("contracts_message", "contracts_grpc", "projection"):
        identity = observed[key]
        print(f"  {key} source: {EXPECTED[key]['source']}")
        print(f"  {key} RREV: {identity['rrev']}")
        print(f"  {key} package ID: {identity['package_id']}")
        print(f"  {key} PREV: {identity['prev']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
