import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERIFIER = ROOT / "scripts/g1-verify-graph.py"
SPEC = importlib.util.spec_from_file_location("g1_verify_graph", VERIFIER)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {VERIFIER}")
VERIFIER_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER_MODULE)


def make_node(name, rrev, package_id, prev, dependencies=None):
    return {
        "name": name,
        "rrev": rrev,
        "package_id": package_id,
        "prev": prev,
        "package_folder": f"/tmp/{name}",
        "dependencies": dependencies or {},
    }


def representative_graph():
    expected = VERIFIER_MODULE.PORTABLE_EXPECTED
    message = expected["contracts_message"]
    grpc = expected["contracts_grpc"]
    projection = expected["projection"]
    message_node = make_node(
        message["name"],
        message["rrev"],
        "a" * 40,
        "b" * 32,
    )
    return {
        "graph": {
            "nodes": {
                "0": {
                    "name": "binance-market-data-gateway-g1-consumer",
                    "dependencies": {
                        "1": {},
                        "2": {},
                        "3": {},
                    },
                },
                "1": message_node,
                "2": make_node(
                    grpc["name"],
                    grpc["rrev"],
                    "c" * 40,
                    "d" * 32,
                    {"1": {}},
                ),
                "3": make_node(
                    projection["name"],
                    projection["rrev"],
                    "e" * 40,
                    "f" * 32,
                    {"1": {}},
                ),
            }
        }
    }


class PortableGraphVerifierTest(unittest.TestCase):
    def run_verifier(self, graph):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            graph_path = directory / "graph.json"
            prefix_path = directory / "prefixes.txt"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(VERIFIER),
                    "--portable-lineage",
                    str(graph_path),
                    str(prefix_path),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            return result, prefix_path.is_file()

    def test_portable_mode_accepts_linux_package_identity(self):
        result, prefix_created = self.run_verifier(representative_graph())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(prefix_created)

    def test_rejects_wrong_projection_rrev(self):
        graph = representative_graph()
        graph["graph"]["nodes"]["3"]["rrev"] = "1" * 32
        result, _ = self.run_verifier(graph)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_wrong_contracts_message_rrev(self):
        graph = representative_graph()
        graph["graph"]["nodes"]["1"]["rrev"] = "2" * 32
        result, _ = self.run_verifier(graph)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_second_contracts_message_lineage(self):
        graph = representative_graph()
        graph["graph"]["nodes"]["4"] = copy.deepcopy(graph["graph"]["nodes"]["1"])
        graph["graph"]["nodes"]["4"]["rrev"] = "3" * 32
        result, _ = self.run_verifier(graph)
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
