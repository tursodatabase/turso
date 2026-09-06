import pathlib
import unittest

from check_cargo_bazel_parity import parity_errors


ROOT = pathlib.Path("/repo")


class ParityErrorsTest(unittest.TestCase):
    def test_reports_missing_cargo_target(self):
        metadata = {
            "workspace_members": ["demo 0.1.0"],
            "packages": [
                {
                    "id": "demo 0.1.0",
                    "manifest_path": "/repo/demo/Cargo.toml",
                    "targets": [{"kind": ["lib"], "name": "demo"}],
                }
            ],
        }
        query_xml = "<query version='2'></query>"

        self.assertEqual(
            parity_errors(metadata, query_xml, ROOT, {}),
            ["missing Bazel marker for demo lib target demo"],
        )

    def test_accepts_one_kind_of_multi_kind_target(self):
        metadata = {
            "workspace_members": ["demo 0.1.0"],
            "packages": [
                {
                    "id": "demo 0.1.0",
                    "manifest_path": "/repo/demo/Cargo.toml",
                    "targets": [{"kind": ["cdylib", "lib"], "name": "demo"}],
                }
            ],
        }
        query_xml = """<query version='2'><rule name='//demo:demo'><list name='tags'>
            <string value='cargo-target=lib:demo'/>
        </list></rule></query>"""

        self.assertEqual(parity_errors(metadata, query_xml, ROOT, {}), [])

    def test_reports_marker_for_removed_cargo_target(self):
        metadata = {"workspace_members": [], "packages": []}
        query_xml = """<query version='2'><rule name='//demo:old'><list name='tags'>
            <string value='cargo-target=bin:old'/>
        </list></rule></query>"""

        self.assertEqual(
            parity_errors(metadata, query_xml, ROOT, {}),
            ["Bazel marker has no matching Cargo target: demo:bin:old"],
        )


if __name__ == "__main__":
    unittest.main()
