"""Guard against stale official references and unsupported support claims."""
import copy
import importlib.util
import pathlib
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "parity.py"
SPEC = importlib.util.spec_from_file_location("parity", SCRIPT)
parity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(parity)


class SupportClaimGuards(unittest.TestCase):
    def setUp(self):
        self.matrix = parity.read(parity.MATRIX)
        self.snapshot = parity.read(parity.SNAPSHOT)

    def test_new_official_versions_require_a_fresh_implementation_review(self):
        self.snapshot["typescript"]["version"] = "99.0.0"
        with self.assertRaisesRegex(AssertionError, "inspect official implementations"):
            parity.validate(self.matrix, self.snapshot)

    def test_embedded_release_and_main_changes_require_review(self):
        for field in ["version", "main"]:
            changed = copy.deepcopy(self.snapshot)
            if field == "version":
                changed["embedded"]["version"] = "v99.0.0"
            else:
                changed["embedded"]["main"]["revision"] = "different revision"
            with self.assertRaisesRegex(AssertionError, "inspect official implementations"):
                parity.validate(self.matrix, changed)

    def test_embedded_binary_changes_are_drift_even_without_a_version_change(self):
        changed = copy.deepcopy(self.snapshot)
        asset = next(iter(changed["embedded"]["assets"].values()))
        asset["digest"] = "different published artifact"
        self.assertNotEqual(parity.comparison(changed), parity.comparison(self.snapshot))

    def test_supported_features_require_code_and_a_named_existing_test(self):
        row = next(row for row in self.matrix["features"] if row["rust"]["status"] == "supported")
        row["rust"]["tests"] = []
        with self.assertRaisesRegex(AssertionError, "needs supplied code and tests"):
            parity.validate(self.matrix, self.snapshot)

    def test_removed_or_renamed_test_cannot_leave_a_support_claim_green(self):
        row = next(row for row in self.matrix["features"] if row["rust"].get("tests"))
        row["rust"]["tests"][0]["name"] = "a_test_that_does_not_exist"
        with self.assertRaisesRegex(AssertionError, "test missing"):
            parity.validate(self.matrix, self.snapshot)

    def test_matrix_engine_requirements_must_match_runtime_guards(self):
        row = next(row for row in self.matrix["features"] if row.get("engine_requirement"))
        row["engine_requirement"]["version"] = "0.0.1"
        with self.assertRaisesRegex(AssertionError, "engine floor mismatch"):
            parity.validate(self.matrix, self.snapshot)

    def test_native_embedded_ci_must_use_the_recorded_platform_checksum(self):
        target = next(row for row in self.matrix["engine_targets"].values() if row.get("kind") == "native_embedded")
        target["checksum"] = "different"
        with self.assertRaisesRegex(AssertionError, "native embedded CI"):
            parity.validate(self.matrix, self.snapshot)

    def test_official_support_requires_an_implementation_reference(self):
        row = next(row for row in self.matrix["features"] if row["go"]["status"] != "absent")
        row["go"].pop("source")
        with self.assertRaisesRegex(AssertionError, "official go source"):
            parity.validate(self.matrix, self.snapshot)

    def test_check_time_changes_are_not_version_drift_but_artifact_changes_are(self):
        other = copy.deepcopy(self.snapshot)
        other["checked_at"] = "later"
        self.assertEqual(parity.comparison(other), parity.comparison(self.snapshot))
        other["typescript"]["integrity"] = "different published artifact"
        self.assertNotEqual(parity.comparison(other), parity.comparison(self.snapshot))


if __name__ == "__main__":
    unittest.main()
