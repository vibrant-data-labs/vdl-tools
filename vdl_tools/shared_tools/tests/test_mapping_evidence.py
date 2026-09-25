import json
import unittest

import pandas as pd

from vdl_tools.shared_tools.taxonomy_mapping.mapping_evidence import assignment_explanations


class MappingEvidenceTests(unittest.TestCase):
    def test_full_path_identity_collapse_and_funding_unchanged(self):
        serving = pd.DataFrame([
            dict(org_uid="a", level0="Energy", level1="Power", level2="Solar", fraction=.5),
            dict(org_uid="a", level0="Nature", level1="Power", level2="Solar", fraction=.5),
            dict(org_uid="b", level0="Energy", level1="Power", level2="Solar", fraction=1),
        ], index=[3, 7, 9])
        before = serving.copy(deep=True)
        mapping = pd.DataFrame([
            dict(id="a", level0="Energy", level1="Power", level2="Solar", level3="Rooftops", evidence="roof panels", reason="generates electricity"),
            dict(id="a", level0="Energy", level1="Power", level2="Solar", level3="Farms", evidence="solar farms", reason="generates electricity"),
        ])
        mapping = pd.concat([mapping, mapping])
        result = assignment_explanations(serving, mapping, level_columns=[f"level{i}" for i in range(4)], source="run-17/mapping.json")
        self.assertEqual(list(result.index), [3, 7, 9])
        self.assertEqual(len(json.loads(result.loc[3])), 2)
        self.assertEqual(json.loads(result.loc[3])[0]["path"][-1], "Rooftops")
        self.assertEqual(json.loads(result.loc[7]), [])
        self.assertEqual(json.loads(result.loc[9]), [])
        pd.testing.assert_frame_equal(serving, before)

    def test_shallow_sentinels_and_missing_text(self):
        serving = pd.DataFrame([
            dict(org_uid="a", level0="Energy", level1=None, level2=None),
            dict(org_uid="a", level0="Energy", level1="Power", level2="Solar"),
            dict(org_uid="b", level0="No Match", level1=None, level2=None),
        ])
        mapping = pd.DataFrame([
            dict(id="a", level0="Energy", level1="No_Level_1_Energy", level2=None, reason="energy work", evidence=float("nan")),
            dict(id="b", level0="No Match", level1=None, level2=None, reason="no match"),
        ])
        result = assignment_explanations(serving, mapping, source="mapping.json").map(json.loads)
        self.assertEqual(result[0][0]["path"], ["Energy"])
        self.assertIsNone(result[0][0]["evidence"])
        self.assertEqual(result[1], [])
        self.assertEqual(result[2], [])

    def test_empty_legacy_and_bad_schema(self):
        serving = pd.DataFrame([dict(org_uid="a", level0="Energy", level1=None, level2=None)])
        self.assertEqual(assignment_explanations(serving, pd.DataFrame(), source="legacy").iloc[0], "[]")
        with self.assertRaises(ValueError):
            assignment_explanations(serving, pd.DataFrame([dict(id="a", reason="x")]), source="bad")


if __name__ == "__main__":
    unittest.main()
