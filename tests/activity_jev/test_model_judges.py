"""Offline checks of the judged comparison: input formats, keys, scoring and report."""

import json

import pandas as pd
import pytest

from vdl_tools.shared_tools.activity_extraction import model_judges as mj
from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.general import GeneralActivityProfile

ACTIVITY = {
    "activity": "River cleanups",
    "what_happens": "Volunteers remove trash.",
    "who_carries_it_out": "Volunteers",
    "aim": "Cleaner rivers",
    "sector": "Environment",
    "methods": "Cleanups",
    "geography": "Not stated",
    "evidence": "monthly river cleanups",
    "certainty": "high",
    "prominence": "core",
    "recurrence": "recurring",
}


def profile(*names):
    return GeneralActivityProfile.model_validate(
        {
            "summary": "s",
            "geographic_orientation_evidence": "g",
            "site_matches_organization": "yes",
            "site_match_evidence": "e",
            "coverage": "thin",
            "activities": [{**ACTIVITY, "activity": n} for n in names],
        }
    )


SOURCE = "ORGANIZATION: River Friends\n\nURL: https://example.org\nTEXT: We run cleanups.\x07"
ORG = probe.OrgInput("org1", SOURCE, profile("Cleanups").model_dump_json(), {})


def runs():
    base = mj.Run(mj.BASELINE_RUN, {"org1": profile("Cleanups")})
    cand = mj.Run("baseten-glm-5.3-flash", {"org1": profile("Cleanups", "Tree planting")})
    return base, cand


def test_precision_input_keeps_the_original_layout_with_every_field():
    base, cand = runs()
    ((gid, body),) = mj.judge_inputs("precision", cand, [ORG], {"org1": "A river group."}, base)
    assert gid == f"{mj.JUDGE_RUN}_baseten-glm-5.3-flash|org1"
    assert body.startswith("ORGANIZATION: River Friends\n\nURL:")
    assert "\x07" not in body  # scrubbed, as the originals scrub
    assert (
        "\n\n========\nDESCRIPTION ON RECORD\n========\n\nA river group.\n\n"
        "========\nEXTRACTED ACTIVITIES\n========\n\n1. Cleanups\n"
        "   What happens: Volunteers remove trash.\n"
        "   Who carries it out: Volunteers\n"
        "   Aim: Cleaner rivers\n"
        "   Sector: Environment\n"
        "   Methods: Cleanups\n"
        "   Geography: Not stated\n"
        "   Evidence quoted by the extraction model: monthly river cleanups\n\n2. Tree planting\n"
    ) in body


def test_precision_without_description_says_none_on_record():
    base, cand = runs()
    ((_, body),) = mj.judge_inputs("precision", cand, [ORG], {}, base)
    assert "========\n\nnone on record\n\n========" in body


def test_recall_input_has_no_description_or_evidence():
    base, cand = runs()
    ((_, body),) = mj.judge_inputs("recall", cand, [ORG], {"org1": "A river group."}, base)
    assert "DESCRIPTION ON RECORD" not in body and "Evidence quoted" not in body
    # Recall v2 adds aim and sector, which its covered rule turns on.
    assert (
        "\n\n========\nEXTRACTED ACTIVITIES\n========\n\n1. Cleanups\n"
        "   What happens: Volunteers remove trash.\n"
        "   Who carries it out: Volunteers\n"
        "   Aim: Cleaner rivers\n"
        "   Sector: Environment\n\n2. Tree planting\n"
    ) in body


def test_recall_judges_an_empty_list_but_not_a_failed_extraction():
    base, _ = runs()
    empty = mj.Run("x", {"org1": profile()})
    ((_, body),) = mj.judge_inputs("recall", empty, [ORG], {}, base)
    assert body.endswith("(no activities listed)")
    failed = mj.Run("y", {}, invalid=["org1"])
    assert mj.judge_inputs("recall", failed, [ORG], {}, base) == []
    # Precision has nothing to judge without activities.
    assert mj.judge_inputs("precision", empty, [ORG], {}, base) == []


def test_alignment_puts_baseline_in_list_a_and_skips_the_baseline_itself():
    base, cand = runs()
    ((gid, body),) = mj.judge_inputs("alignment", cand, [ORG], {}, base)
    assert gid.endswith("|org1")
    assert body == (
        "ORGANIZATION: River Friends\n\nLIST A\n1. Cleanups — Volunteers remove trash. "
        "Who: Volunteers\n\nLIST B\n1. Cleanups — Volunteers remove trash. Who: Volunteers\n"
        "2. Tree planting — Volunteers remove trash. Who: Volunteers"
    )
    assert mj.judge_inputs("alignment", base, [ORG], {}, base) == []


def test_new_keys_cannot_meet_existing_judge_rows():
    # Existing rows are bare org ids, nn150_*|, agree_v*| and mitig60_*| families.
    base, cand = runs()
    ids = [
        g
        for name in mj.JUDGES
        for r in (base, cand)
        for g, _ in mj.judge_inputs(name, r, [ORG], {}, base)
    ]
    prefixes = (f"{mj.JUDGE_RUN}_", f"{mj.BASELINE_JUDGE_RUN}_{mj.BASELINE_RUN}|")
    assert ids and all(g.startswith(prefixes) for g in ids)
    assert not any(g.split("|")[0] in mj.NOISE_FLOOR_FAMILIES for g in ids)


def test_fingerprint_check_stops_on_a_mismatched_schema():
    tag = f"[response schema RecallVerdict #{mj.schema_fingerprint(mj.RecallVerdict)}]"
    mj.check_fingerprint(f"prompt text\n\n{tag}", mj.RecallVerdict)
    with pytest.raises(SystemExit, match="does not match"):
        mj.check_fingerprint(
            "prompt text\n\n[response schema RecallVerdict #000000000000]", mj.RecallVerdict
        )


@pytest.mark.parametrize(
    "judge, model, name",
    [
        (mj.JUDGE_SPECS["precision"], mj.PrecisionVerdictsV3_2, mj.PRECISION_V3_2_NAME),
        (mj.PRECISION_V3_1, mj.PrecisionVerdictsV3, mj.PRECISION_V3_NAME),
        (mj.JUDGE_SPECS["recall"], mj.RecallVerdictV2, mj.RECALL_V2_NAME),
        (mj.PRECISION_V2, mj.PrecisionVerdictsV2, mj.PRECISION_V2_NAME),
    ],
)
def test_prompts_written_here_are_tagged_with_their_own_schema(judge, model, name):
    mj.check_fingerprint(judge.prompt_str(), model)
    assert judge.prompt_id is None and judge.prompt_name == name


def test_stored_judges_have_no_text_of_their_own():
    assert mj.JUDGE_SPECS["alignment"].prompt_str() is None
    assert mj.RECALL_V1.prompt_str() is None


def test_v3_prompt_checks_every_field_the_schema_can_flag():
    section = mj.PRECISION_V3_INSTRUCTIONS.split("4. CHECK EACH FIELD AGAINST THE SOURCE", 1)[1]
    for name in mj.ACTIVITY_FIELDS:
        assert name in section


def test_ported_schemas_keep_the_originals_fingerprints():
    # Recorded in the stored judge prompts (checked against the DB on 2026-11-07).
    assert mj.schema_fingerprint(mj.ListAlignment) == "bbad3484dbb8"
    assert mj.schema_fingerprint(mj.PrecisionVerdicts) == "6f9bd82f66b8"
    assert mj.schema_fingerprint(mj.RecallVerdict) == "bf1974e59d56"


def _align(*groups):
    return mj.ListAlignment.model_validate(
        {"groups": [{"a": a, "b": b, "relation": r, "note": ""} for a, b, r in groups]}
    )


def test_alignment_scores_pairs_and_splits():
    scored = mj.score_alignment(
        3, 3, _align(([1], [1], "same"), ([2, 3], [2], "merge_split"), ([], [3], "only_b"))
    )
    assert scored["content_agreement"] == round(5 / 6, 3)
    assert scored["granularity_agreement"] == round(2 / 6, 3)
    assert not scored["repaired"]


def test_alignment_repairs_like_the_original():
    scored = mj.score_alignment(
        2,
        2,
        _align(
            ([1, 9], [1, 2], "same"),  # out-of-range dropped; 'same' 1x2 -> merge_split
            ([1, 2], [], "same"),  # a1 already used, dropped; empty side -> only_a
        ),
    )
    assert scored["n_merge_split"] == 1 and scored["n_only_a"] == 1
    assert scored["content_agreement"] == round(3 / 4, 3)
    # Like the original, an index repeated within one group is kept and counted twice,
    # since "already seen" is updated only after each group; kept so the scores stay
    # comparable to the originals' noise floor.
    twice = mj.score_alignment(1, 1, _align(([1, 1], [1], "same")))
    assert twice["content_agreement"] == 1.5
    missing = mj.score_alignment(2, 1, _align(([1], [1], "same")))  # a2 left out by the judge
    assert missing["repaired"] and missing["n_only_a"] == 1


def test_list_lengths_reads_numbered_items():
    body = "ORGANIZATION: X\n\nLIST A\n1. a — w Who: x\n2. b — w Who: x\n\nLIST B\n1. c — w Who: x"
    assert mj.list_lengths(body) == (2, 1)


def _precision_frame():
    def row(run, org, outcome, merge=False, fields="", material="", severity=0.0):
        return {
            "run": run,
            "org_id": org,
            "outcome": outcome,
            "severity": severity,
            "should_merge": merge,
            "unsupported_fields": fields,
            "material_fields": material,
        }

    return pd.DataFrame(
        [
            row(mj.BASELINE_RUN, "o1", "line_of_work"),
            row(mj.BASELINE_RUN, "o2", "minor_issue", fields="methods", severity=0.5),
            row("cand", "o1", "line_of_work"),
            row("cand", "o1", "line_of_work", merge=True),
            row(
                "cand",
                "o2",
                "material_issue",
                fields="geography",
                material="geography",
                severity=3.0,
            ),
            row(
                "cand", "o2", "judge_failed", merge=None, fields=None, material=None, severity=None
            ),
        ]
    )


def test_report_shows_runs_noise_floor_and_paired_differences():
    recall = pd.DataFrame(
        [
            {
                "run": r,
                "org_id": o,
                "judged": True,
                "n_activities": 1,
                "missed_line_of_work": m,
                "missed_minor_item": 0,
            }
            for r, o, m in [
                (mj.BASELINE_RUN, "o1", 1),
                (mj.BASELINE_RUN, "o2", 0),
                ("cand", "o1", 0),
                ("cand", "o2", 0),
            ]
        ]
    )
    alignment = pd.DataFrame(
        [{"run": "cand", "org_id": "o1", "content_agreement": 1.0, "granularity_agreement": 0.5}]
    )
    floor = pd.DataFrame(
        [
            {
                "run": "agree_v3",
                "org_id": "o1",
                "content_agreement": 0.9,
                "granularity_agreement": 0.8,
            }
        ]
    )
    out = mj.report(_precision_frame(), recall, alignment, floor)
    # A minor issue is not an error; shares are of judged activities, failures apart.
    assert (
        "| baseline | 2 | 2 | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 50.0% | 0.0% | 0.0% | 0.25 | 0.0% |"
        in out
    )
    assert (
        "| cand | 2 | 4 | 33.3% | 0.0% | 0.0% | 0.0% | 33.3% | 0.0% | 0.0% | 33.3% | 1.00 | 25.0% |"
        in out
    )
    material, anything = out.split("### Fields with any issue")
    assert "| cand | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 33.3% |" in material  # geography
    assert "| baseline | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% |" in material
    assert "| baseline | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 50.0% | 0.0% |" in anything  # methods
    assert "| noise floor: two baseline runs (agree_v3) | 1 | 0.900 | 0.800 |" in out
    assert "| baseline vs cand | 1 | 1.000 | 0.500 |" in out
    # Per org, judged activities only: o1 0 -> 0 errors, o2 0 -> 1.
    assert "| cand | error share | 2 | +0.500 |" in out
    # o1 0 -> 0.5 merge share, o2 0 -> 0.
    assert "| cand | should_merge share | 2 | +0.250 |" in out
    assert "| cand | missed line_of_work per org | 2 | -0.50 |" in out


def _verdict(index, label="line_of_work", overlap="none", same=None, fields=()):
    return mj.ActivityVerdictV2(
        activity_index=index,
        label=label,
        overlap=overlap,
        same_work_as=same,
        field_notes="n",
        unsupported_fields=list(fields),
        prominence="core",
        reason="r",
    )


def _issue(field="geography", support="inferred", impact="none"):
    return mj.FieldIssue(field=field, claim="c", support=support, impact=impact)


def _verdict3(index, label="line_of_work", overlap="none", same=None, issues=()):
    return mj.ActivityVerdictV3(
        activity_index=index,
        work_check="w",
        label=label,
        overlap=overlap,
        same_work_as=same,
        field_notes="n",
        field_issues=list(issues),
        prominence="core",
    )


@pytest.mark.parametrize(
    "verdict, outcome, severity",
    [
        (_verdict3(1), "line_of_work", 0.0),
        (_verdict3(1, issues=[_issue()]), "minor_issue", 0.5),
        (_verdict3(1, issues=[_issue(support="fabricated")]), "minor_issue", 1.0),
        (
            _verdict3(1, issues=[_issue(), _issue("methods", impact="material")]),
            "material_issue",
            1.0,
        ),
        (
            _verdict3(1, issues=[_issue(support="fabricated", impact="material")]),
            "material_issue",
            3.0,
        ),
        (_verdict3(2, overlap="duplicate", same=1, issues=[_issue()]), "duplicate", 3.0),
        (_verdict3(1, "not_an_activity"), "not_an_activity", 3.0),
        # A finer cut is not an error by itself.
        (_verdict3(2, overlap="should_merge", same=1), "line_of_work", 0.0),
    ],
)
def test_v3_outcome_and_severity(verdict, outcome, severity):
    assert mj.precision_outcome(verdict) == outcome
    assert mj.activity_severity(verdict) == severity
    assert (outcome in mj.ERROR_OUTCOMES) == (severity >= 1 and outcome != "minor_issue")


@pytest.mark.parametrize(
    "verdict, outcome",
    [
        (_verdict(1), "line_of_work"),
        (_verdict(1, "minor_item"), "minor_item"),
        (_verdict(1, fields=["geography"]), "embellished"),
        (_verdict(2, overlap="duplicate", same=1, fields=["aim"]), "duplicate"),
        (_verdict(2, "unsupported", overlap="duplicate", same=1), "unsupported"),
        (_verdict(1, "not_an_activity", fields=["aim"]), "not_an_activity"),
        # A finer cut is not an error by itself.
        (_verdict(2, overlap="should_merge", same=1), "line_of_work"),
    ],
)
def test_v2_outcome_order_still_reads_the_v2_run(verdict, outcome):
    assert mj.precision_outcome(verdict) == outcome


def test_precision_rows_mark_unjudged_activities():
    _, cand = runs()
    issues = [_issue(), _issue("aim", impact="material"), _issue("aim")]
    verdict = mj.PrecisionVerdictsV3(
        verdicts=[_verdict3(1, issues=issues)],
        site_identity="same_organization",
        site_identity_reason="r",
    )
    rows = mj.precision_rows(cand, {"org1": verdict})
    assert [r["outcome"] for r in rows] == ["material_issue", "missing_verdict"]
    assert rows[0]["unsupported_fields"] == "geography; aim" and rows[0]["material_fields"] == "aim"
    assert rows[0]["severity"] == 1.0 and rows[0]["should_merge"] is False
    failed = mj.precision_rows(cand, {"org1": None})
    assert [r["outcome"] for r in failed] == ["judge_failed"] * 2


def test_cost_estimate_uses_list_prices():
    judge = mj.JUDGE_SPECS["precision"]
    assert mj.estimate_cost(judge, 1_000_000, 10, 1000) == pytest.approx(2.00 + 10 * 1000 * 10e-6)
    assert json.loads(json.dumps(judge.kwargs(), default=str))["text_format"]


def test_baseline_keeps_its_first_judge_keys():
    base, cand = runs()
    ((gid, _),) = mj.judge_inputs("precision", base, [ORG], {}, base)
    assert gid == f"{mj.BASELINE_JUDGE_RUN}_{mj.BASELINE_RUN}|org1"
    assert mj.BASELINE_JUDGE_RUN != mj.JUDGE_RUN


def test_v3_2_prompt_is_v3_1_plus_the_recurrence_rating():
    v3_2 = mj.PRECISION_V3_2_INSTRUCTIONS
    assert "Recurrence:" in v3_2 and "- past —" in v3_2
    restored = v3_2.replace(mj._PROMINENCE_AND_RECURRENCE, mj._PROMINENCE_V3)
    assert restored == mj.PRECISION_V3_INSTRUCTIONS


@pytest.mark.parametrize(
    "model, judge, mismatch",
    [
        ("ongoing", "ongoing", "none"),
        ("ongoing", "recurring", "minor"),
        ("one_off", "recurring", "minor"),
        ("ongoing", "past", "material"),
        ("past", "recurring", "material"),
        ("one_off", "past", "minor"),
        ("past", "one_off", "minor"),
    ],
)
def test_recurrence_mismatch_is_material_only_across_past(model, judge, mismatch):
    assert mj.recurrence_mismatch(model, judge) == mismatch


def _verdict3_2(index, prominence="core", recurrence="ongoing"):
    v = _verdict3(index).model_dump()
    return mj.ActivityVerdictV3_2(**{**v, "prominence": prominence, "recurrence": recurrence})


def test_precision_rows_carry_model_and_judge_ratings():
    _, cand = runs()
    verdict = mj.PrecisionVerdictsV3_2(
        verdicts=[_verdict3_2(1, "secondary", "past"), _verdict3_2(2)],
        site_identity="same_organization",
        site_identity_reason="r",
    )
    rows = mj.precision_rows(cand, {"org1": verdict})
    assert rows[0]["model_prominence"] == "core" and rows[0]["judge_prominence"] == "secondary"
    assert rows[0]["model_recurrence"] == "recurring" and rows[0]["judge_recurrence"] == "past"
    # A v3.1 verdict has no recurrence of its own.
    old = mj.PrecisionVerdictsV3(
        verdicts=[_verdict3(1)], site_identity="same_organization", site_identity_reason="r"
    )
    assert mj.precision_rows(cand, {"org1": old})[0]["judge_recurrence"] is None


def test_report_compares_ratings_when_the_judge_gave_them():
    frame = pd.DataFrame(
        [
            {
                "run": "cand",
                "org_id": "o1",
                "activity_index": i,
                "outcome": "line_of_work",
                "severity": 0.0,
                "should_merge": False,
                "material_fields": "",
                "unsupported_fields": "",
                "model_prominence": mp,
                "judge_prominence": jp,
                "model_recurrence": mr,
                "judge_recurrence": jr,
            }
            for i, (mp, jp, mr, jr) in enumerate(
                [
                    ("core", "core", "ongoing", "ongoing"),
                    ("core", "minor", "ongoing", "past"),
                    ("minor", "secondary", "recurring", "ongoing"),
                    ("secondary", "secondary", "ongoing", "ongoing"),
                ],
                start=1,
            )
        ]
    )
    frame = pd.concat([frame.iloc[[0]].assign(run=mj.BASELINE_RUN), frame])
    out = mj.report(frame, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    assert "| baseline | 1 | 100.0% | 0.0% | 0.0% | 0.0% | 100.0% | 0.0% | 0.0% |" in out
    assert "| cand | 4 | 50.0% | 25.0% | 25.0% | 25.0% | 50.0% | 25.0% | 25.0% |" in out
    assert "Prominence and recurrence" not in mj.report(
        _precision_frame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    )
