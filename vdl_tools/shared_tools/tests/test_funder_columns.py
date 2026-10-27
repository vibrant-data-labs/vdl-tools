import pandas as pd

from vdl_tools.scrape_enrich.display_name import PROMPT, PROMPT_FILED_NAME, PROMPT_SHARED_SITE
from vdl_tools.scrape_enrich import display_name
from vdl_tools.shared_tools.funders.columns import build_funder_columns, collect_funders, funder_names, pick_funder_names

CLEVELAND_CB = "11111111-1111-1111-1111-111111111111"


def columns(df, **kwargs):
    return build_funder_columns(df, funder_names(df, **kwargs)["label"])


def attributes(ids, state=None, website=None):
    """What ``attributes_of`` returns: ``state`` and ``website`` for each id, ``None`` where unknown."""
    state, website = state or {}, website or {}
    return pd.DataFrame(
        {"state": [state.get(i) for i in ids], "website": [website.get(i) for i in ids]},
        index=pd.Index(ids, name="funder_id"),
    )


def granter(ein, name1, name2=None, dba=None):
    return {"ein": ein, "businessname1": name1, "businessname2": name2, "dba_name": dba}


def frame():
    cleveland = granter("340714585", "THE CLEVELAND FOUNDATION")
    smith_nc = granter("566040787", "SMITH FAMILY FOUNDATION")
    smith_oh = granter("233018741", "SMITH FAMILY FOUNDATION")
    return pd.DataFrame(
        {
            "uid": ["gt-1", "gt-2", "gt-3", "cb-1"],
            "Investors": [[], [], [], ["The Cleveland Foundation"]],
            "Investors Data": [
                [], [], [], [{"name": "The Cleveland Foundation", "uuid": CLEVELAND_CB, "entity_def_id": "organization"}],
            ],
            "Donors": [
                ["THE CLEVELAND FOUNDATION", "SMITH FAMILY FOUNDATION"],
                ["THE CLEVELAND FOUNDATION", "SMITH FAMILY FOUNDATION"],
                ["COMMUNITY FOUNDATION OF"],
                [],
            ],
            "granters": [
                [cleveland, smith_nc], [cleveland, smith_oh], [granter("061477711", "COMMUNITY FOUNDATION OF", "MIDDLESEX COUNTY INC")], [],
            ],
        }
    )


def test_collect_funders_gives_each_source_its_own_id():
    long = collect_funders(frame())
    assert set(long["funder_id"]) == {
        f"cb:{CLEVELAND_CB}", "ein:340714585", "ein:566040787", "ein:233018741", "ein:061477711",
    }
    assert "COMMUNITY FOUNDATION OF MIDDLESEX COUNTY INC" in set(long["name"])


def test_two_eins_with_one_name_stay_two_funders():
    df = columns(frame(), min_occurrences=1)
    assert df.loc[0, "Funder_Ids"] == ["ein:340714585", "ein:566040787"]
    assert df.loc[1, "Funder_Ids"] == ["ein:340714585", "ein:233018741"]
    assert df.loc[0, "Funders"] == df.loc[1, "Funders"][:1] + ["SMITH FAMILY FOUNDATION"]


def test_a_funder_linked_to_one_org_is_pruned_and_no_org_is_dropped():
    df = columns(frame(), min_occurrences=2)
    assert len(df) == 4
    assert df.loc[2, "Funders"] == []
    assert df.loc[0, "Funder_Ids"] == ["ein:340714585"]


def test_names_with_no_id_record_get_a_name_id():
    df = pd.DataFrame({"Investors": [["Acme Capital"], ["acme capital"]], "Donors": [[], []],
                       "Investors Data": [None, None], "granters": [None, None]})
    out = columns(df, min_occurrences=2)
    assert out.loc[0, "Funder_Ids"] == ["name:acme-capital"]
    assert out.loc[1, "Funders"] == ["Acme Capital"]


def test_a_leftover_name_joins_the_one_funder_with_its_key_and_gives_it_a_mixed_case_name():
    df = frame()
    df.loc[2, "Donors"] = ["COMMUNITY FOUNDATION OF", "The Cleveland Foundation"]  # not in this org's granters
    df.loc[0, "Donors"] = ["THE CLEVELAND FOUNDATION", "SMITH FAMILY FOUNDATION", "Cleveland Foundation"]
    df.at[3, "Investors"], df.at[3, "Investors Data"] = [], []  # the one funder with this key is the EIN
    out = columns(df, min_occurrences=1)
    assert out.loc[2, "Funder_Ids"][-1] == "ein:340714585"
    assert out.loc[0, "Funders"][0] == "The Cleveland Foundation"
    assert not any(i.startswith("name:") for ids in out["Funder_Ids"] for i in ids)


def test_a_leftover_name_shared_by_several_funders_gets_a_name_id():
    df = frame()
    df.loc[2, "Donors"] = ["COMMUNITY FOUNDATION OF", "Smith Family Foundation"]
    out = columns(df, min_occurrences=1)
    assert "name:smith-family-foundation" in out.loc[2, "Funder_Ids"]


def test_a_leftover_that_is_the_cut_off_start_of_a_funder_on_the_same_org_is_dropped():
    df = frame()
    df.loc[2, "Donors"] = ["Community Foundation"]
    out = columns(df, min_occurrences=1)
    assert out.loc[2, "Funders"] == ["COMMUNITY FOUNDATION OF MIDDLESEX COUNTY INC"]


def test_funders_that_share_a_name_get_their_state_added():
    states = {"ein:566040787": "NC", "ein:233018741": "OH"}
    out = columns(frame(), min_occurrences=1, attributes_of=lambda ids: attributes(ids, state=states))
    names = dict(zip(out.loc[0, "Funder_Ids"], out.loc[0, "Funders"]))
    assert names["ein:566040787"] == "SMITH FAMILY FOUNDATION (NC)"
    assert dict(zip(out.loc[1, "Funder_Ids"], out.loc[1, "Funders"]))["ein:233018741"] == "SMITH FAMILY FOUNDATION (OH)"
    assert names["ein:340714585"] == "THE CLEVELAND FOUNDATION"


def test_the_state_is_in_the_label_but_not_in_the_display_name():
    states = {"ein:566040787": "NC", "ein:233018741": "OH"}
    names = funder_names(frame(), min_occurrences=1, attributes_of=lambda ids: attributes(ids, state=states))
    assert names.loc["ein:566040787"].to_dict() == {
        "display_name": "SMITH FAMILY FOUNDATION", "label": "SMITH FAMILY FOUNDATION (NC)",
    }
    assert names.index.name == "funder_id"


def test_a_shared_name_with_no_known_state_keeps_its_name():
    out = columns(frame(), min_occurrences=1, attributes_of=lambda ids: attributes(ids))
    assert out.loc[0, "Funders"][1] == "SMITH FAMILY FOUNDATION"


class FakePicker:
    """Stands in for ``pick_display_names``: records each call and answers from ``answers``."""

    def __init__(self, answers, confidence="high", source="legal_name"):
        self.answers, self.confidence, self.source, self.calls = answers, confidence, source, []

    def __call__(self, rows, session, name_col, id_col, prompt=None, **kwargs):
        self.calls.append({"prompt": prompt, "rows": rows.copy(), **kwargs})
        found = [(i, self.answers[i]) for i in rows[id_col] if i in self.answers]
        return pd.DataFrame(
            {id_col: [i for i, _ in found], "display_name": [a for _, a in found],
             "confidence": self.confidence, "source": self.source}
        )


def test_a_gt_funder_with_a_dba_or_website_goes_to_the_organization_prompt(monkeypatch):
    picker = FakePicker({"ein:111111111": "Zest Education", "ein:222222222": "Plain Foundation"})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = pd.Series({"ein:111111111": "THE OPPORTUNITY TRUST", "ein:222222222": "PLAIN FOUNDATION"})
    out = pick_funder_names(
        names,
        dbas=pd.Series({"ein:111111111": "ZEST EDUCATION"}),
        websites=pd.Series({"ein:111111111": "https://www.zesteducation.org/"}),
        session=None,
    )
    by_prompt = {c["prompt"]: list(c["rows"]["funder_id"]) for c in picker.calls}
    assert by_prompt == {PROMPT: ["ein:111111111"], PROMPT_FILED_NAME: ["ein:222222222"]}
    org_call = next(c for c in picker.calls if c["prompt"] == PROMPT)
    assert org_call["rows"]["website"].tolist() == ["zesteducation.org"]
    assert org_call["rows"]["dba"].tolist() == ["ZEST EDUCATION"]
    assert out.to_dict() == {"ein:111111111": "Zest Education", "ein:222222222": "Plain Foundation"}


def test_funders_that_share_a_website_get_the_shared_site_prompt_with_their_siblings(monkeypatch):
    picker = FakePicker({"ein:111111111": "Mercy Health North"})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = pd.Series({"ein:111111111": "MERCY HEALTH NORTH", "ein:222222222": "MERCY HEALTH SOUTH"})
    websites = pd.Series({"ein:111111111": "mercy.net", "ein:222222222": "https://www.mercy.net"})
    pick_funder_names(names, pd.Series(dtype=object), websites, session=None)
    (call,) = [c for c in picker.calls if c["prompt"] == PROMPT_SHARED_SITE]
    assert call["siblings_col"] == "siblings"
    row = call["rows"].set_index("funder_id")["siblings"]
    assert row["ein:111111111"] == ["MERCY HEALTH SOUTH"]
    assert row["ein:222222222"] == ["MERCY HEALTH NORTH"]


def test_a_low_confidence_answer_is_not_used(monkeypatch):
    monkeypatch.setattr(display_name, "pick_display_names", FakePicker({"ein:111111111": "Somebody Else"}, confidence="low"))
    names = pd.Series({"ein:111111111": "SHETOME CORP"})
    out = pick_funder_names(names, pd.Series({"ein:111111111": "KATHERINE S & FARRA ALFORD CORP"}), pd.Series(dtype=object), None)
    assert out.empty


def test_a_crunchbase_funder_is_not_sent_for_its_website_only_its_casing(monkeypatch):
    picker = FakePicker({"cb:11111111-1111-1111-1111-111111111111": "Acme Ventures"})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = pd.Series({"cb:11111111-1111-1111-1111-111111111111": "ACME VENTURES", "cb:22222222-2222-2222-2222-222222222222": "Mixed Case Fund"})
    websites = pd.Series({"cb:11111111-1111-1111-1111-111111111111": "acme.vc", "cb:22222222-2222-2222-2222-222222222222": "mixed.vc"})
    pick_funder_names(names, pd.Series(dtype=object), websites, None)
    assert [c["prompt"] for c in picker.calls] == [PROMPT_FILED_NAME]
    assert list(picker.calls[0]["rows"]["funder_id"]) == ["cb:11111111-1111-1111-1111-111111111111"]


def test_a_filed_name_answer_that_replaces_a_word_is_not_used(monkeypatch):
    monkeypatch.setattr(display_name, "pick_display_names", FakePicker({"ein:111111111": "Hofer Family Foundation"}))
    names = pd.Series({"ein:111111111": "HOEFER FAMILY FOUNDATION"})
    assert pick_funder_names(names, pd.Series(dtype=object), pd.Series(dtype=object), None).empty


def test_the_dba_of_a_granter_reaches_the_model_through_build_funder_columns(monkeypatch):
    picker = FakePicker({"ein:566040787": "Smith Family Fund"})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    df = frame()
    df.at[0, "granters"] = [granter("340714585", "THE CLEVELAND FOUNDATION"), granter("566040787", "SMITH FAMILY FOUNDATION", None, "SMITH FAMILY FUND")]
    out = columns(df, min_occurrences=1, session=object(), attributes_of=lambda ids: attributes(ids))
    org_call = next(c for c in picker.calls if c["prompt"] == PROMPT)
    assert org_call["rows"]["dba"].tolist() == ["SMITH FAMILY FUND"]
    assert "Smith Family Fund" in out.loc[0, "Funders"]


def test_a_dba_with_no_website_and_no_shared_word_is_not_used_and_the_name_is_recased(monkeypatch):
    picker = FakePicker({"ein:111111111": "H & N BLECHNER MEMORIAL FUND"}, source="dba")
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = pd.Series({"ein:111111111": "JANE CAROL BRESSLER CHARITABLE FUND"})
    out = pick_funder_names(names, pd.Series({"ein:111111111": "H & N BLECHNER MEMORIAL FUND"}), pd.Series(dtype=object), None)
    assert [c["prompt"] for c in picker.calls] == [PROMPT, PROMPT_FILED_NAME]
    assert out.empty  # the filed-name answer (the fake's) drops a word, so it is refused too


def test_a_dba_confirmed_by_a_website_is_used(monkeypatch):
    monkeypatch.setattr(display_name, "pick_display_names", FakePicker({"ein:111111111": "The Long Center"}, source="dba"))
    names = pd.Series({"ein:111111111": "GREATER AUSTIN PERFORMING ARTS CENTER INC"})
    out = pick_funder_names(names, pd.Series({"ein:111111111": "THE LONG CENTER"}), pd.Series({"ein:111111111": "thelongcenter.org"}), None)
    assert out.to_dict() == {"ein:111111111": "The Long Center"}


def test_scraped_text_is_shown_to_the_model_for_funders_that_have_it(monkeypatch):
    picker = FakePicker({"ein:111111111": "Zest Education"})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = pd.Series({"ein:111111111": "THE OPPORTUNITY TRUST", "ein:222222222": "OTHER FOUNDATION"})
    websites = pd.Series({"ein:111111111": "zest.org", "ein:222222222": "other.org"})
    texts = pd.Series({"ein:111111111": "Zest Education supports schools."})
    pick_funder_names(names, pd.Series(dtype=object), websites, None, texts=texts)
    org_call = next(c for c in picker.calls if c["prompt"] == PROMPT)
    assert org_call["text_col"] == "text"
    texts_sent = org_call["rows"].set_index("funder_id")["text"]
    assert texts_sent["ein:111111111"] == "Zest Education supports schools."
    assert pd.isna(texts_sent["ein:222222222"])


def test_a_placeholder_or_platform_website_is_not_evidence(monkeypatch):
    picker = FakePicker({})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = pd.Series({"ein:111111111": "PLAIN FOUNDATION", "ein:222222222": "OTHER FOUNDATION"})
    websites = pd.Series({"ein:111111111": "N/A", "ein:222222222": "https://www.facebook.com/otherfoundation"})
    pick_funder_names(names, pd.Series(dtype=object), websites, None)
    assert [c["prompt"] for c in picker.calls] == [PROMPT_FILED_NAME]
