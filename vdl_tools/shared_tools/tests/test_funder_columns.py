import pandas as pd

from vdl_tools.shared_tools.funders.columns import build_funder_columns, collect_funders

CLEVELAND_CB = "11111111-1111-1111-1111-111111111111"


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
    df = build_funder_columns(frame(), min_occurrences=1)
    assert df.loc[0, "Funder_Ids"] == ["ein:340714585", "ein:566040787"]
    assert df.loc[1, "Funder_Ids"] == ["ein:340714585", "ein:233018741"]
    assert df.loc[0, "Funders"] == df.loc[1, "Funders"][:1] + ["SMITH FAMILY FOUNDATION"]


def test_a_funder_linked_to_one_org_is_pruned_and_no_org_is_dropped():
    df = build_funder_columns(frame(), min_occurrences=2)
    assert len(df) == 4
    assert df.loc[2, "Funders"] == []
    assert df.loc[0, "Funder_Ids"] == ["ein:340714585"]


def test_names_with_no_id_record_get_a_name_id():
    df = pd.DataFrame({"Investors": [["Acme Capital"], ["acme capital"]], "Donors": [[], []],
                       "Investors Data": [None, None], "granters": [None, None]})
    out = build_funder_columns(df, min_occurrences=2)
    assert out.loc[0, "Funder_Ids"] == ["name:acme-capital"]
    assert out.loc[1, "Funders"] == ["Acme Capital"]


def test_a_leftover_name_joins_the_one_funder_with_its_key_and_gives_it_a_mixed_case_name():
    df = frame()
    df.loc[2, "Donors"] = ["COMMUNITY FOUNDATION OF", "The Cleveland Foundation"]  # not in this org's granters
    df.loc[0, "Donors"] = ["THE CLEVELAND FOUNDATION", "SMITH FAMILY FOUNDATION", "Cleveland Foundation"]
    df.at[3, "Investors"], df.at[3, "Investors Data"] = [], []  # the one funder with this key is the EIN
    out = build_funder_columns(df, min_occurrences=1)
    assert out.loc[2, "Funder_Ids"][-1] == "ein:340714585"
    assert out.loc[0, "Funders"][0] == "The Cleveland Foundation"
    assert not any(i.startswith("name:") for ids in out["Funder_Ids"] for i in ids)


def test_a_leftover_name_shared_by_several_funders_gets_a_name_id():
    df = frame()
    df.loc[2, "Donors"] = ["COMMUNITY FOUNDATION OF", "Smith Family Foundation"]
    out = build_funder_columns(df, min_occurrences=1)
    assert "name:smith-family-foundation" in out.loc[2, "Funder_Ids"]


def test_a_leftover_that_is_the_cut_off_start_of_a_funder_on_the_same_org_is_dropped():
    df = frame()
    df.loc[2, "Donors"] = ["Community Foundation"]
    out = build_funder_columns(df, min_occurrences=1)
    assert out.loc[2, "Funders"] == ["COMMUNITY FOUNDATION OF MIDDLESEX COUNTY INC"]


def test_funders_that_share_a_name_get_their_state_added():
    states = {"ein:566040787": "NC", "ein:233018741": "OH"}
    out = build_funder_columns(frame(), min_occurrences=1, state_of=lambda ids: pd.Series([states.get(i) for i in ids]))
    names = dict(zip(out.loc[0, "Funder_Ids"], out.loc[0, "Funders"]))
    assert names["ein:566040787"] == "SMITH FAMILY FOUNDATION (NC)"
    assert dict(zip(out.loc[1, "Funder_Ids"], out.loc[1, "Funders"]))["ein:233018741"] == "SMITH FAMILY FOUNDATION (OH)"
    assert names["ein:340714585"] == "THE CLEVELAND FOUNDATION"


def test_a_shared_name_with_no_known_state_keeps_its_name():
    out = build_funder_columns(frame(), min_occurrences=1, state_of=lambda ids: pd.Series([None] * len(ids)))
    assert out.loc[0, "Funders"][1] == "SMITH FAMILY FOUNDATION"
