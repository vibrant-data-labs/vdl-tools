import pytest

from vdl_tools.portfolio_comparison.matching.source_adapter import (
    Candidate,
    ChainedSourceClient,
    NZIClient,
)


def nzi_hit(cid, name, website, pitch="Does climate things."):
    return {"clientID": cid, "name": name, "website": website, "pitchLine": pitch}


def make_client(monkeypatch, results):
    client = NZIClient()
    monkeypatch.setattr(client, "_search_api", lambda name: results)
    return client


def test_nzi_domain_confirmed_hit_wins_and_drops_noise(monkeypatch):
    client = make_client(monkeypatch, [
        nzi_hit(1, "Solaris", "https://other-solaris.io"),
        nzi_hit(2, "Solaris Energy", "https://solarisenergy.com"),
    ])
    cands = client.search("Solaris Energy", "solarisenergy.com")
    # Domain-confirmed hit makes same-name noise irrelevant.
    assert [c.matched_id for c in cands] == ["2"]
    assert cands[0].evidence["signal"] == "domain"
    assert cands[0].score == 0.97
    assert cands[0].evidence["nzi"] is True


def test_nzi_name_only_hits_never_domain_grade(monkeypatch):
    client = make_client(monkeypatch, [nzi_hit(3, "Tidal Power", "https://tidal.example")])
    cands = client.search("Tidal Power", None)
    # Exact name may score 1.0 but stays name-signal — never auto-grade.
    assert cands[0].evidence["signal"] == "name"


def test_nzi_blank_name_skips_api(monkeypatch):
    client = NZIClient()
    monkeypatch.setattr(client, "_search_api",
                        lambda name: pytest.fail("should not call API"))
    assert client.search("", "solarisenergy.com") == []


def test_chained_client_prefers_first_source_with_hits():
    class Stub:
        def __init__(self, source, hits, fail=False):
            self.source, self.hits, self.fail = source, hits, fail

        def search(self, name, url):
            if self.fail:
                raise RuntimeError("no credentials")
            return self.hits

    def cand(cid, signal):
        return Candidate(matched_id=cid, matched_name="X", matched_url="",
                         score=0.9, method="api_search", evidence={"signal": signal})

    cb_domain = cand("cb-1", "domain")
    nzi_name = cand("nzi-1", "name")
    nzi_domain = cand("nzi-2", "domain")

    # NZI empty -> CB answers
    chain = ChainedSourceClient([Stub("nzi", []), Stub("crunchbase", [cb_domain])])
    assert chain.search("X", None) == [cb_domain]
    assert chain.source == "nzi+crunchbase"
    # NZI fails hard -> CB still answers
    chain = ChainedSourceClient([Stub("nzi", [], fail=True), Stub("crunchbase", [cb_domain])])
    assert chain.search("X", None) == [cb_domain]
    # NZI name-noise must NOT shadow CB's domain-confirmed hit
    chain = ChainedSourceClient([Stub("nzi", [nzi_name]), Stub("crunchbase", [cb_domain])])
    assert chain.search("X", None) == [cb_domain]
    # NZI domain-confirmed wins outright; CB never consulted
    chain = ChainedSourceClient([
        Stub("nzi", [nzi_domain]),
        Stub("crunchbase", [], fail=True),
    ])
    assert chain.search("X", None) == [nzi_domain]
    # Neither has domain evidence -> merged, preferred source first
    chain = ChainedSourceClient([
        Stub("nzi", [nzi_name]),
        Stub("crunchbase", [cand("cb-2", "name")]),
    ])
    assert [c.matched_id for c in chain.search("X", None)] == ["nzi-1", "cb-2"]


def test_supplement_nzi_ids_domain_confirmed_only():
    import pandas as pd
    from vdl_tools.portfolio_comparison.matching.tier2 import supplement_nzi_ids
    from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS

    def mapping(rows):
        df = pd.DataFrame(rows)
        for col in ID_MAPPING_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        return df[ID_MAPPING_COLUMNS]

    def nzi_cand(cid, signal):
        return Candidate(matched_id=cid, matched_name="Solaris", matched_url="",
                         score=0.97, method="api_search", evidence={"signal": signal})

    class StubNZI:
        source = "nzi"

        def __init__(self, results):
            self.results, self.calls = results, []

        def search(self, name, url):
            self.calls.append((name, url))
            return self.results.get(name, [])

    m = mapping([
        # CB-matched -> gets searched, domain-confirmed -> nzi_id filled
        {"customer_row_id": "r1", "entity_type": "for_profit", "customer_name": "Solaris",
         "matched_id": "80d35449-95fa-9c6e-2d83-1bdd92227246",
         "matched_name": "Solaris", "matched_url": "https://solaris.com"},
        # already NZI-matched -> skipped
        {"customer_row_id": "r2", "entity_type": "for_profit", "customer_name": "Windward",
         "matched_id": "12345", "matched_name": "Windward", "matched_url": "https://w.io"},
        # CB-matched but NZI only has name-signal hits -> NOT filled
        {"customer_row_id": "r3", "entity_type": "for_profit", "customer_name": "Tidal",
         "matched_id": "9926bec5-645a-46b7-8281-518acaccad30",
         "matched_name": "Tidal", "matched_url": "https://tidal.com"},
    ])
    client = StubNZI({
        "Solaris": [nzi_cand("777", "domain")],
        "Tidal": [nzi_cand("888", "name")],
    })
    out = supplement_nzi_ids(m, client)
    assert out[out.customer_row_id == "r1"].iloc[0]["nzi_id"] == "777"
    assert pd.isna(out[out.customer_row_id == "r2"].iloc[0]["nzi_id"])
    assert pd.isna(out[out.customer_row_id == "r3"].iloc[0]["nzi_id"])
    assert ("Windward", "w.io") not in [(n, u) for n, u in client.calls]


def test_name_variants_ladder():
    from vdl_tools.portfolio_comparison.intake.normalize import name_variants

    assert name_variants("chifoods.us") == ["chifoods.us", "chifoods", "chifoods us"]
    assert name_variants("Buzz Power Banks") == ["Buzz Power Banks"]
    assert name_variants("M.A.R.S.H. Project")[-1] == "M A R S H Project"
    assert name_variants(None) == []


def test_cb_autocomplete_fallback_with_domain_promotion(monkeypatch):
    from vdl_tools.portfolio_comparison.matching.source_adapter import CrunchbaseClient

    client = CrunchbaseClient()
    stages = []

    def fake_query(filters):
        stages.append("query")
        return []  # domain_eq and contains both miss

    def fake_autocomplete(name):
        stages.append(f"auto:{name}")
        if name == "CycleWatt":
            return [{"uuid": "cb-cyclo", "identifier": {"value": "CycloWatt", "permalink": "cyclowatt"},
                     "website_url": "https://cyclewatt.com", "short_description": "EV charging"}]
        return []

    monkeypatch.setattr(client, "_query", fake_query)
    monkeypatch.setattr(client, "_autocomplete", fake_autocomplete)
    cands = client.search("CycleWatt", "https://cyclewatt.com")
    assert cands[0].matched_id == "cb-cyclo"
    # Autocomplete hit whose website exact-hosts the customer domain is
    # promoted to domain-grade evidence.
    assert cands[0].evidence["signal"] == "domain"
    assert cands[0].score == 0.97
