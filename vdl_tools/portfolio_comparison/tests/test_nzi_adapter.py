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
