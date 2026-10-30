"""D3: what the Firecrawl text does to summaries, taxonomy and tracker membership.

The fetch rate says nothing about what new text does downstream. Every
re-scraped org gets a new website summary, a new summary of summaries, a new
One Earth assignment and possibly a different membership verdict. This script
measures that shift on the vendor pilot's 2,000 climate-tracker orgs, against
a noise floor: the same in-house text run through the same chain twice.

Three chains per org, each the production code path with its production
prompt and model:

- ``firecrawl``: the org's Firecrawl text under the production text rules
  (``rescore_orgs.parquet``, written by ``rescore_text_rules.py``);
- ``inhouse``: the in-house text from the same day;
- ``inhouse_rerun``: the in-house text again, a fresh run of every stage.

Stages, in pipeline order (``enrichment_pipeline.run_pipeline`` and
climate-landscape's ``process_map_taxonomies``):

``ws``      website summary (``GENERIC_ORG_WEBSITE_PROMPT_TEXT``, gpt-4.1-mini)
``sos``     summary of summaries over Description, Description_990, the
            website summary and About LinkedIn
``rel``     relevance model, only for orgs whose description is under 100
            characters: the only orgs whose relevance input is the website
            summary
``adapt``   the Baseten adaptation/mitigation filter over the joined text fields
``oe``      One Earth hierarchical mapping of the longer of the two summaries
``emb``     text-embedding-3-large of both summaries, for cosine similarity
``judge``   gpt-4.1 pairwise preference on a sample, and a faithfulness check of
            the orgs only Firecrawl could read
``report``  measures against the noise floor, cost per 1,000 orgs, and a
            50-org sheet for human spot checks

Nothing is read from or written to the shared prompt cache: the website
summary and summary of summaries call ``openai_api_utils.get_completion``
directly, as the cache classes do, and the relevance, adaptation and One
Earth stages run their production classes with ``read_from_cache=False`` and
``write_to_cache=False``. A forced refresh with writes on would replace the
cached production summaries of these orgs. Results go to JSONL files under
``<out-dir>/d3/``, so every stage resumes where it stopped and a sizing run
with ``--limit`` is reused by the full run.

    python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.paired_llm_test \\
        --out-dir .cache/vendor_pilot --limit 10 --stages ws,sos,rel,adapt,oe
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc
from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import DEFAULT_OUT_DIR, git_sha
from vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_orgs import CFT_URI
from vdl_tools.scrape_enrich.website_junk_rules import is_junk_summary
from vdl_tools.shared_tools import climatebert_adaptation as adp
from vdl_tools.shared_tools import common_functions as cf
from vdl_tools.shared_tools.all_source_organization_summarization import BASE_SUMMARY_OF_SUMMARIES_PROMPT
from vdl_tools.shared_tools.climate_landscape.enrichment_pipeline import (
    MIN_DESCRIPTION_LENGTH,
    TEXT_FIELDS,
    choose_longer_text,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.model_caches.climate_relevance_cache import (
    CB_CD_MODEL_4OMINI,
    DEFAULT_CLIMATE_PROMPT_FORMAT,
    DEFAULT_CLIMATE_SYSTEM_PROMPT,
)
from vdl_tools.shared_tools.model_caches.relevance_cache import RelevanceCache
from vdl_tools.shared_tools.openai import openai_api_utils as oau
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL as SUMMARY_MODEL
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.taxonomy_mapping.oe_hierarchical_taxonomy_mapping import (
    MODEL as OE_MODEL,
    add_one_earth_hierarchical_taxonomy,
)
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.web_summarization.website_summarization_cache_psql import GENERIC_ORG_WEBSITE_PROMPT_TEXT

CHAINS = ("firecrawl", "inhouse", "inhouse_rerun")
STAGES = ("ws", "sos", "rel", "adapt", "oe", "emb", "judge", "report")
OE_LEVELS = (0, 1, 2, 3)
OE_TAXONOMY = Path("~/dev/vdl/shared-data-clean/data/taxonomies/oneearth/"
                   "OE Solutions Terms 20260514_expanded_VDL.xlsx").expanduser()  # climate-landscape's pin
ADAPT_MODEL_ID = adp.CPI_ADAPTATION_MODEL_2024_ID   # run_pipeline's default
ADAPT_KEEP = ("adaptation", "mitigation", "both")
EMBED_MODEL = "text-embedding-3-large"
JUDGE_MODEL = "gpt-4.1"
JUDGE_SAMPLE = 300          # paired orgs judged; plus every Firecrawl-only org
JUDGE_SOURCE_TOKENS = 6000  # each scrape shown to the judge is cut to this
SPOT_CHECKS = {"firecrawl_only": 20, "largest_length_ratio": 20, "random_paired": 10}
SEED = 7118

# Prices for models MODEL_DATA does not list, per token (OpenAI list prices).
EXTRA_PRICES = {
    CB_CD_MODEL_4OMINI: (0.30e-6, 1.20e-6),   # fine-tuned gpt-4o-mini inference
    EMBED_MODEL: (0.13e-6, 0.0),
}


# ---------------------------------------------------------------------------
# Token accounting
# ---------------------------------------------------------------------------

_STAGE = {"name": "setup"}


class Usage:
    """Tokens per (stage, model), from every Responses call on the shared client."""

    def __init__(self):
        self.lock = threading.Lock()
        self.rows: dict[tuple[str, str], list[int]] = {}

    def add(self, model: str, input_tokens: int, output_tokens: int, cached: int = 0):
        key = (_STAGE["name"], str(model))
        with self.lock:
            row = self.rows.setdefault(key, [0, 0, 0, 0])
            row[0] += 1
            row[1] += int(input_tokens or 0)
            row[2] += int(cached or 0)
            row[3] += int(output_tokens or 0)

    def frame(self) -> pd.DataFrame:
        out = []
        for (stage, model), (calls, inp, cached, outp) in sorted(self.rows.items()):
            price_in, price_out = _prices(model)
            out.append({"stage": stage, "model": model, "calls": calls, "input_tokens": inp,
                        "cached_input_tokens": cached, "output_tokens": outp,
                        # cached input billed at full price: an upper bound
                        "cost_usd": inp * price_in + outp * price_out})
        return pd.DataFrame(out)


def _prices(model: str) -> tuple[float, float]:
    if model in EXTRA_PRICES:
        return EXTRA_PRICES[model]
    data = MODEL_DATA.get(model, {})
    return data.get("input_cost_per_token", 0.0), data.get("output_cost_per_token", 0.0)


def _install_usage_hook(usage: Usage) -> None:
    responses = oau.CLIENT.responses
    original = responses.parse

    def parse(*args, **kwargs):
        response = original(*args, **kwargs)
        u = getattr(response, "usage", None)
        if u is not None:
            details = getattr(u, "input_tokens_details", None)
            usage.add(kwargs.get("model"), u.input_tokens, u.output_tokens,
                      getattr(details, "cached_tokens", 0) if details else 0)
        return response

    responses.parse = parse


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

def _order_key(org_id: str) -> str:
    return hashlib.md5(f"{SEED}:{org_id}".encode()).hexdigest()


def load_orgs(out_dir: Path, limit: int | None) -> pd.DataFrame:
    """Orgs usable on either side, with both texts and the tracker's text fields."""
    rescore = read_dataframe(out_dir / "rescore_orgs.parquet")
    rescore = rescore[rescore["usable"] | rescore["inhouse_usable"]].copy()
    cft = read_dataframe(CFT_URI, columns=["id", "Organization", "Description", "Description_990",
                                           "About LinkedIn", "Website Summary", "Summary",
                                           "all_level0_one_earth_category", "all_level1_one_earth_category",
                                           "all_level2_one_earth_category", "all_level3_one_earth_category"])
    cft = cft.drop_duplicates("id").rename(columns={"id": "org_id", "Website Summary": "cft_website_summary",
                                                    "Summary": "cft_summary"})
    orgs = rescore.merge(cft, on="org_id", how="left")
    orgs["group"] = np.select(
        [orgs["usable"] & orgs["inhouse_usable"], orgs["usable"]],
        ["paired", "firecrawl_only"], "inhouse_only")
    orgs["firecrawl_text"] = orgs["combined_text"].where(orgs["usable"])
    orgs["inhouse_text"] = orgs["inhouse_text"].where(orgs["inhouse_usable"])
    orgs = orgs.assign(_k=orgs["org_id"].map(_order_key)).sort_values("_k").drop(columns="_k")
    if limit:
        orgs = orgs.head(limit)
    orgs = orgs.rename(columns={"About LinkedIn": "about_linkedin"})
    keep = ["org_id", "Organization", "home_url", "group", "firecrawl_text", "inhouse_text", "tokens",
            "inhouse_tokens", "Description", "Description_990", "about_linkedin", "cft_website_summary",
            "cft_summary", "extractor"]
    return orgs[keep].reset_index(drop=True)


def _website_text(org, chain: str) -> str | None:
    text = org.firecrawl_text if chain == "firecrawl" else org.inhouse_text
    return text if isinstance(text, str) and text.strip() else None


# ---------------------------------------------------------------------------
# Resumable result store
# ---------------------------------------------------------------------------

class Store:
    """Append-only JSONL per stage, keyed by (chain, org_id)."""

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.rows: dict[tuple[str, str], dict] = {}
        if path.exists():
            for line in path.read_text().splitlines():
                if line.strip():
                    row = json.loads(line)
                    self.rows[(row["chain"], row["org_id"])] = row

    def __contains__(self, key) -> bool:
        return key in self.rows

    def get(self, chain: str, org_id: str) -> dict | None:
        return self.rows.get((chain, org_id))

    def put(self, row: dict) -> None:
        with self.lock:
            self.rows[(row["chain"], row["org_id"])] = row
            with self.path.open("a") as fh:
                fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _retry(fn, attempts: int = 5):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - rate limits and transient API errors
            if i == attempts - 1:
                raise
            wait = 2 ** i + random.random()
            logger.warning("retrying after %s (%.1fs)", type(exc).__name__, wait)
            time.sleep(wait)


def _parallel(tasks, fn, workers: int, label: str):
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fn, *t) for t in tasks]
        for f in as_completed(futures):
            f.result()
            done += 1
            if done % 100 == 0 or done == len(futures):
                logger.info("%s: %s/%s", label, done, len(futures))


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

def _completion(prompt: str, text: str, model: str, **kwargs):
    return _retry(lambda: oau.get_completion(prompt=prompt, model=model, text=text, return_all=True, **kwargs))


def stage_ws(orgs, store: Store, workers: int):
    def run(chain, org):
        r = _completion(GENERIC_ORG_WEBSITE_PROMPT_TEXT, _website_text(org, chain), SUMMARY_MODEL)
        store.put({"chain": chain, "org_id": org.org_id, "summary": r.output_text})
    tasks = [(c, o) for o in orgs.itertuples(index=False) for c in CHAINS
             if _website_text(o, c) and (c, o.org_id) not in store]
    _parallel(tasks, run, workers, "ws")


def _fields(org, chain: str, ws: Store) -> dict:
    """The tracker's text fields for one chain, cleaned as run_pipeline cleans them."""
    row = ws.get(chain, org.org_id)
    fields = {"Description": org.Description, "Description_990": org.Description_990,
              "Website Summary": row["summary"] if row else None, "About LinkedIn": org.about_linkedin}
    df = pd.DataFrame([fields])
    for col in TEXT_FIELDS:
        if df[col].notna().any():
            cf.clean_spaces_linebreaks_col(df, col)
    return df.iloc[0].to_dict()


def stage_sos(orgs, ws: Store, store: Store, workers: int):
    def run(chain, org):
        fields = _fields(org, chain, ws)
        texts = [fields[c] for c in TEXT_FIELDS if isinstance(fields[c], str) and fields[c]]
        text = "\n\n".join(f"Description {i + 1}\n{t}" for i, t in enumerate(texts))
        r = _completion(BASE_SUMMARY_OF_SUMMARIES_PROMPT, text, SUMMARY_MODEL)
        store.put({"chain": chain, "org_id": org.org_id, "summary": r.output_text})
    tasks = [(c, o) for o in orgs.itertuples(index=False) for c in CHAINS if (c, o.org_id) not in store]
    _parallel(tasks, run, workers, "sos")


def stage_rel(orgs, ws: Store, store: Store, workers: int):
    """Relevance for the orgs whose description is too short to be the input."""
    short = orgs[orgs["Description"].isna() | (orgs["Description"].str.len() < MIN_DESCRIPTION_LENGTH)]
    pending, inputs = [], {}
    for org in short.itertuples(index=False):
        for chain in CHAINS:
            if (chain, org.org_id) in store:
                continue
            row = ws.get(chain, org.org_id)
            summary = row["summary"] if row else None
            if summary and not is_junk_summary(summary, f"{org.Organization} {org.Description}"):
                text, kind = summary, "website_summary"
            elif isinstance(org.Description, str) and org.Description:
                text, kind = org.Description, "description"
            else:
                store.put({"chain": chain, "org_id": org.org_id, "input": "none", "prediction": None,
                           "probability": None})
                continue
            gid = f"{chain}:{org.org_id}"
            inputs[gid] = (chain, org.org_id, kind)
            pending.append((gid, text))
    if not pending:
        return
    with get_session() as session:
        cache = RelevanceCache(session=session, model=CB_CD_MODEL_4OMINI,
                               system_prompt=DEFAULT_CLIMATE_SYSTEM_PROMPT,
                               prompt_format=DEFAULT_CLIMATE_PROMPT_FORMAT,
                               prompt_name="climate_relevance_classification")
        results = cache.bulk_get_cache_or_run(pending, read_from_cache=False, write_to_cache=False,
                                              max_workers=workers)
        for gid, result in results.items():
            chain, org_id, kind = inputs[gid]
            pred, prob = cache._parse_stored_response(result)
            store.put({"chain": chain, "org_id": org_id, "input": kind, "prediction": pred,
                       "probability": prob})
        session.rollback()


def stage_adapt(orgs, ws: Store, store: Store, chunk_size: int = 50):
    api_key = get_configuration()["baseten"]["api_key"]
    deployment = adp.ensure_deployment_active(ADAPT_MODEL_ID, api_key)
    logger.info("Baseten deployment %s is %s", deployment.get("id"), deployment.get("status"))
    pending = []
    for org in orgs.itertuples(index=False):
        for chain in CHAINS:
            if (chain, org.org_id) in store:
                continue
            fields = pd.DataFrame([_fields(org, chain, ws)])
            text = cf.join_strings_no_missing(fields, list(TEXT_FIELDS)).iloc[0]
            pending.append((chain, org.org_id, text))
    with get_session() as session:
        cache = adp.AdaptationMitigationCache(session=session, api_key=api_key, model_id=ADAPT_MODEL_ID)
        for i in range(0, len(pending), chunk_size):
            chunk = pending[i:i + chunk_size]
            preds = _retry(lambda: cache._post_batch([(f"{c}:{o}", t) for c, o, t in chunk]))
            for (chain, org_id, _), pred in zip(chunk, preds):
                label = pred if isinstance(pred, str) else (pred.get("label") or pred.get("prediction"))
                store.put({"chain": chain, "org_id": org_id, "label": label, "raw": pred})
            logger.info("adapt: %s/%s", min(i + chunk_size, len(pending)), len(pending))
        session.rollback()


def stage_oe(orgs, ws: Store, sos: Store, store: Store, workers: int, chunk_size: int = 100):
    for chain in CHAINS:
        rows = []
        for org in orgs.itertuples(index=False):
            if (chain, org.org_id) in store:
                continue
            w, s = ws.get(chain, org.org_id), sos.get(chain, org.org_id)
            text = choose_longer_text({"Summary": s["summary"] if s else None,
                                       "Website Summary": w["summary"] if w else None})
            rows.append({"org_id": org.org_id, "Organization": org.Organization, "text_for_one_earth": text})
        for i in range(0, len(rows), chunk_size):
            chunk = pd.DataFrame(rows[i:i + chunk_size])
            mapped, _ = add_one_earth_hierarchical_taxonomy(
                chunk, id_col="org_id", text_col="text_for_one_earth", name_col="Organization",
                taxonomy_path=OE_TAXONOMY, emit_per_level=True, read_from_cache=False,
                write_to_cache=False, max_workers=workers)
            for r in mapped.itertuples(index=False):
                levels = {str(lv): _as_list(getattr(r, f"all_level{lv}_one_earth_category", None))
                          for lv in OE_LEVELS}
                store.put({"chain": chain, "org_id": r.org_id, "levels": levels})
            logger.info("oe %s: %s/%s", chain, min(i + chunk_size, len(rows)), len(rows))


def _as_list(value) -> list[str]:
    if value is None or (isinstance(value, float) and value != value):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            value = [v for v in value.split("|") if v] if value else []
    if isinstance(value, str):
        value = [value]
    return sorted({str(v).strip() for v in value if str(v).strip()})


def stage_emb(orgs, ws: Store, sos: Store, store: Store, batch: int = 100):
    for kind, source in (("ws", ws), ("sos", sos)):
        pending = [(c, o) for o in orgs["org_id"] for c in CHAINS
                   if source.get(c, o) and (f"{kind}:{c}", o) not in store]
        for i in range(0, len(pending), batch):
            chunk = pending[i:i + batch]
            texts = [source.get(c, o)["summary"] or " " for c, o in chunk]
            r = _retry(lambda: oau.CLIENT.embeddings.create(model=EMBED_MODEL, input=texts))
            USAGE.add(EMBED_MODEL, r.usage.total_tokens, 0)
            for (c, o), item in zip(chunk, r.data):
                store.put({"chain": f"{kind}:{c}", "org_id": o, "vector": item.embedding})


class PairwiseVerdict(BaseModel):
    preferred: Literal["1", "2", "tie"] = Field(description="Which summary describes the organization better")
    summary_1_unsupported: bool = Field(description="Summary 1 states something neither scrape supports")
    summary_2_unsupported: bool = Field(description="Summary 2 states something neither scrape supports")
    reason: str = Field(description="One sentence")


class FaithfulnessVerdict(BaseModel):
    faithful: bool = Field(description="Every claim in the summary is supported by the scrape")
    specific: bool = Field(description="It says what this organization does, beyond generic language")
    wrong_entity: bool = Field(description="The scrape is a parked, hijacked or unrelated site, not the organization")
    reason: str = Field(description="One sentence")


PAIRWISE_PROMPT = (
    "You compare two summaries of the same organization's website. You get two scrapes of the site, "
    "taken the same day by different scrapers, and two summaries, one written from each scrape. "
    "Decide which summary better describes what the organization does: accurate, specific about its "
    "work, objective rather than marketing language. Use both scrapes as evidence. Prefer neither when "
    "they are equally good. Flag any statement in a summary that neither scrape supports."
)
FAITHFULNESS_PROMPT = (
    "You check a summary of an organization's website against the scrape it was written from. "
    "Say whether every claim is supported by the scrape, whether it is specific about what the "
    "organization does, and whether the scrape is a parked, hijacked or unrelated site rather than "
    "the organization's own."
)


def stage_judge(orgs, ws: Store, store: Store, workers: int):
    rng = random.Random(SEED)
    paired = sorted(o for o in orgs.loc[orgs["group"] == "paired", "org_id"]
                    if ws.get("firecrawl", o) and ws.get("inhouse", o))
    sample = set(rng.sample(paired, min(JUDGE_SAMPLE, len(paired))))
    by_id = orgs.set_index("org_id")

    def scrape(text):
        return mc.cap_tokens(text or "", JUDGE_SOURCE_TOKENS)

    def pairwise(org_id):
        org = by_id.loc[org_id]
        flip = int(_order_key(org_id), 16) % 2 == 1   # which side is shown as "1"
        sides = [("firecrawl", org.firecrawl_text), ("inhouse", org.inhouse_text)]
        if flip:
            sides.reverse()
        text = (f"Organization: {org.Organization}\n\nScrape A:\n{scrape(sides[0][1])}\n\n"
                f"Scrape B:\n{scrape(sides[1][1])}\n\n"
                f"Summary 1:\n{ws.get(sides[0][0], org_id)['summary']}\n\n"
                f"Summary 2:\n{ws.get(sides[1][0], org_id)['summary']}")
        v = _retry(lambda: oau.get_completion(prompt=PAIRWISE_PROMPT, model=JUDGE_MODEL, text=text,
                                               text_format=PairwiseVerdict))
        side = {"1": sides[0][0], "2": sides[1][0], "tie": "tie"}[v.preferred]
        unsupported = {sides[0][0]: v.summary_1_unsupported, sides[1][0]: v.summary_2_unsupported}
        store.put({"chain": "pairwise", "org_id": org_id, "preferred": side,
                   "firecrawl_unsupported": unsupported["firecrawl"],
                   "inhouse_unsupported": unsupported["inhouse"], "reason": v.reason})

    def faithfulness(org_id):
        org = by_id.loc[org_id]
        text = (f"Organization: {org.Organization}\n\nScrape:\n{scrape(org.firecrawl_text)}\n\n"
                f"Summary:\n{ws.get('firecrawl', org_id)['summary']}")
        v = _retry(lambda: oau.get_completion(prompt=FAITHFULNESS_PROMPT, model=JUDGE_MODEL, text=text,
                                               text_format=FaithfulnessVerdict))
        store.put({"chain": "faithfulness", "org_id": org_id, **v.model_dump()})

    tasks = [(pairwise, o) for o in sorted(sample) if ("pairwise", o) not in store]
    tasks += [(faithfulness, o) for o in orgs.loc[orgs["group"] == "firecrawl_only", "org_id"]
              if ws.get("firecrawl", o) and ("faithfulness", o) not in store]
    _parallel(tasks, lambda fn, o: fn(o), workers, "judge")


USAGE = Usage()


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _cos(a, b) -> float | None:
    if a is None or b is None:
        return None
    a, b = np.asarray(a["vector"]), np.asarray(b["vector"])
    return float(a @ b / (np.linalg.norm(a) * np.linalg.norm(b)))


def _jaccard(a: list, b: list) -> float:
    a, b = set(a), set(b)
    return 1.0 if not (a | b) else len(a & b) / len(a | b)


def _boot_ci(diff: np.ndarray, n: int = 2000) -> tuple[float, float]:
    rng = np.random.default_rng(SEED)
    means = rng.choice(diff, size=(n, len(diff)), replace=True).mean(axis=1)
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def _member(rel_row, adapt_row) -> bool | None:
    """In the tracker under this chain: relevant (when relevance read the
    website summary) and labeled adaptation, mitigation or both."""
    if adapt_row is None:
        return None
    relevant = True if rel_row is None else rel_row.get("prediction") == 1
    return bool(relevant and adapt_row.get("label") in ADAPT_KEEP)


def measures(orgs, stores) -> pd.DataFrame:
    ws, sos, rel, adapt, oe, emb = (stores[k] for k in ("ws", "sos", "rel", "adapt", "oe", "emb"))
    rows = []
    for org in orgs.itertuples(index=False):
        o = org.org_id
        row = {"org_id": o, "group": org.group, "tokens_firecrawl": org.tokens,
               "tokens_inhouse": org.inhouse_tokens}
        for kind in ("ws", "sos"):
            row[f"{kind}_cos_vendor"] = _cos(emb.get(f"{kind}:firecrawl", o), emb.get(f"{kind}:inhouse", o))
            row[f"{kind}_cos_noise"] = _cos(emb.get(f"{kind}:inhouse", o), emb.get(f"{kind}:inhouse_rerun", o))
        f, i, r = (oe.get(c, o) for c in CHAINS)
        for lv in OE_LEVELS:
            if f and i and r:
                row[f"oe_l{lv}_jaccard_vendor"] = _jaccard(f["levels"][str(lv)], i["levels"][str(lv)])
                row[f"oe_l{lv}_jaccard_noise"] = _jaccard(i["levels"][str(lv)], r["levels"][str(lv)])
        rf, ri, rr = (rel.get(c, o) for c in CHAINS)
        if rf and ri and rr:
            row["rel_flip_vendor"] = rf["prediction"] != ri["prediction"]
            row["rel_flip_noise"] = ri["prediction"] != rr["prediction"]
        af, ai, ar = (adapt.get(c, o) for c in CHAINS)
        if af and ai and ar:
            row["adapt_flip_vendor"] = (af["label"] in ADAPT_KEEP) != (ai["label"] in ADAPT_KEEP)
            row["adapt_flip_noise"] = (ai["label"] in ADAPT_KEEP) != (ar["label"] in ADAPT_KEEP)
            mf, mi, mr = _member(rf, af), _member(ri, ai), _member(rr, ar)
            row["member_firecrawl"], row["member_inhouse"] = mf, mi
            row["member_flip_vendor"] = mf != mi
            row["member_flip_noise"] = mi != mr
        rows.append(row)
    return pd.DataFrame(rows)


def _compare(df: pd.DataFrame, name: str, vendor: str, noise: str, higher_is_closer: bool) -> dict | None:
    d = df[[vendor, noise]].dropna().astype(float)
    if d.empty:
        return None
    diff = (d[vendor] - d[noise]).to_numpy()
    lo, hi = _boot_ci(diff)
    if higher_is_closer:   # similarity: share of orgs below the noise floor's 5th percentile
        beyond = float((d[vendor] < d[noise].quantile(0.05)).mean())
    else:                  # flip rate: no per-org threshold
        beyond = float("nan")
    return {"measure": name, "n": len(d), "vendor": d[vendor].mean(), "noise": d[noise].mean(),
            "diff": diff.mean(), "ci_low": lo, "ci_high": hi, "beyond_noise_p05": beyond}


def report(orgs, stores, out: Path, rescore_lineage: dict) -> str:
    m = measures(orgs, stores)
    write_dataframe(m, out / "d3_measures.parquet", lineage=_lineage(rescore_lineage))
    tables = []
    for group in ("paired", "firecrawl_only"):
        g = m[m["group"] == group]
        rows = [
            _compare(g, "website summary cosine", "ws_cos_vendor", "ws_cos_noise", True),
            _compare(g, "summary of summaries cosine", "sos_cos_vendor", "sos_cos_noise", True),
            *[_compare(g, f"One Earth level {lv} Jaccard", f"oe_l{lv}_jaccard_vendor",
                       f"oe_l{lv}_jaccard_noise", True) for lv in OE_LEVELS],
            _compare(g, "relevance flip (short descriptions)", "rel_flip_vendor", "rel_flip_noise", False),
            _compare(g, "adaptation filter flip", "adapt_flip_vendor", "adapt_flip_noise", False),
            _compare(g, "tracker membership flip", "member_flip_vendor", "member_flip_noise", False),
        ]
        t = pd.DataFrame([r for r in rows if r])
        if not t.empty:
            tables.append((group, t))

    judge = stores["judge"]
    pair = pd.DataFrame([r for (c, _), r in judge.rows.items() if c == "pairwise"])
    faith = pd.DataFrame([r for (c, _), r in judge.rows.items() if c == "faithfulness"])
    usage = _usage_table(out)
    spot = spot_checks(orgs, stores, m, out)

    lines = [f"## D3 paired LLM test (run {datetime.now(timezone.utc).date().isoformat()})", "",
             f"Firecrawl text: `{rescore_lineage.get('extractor')}`, CLEANER_VERSION "
             f"{rescore_lineage.get('cleaner_version')}, rescore at git {str(rescore_lineage.get('git_sha'))[:10]}. "
             f"This run at git {git_sha()[:10]}. Orgs: "
             + ", ".join(f"{k} {v}" for k, v in orgs["group"].value_counts().items()) + ".", "",
             "`vendor` compares the Firecrawl chain with the in-house chain; `noise` compares the in-house "
             "chain with a fresh rerun of it. `diff` is vendor minus noise with a 95% bootstrap interval. "
             "For similarities, `beyond_noise_p05` is the share of orgs whose vendor similarity is below "
             "the 5th percentile of the noise floor.", ""]
    for group, t in tables:
        lines += [f"### {group}", "", t.to_markdown(index=False, floatfmt=".3f"), ""]
    if not pair.empty:
        lines += ["### Judge: which website summary describes the org better (gpt-4.1, paired sample)", "",
                  pair["preferred"].value_counts().rename_axis("preferred").to_frame("orgs").to_markdown(), "",
                  f"Unsupported statements flagged: Firecrawl {pair['firecrawl_unsupported'].mean():.1%}, "
                  f"in-house {pair['inhouse_unsupported'].mean():.1%} (n={len(pair)}).", ""]
    if not faith.empty:
        lines += ["### Judge: Firecrawl-only orgs, summary against its scrape", "",
                  f"Faithful {faith['faithful'].mean():.1%}, specific {faith['specific'].mean():.1%}, "
                  f"wrong entity {faith['wrong_entity'].mean():.1%} (n={len(faith)}).", ""]
    if not usage.empty:
        lines += ["### Cost", "", usage.to_markdown(index=False, floatfmt=".4f"), ""]
    lines += [f"Human spot-check sheet: `{spot}` ({len(pd.read_excel(spot))} orgs).", ""]
    text = "\n".join(lines)
    (out / "d3_summary.md").write_text(text)
    return text


def spot_checks(orgs, stores, m: pd.DataFrame, out: Path) -> Path:
    rng = random.Random(SEED)
    ws, sos, judge = stores["ws"], stores["sos"], stores["judge"]
    fc_only = sorted(orgs.loc[orgs["group"] == "firecrawl_only", "org_id"])
    paired = orgs[orgs["group"] == "paired"].copy()
    paired["ratio"] = (np.log((paired["tokens"] + 1) / (paired["inhouse_tokens"] + 1))).abs()
    pick = rng.sample(fc_only, min(SPOT_CHECKS["firecrawl_only"], len(fc_only)))
    pick += list(paired.sort_values("ratio", ascending=False)["org_id"].head(SPOT_CHECKS["largest_length_ratio"]))
    rest = sorted(set(paired["org_id"]) - set(pick))
    pick += rng.sample(rest, min(SPOT_CHECKS["random_paired"], len(rest)))
    by_id = orgs.set_index("org_id")
    rows = []
    for o in pick:
        org = by_id.loc[o]
        get = lambda store, c: (store.get(c, o) or {}).get("summary")  # noqa: E731
        verdict = judge.get("pairwise", o) or judge.get("faithfulness", o) or {}
        rows.append({
            "org_id": o, "organization": org.Organization, "website": org.home_url, "group": org.group,
            "firecrawl_tokens": org.tokens, "inhouse_tokens": org.inhouse_tokens,
            "firecrawl_website_summary": get(ws, "firecrawl"), "inhouse_website_summary": get(ws, "inhouse"),
            "firecrawl_summary_of_summaries": get(sos, "firecrawl"), "inhouse_summary_of_summaries": get(sos, "inhouse"),
            "judge": json.dumps({k: v for k, v in verdict.items() if k not in ("chain", "org_id")}),
            "human_better (firecrawl / inhouse / tie)": "", "human_firecrawl_accurate (y/n)": "",
            "human_inhouse_accurate (y/n)": "", "human_notes": "",
        })
    path = out / "d3_spot_checks.xlsx"
    pd.DataFrame(rows).to_excel(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def _usage_table(out: Path) -> pd.DataFrame:
    """Cost per stage over every run so far, and per 1,000 orgs re-run."""
    path = out / "usage.jsonl"
    if not path.exists():
        return pd.DataFrame()
    df = pd.DataFrame([json.loads(x) for x in path.read_text().splitlines() if x.strip()])
    agg = df.groupby(["stage", "model"], as_index=False)[
        ["items", "calls", "input_tokens", "output_tokens", "cost_usd"]].sum()
    agg["cost_per_1000_items"] = 1000 * agg["cost_usd"] / agg["items"].clip(lower=1)
    return agg


def _lineage(rescore_lineage: dict) -> dict:
    return {"created_by": "vdl_tools.scrape_enrich.scraper.firecrawl.validation.paired_llm_test",
            "git_sha": git_sha(), "rescore": {k: rescore_lineage.get(k) for k in
                                              ("git_sha", "cleaner_version", "extractor", "org_token_cap")},
            "models": {"summary": SUMMARY_MODEL, "relevance": CB_CD_MODEL_4OMINI, "one_earth": OE_MODEL,
                       "adaptation": ADAPT_MODEL_ID, "embedding": EMBED_MODEL, "judge": JUDGE_MODEL},
            "one_earth_taxonomy": OE_TAXONOMY.name, "cft_uri": CFT_URI}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="first N orgs in a fixed shuffled order")
    parser.add_argument("--stages", default=",".join(STAGES))
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args(argv)
    stages = [s for s in args.stages.split(",") if s]
    unknown = set(stages) - set(STAGES)
    if unknown:
        parser.error(f"unknown stages {sorted(unknown)}")

    out = args.out_dir / "d3"
    out.mkdir(parents=True, exist_ok=True)
    from vdl_tools.shared_tools.parquet_cache import get_lineage
    rescore_lineage = get_lineage(args.out_dir / "rescore_orgs.parquet") or {}
    orgs = load_orgs(args.out_dir, args.limit)
    logger.info("D3 over %s orgs: %s", len(orgs), orgs["group"].value_counts().to_dict())
    stores = {k: Store(out / f"{k}.jsonl") for k in ("ws", "sos", "rel", "adapt", "oe", "emb", "judge")}
    _install_usage_hook(USAGE)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    calls = {
        "ws": lambda: stage_ws(orgs, stores["ws"], args.workers),
        "sos": lambda: stage_sos(orgs, stores["ws"], stores["sos"], args.workers),
        "rel": lambda: stage_rel(orgs, stores["ws"], stores["rel"], args.workers),
        "adapt": lambda: stage_adapt(orgs, stores["ws"], stores["adapt"]),
        "oe": lambda: stage_oe(orgs, stores["ws"], stores["sos"], stores["oe"], args.workers),
        "emb": lambda: stage_emb(orgs, stores["ws"], stores["sos"], stores["emb"]),
        "judge": lambda: stage_judge(orgs, stores["ws"], stores["judge"], args.workers),
    }
    for stage in stages:
        if stage == "report":
            print(report(orgs, stores, out, rescore_lineage))
            continue
        _STAGE["name"] = stage
        before_rows, before_usage = len(stores[stage].rows), USAGE.frame()
        t0 = time.monotonic()
        calls[stage]()
        items = len(stores[stage].rows) - before_rows
        after = USAGE.frame()
        delta = after[after["stage"] == stage].copy()
        if not before_usage.empty:
            prev = before_usage[before_usage["stage"] == stage].set_index("model")
            for col in ("calls", "input_tokens", "cached_input_tokens", "output_tokens", "cost_usd"):
                delta[col] = delta[col] - delta["model"].map(prev[col]).fillna(0)
        with (out / "usage.jsonl").open("a") as fh:
            for r in (delta.to_dict("records") or [{"stage": stage, "model": "none", "calls": 0,
                                                    "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}]):
                fh.write(json.dumps({**r, "run_id": run_id, "items": items, "orgs": len(orgs),
                                     "seconds": round(time.monotonic() - t0, 1)}) + "\n")
        logger.info("stage %s: %s new results in %.0fs, $%.4f", stage, items, time.monotonic() - t0,
                    float(delta["cost_usd"].sum()) if not delta.empty else 0.0)


if __name__ == "__main__":
    main()
