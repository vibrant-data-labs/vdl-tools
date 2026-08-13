"""Benchmark candidate summarization models against the current baseline.

Runs the real production prompt (``GENERIC_ORG_WEBSITE_PROMPT_TEXT``) over real
scraped orgs through several models on the Vercel AI Gateway, then grades the
results two ways: free deterministic checks on every summary, and a blind
pairwise comparison against the baseline by a stronger judge.

Deliberately standalone: it does not touch ``PromptResponseCacheSQL``,
``openai_api_utils.CLIENT``, or ``MODEL_DATA``. Nothing here changes production
behavior -- the point is to pick a model before building any routing.

How grading works
-----------------
The workload is input-bound: ``make_group_text`` packs scraped pages until it
fills the context window, then asks for a 100-500 token summary. Cost is driven
by input tokens, and the quality risk is *constraint adherence* rather than
reasoning.

Two layers, chosen so neither duplicates the other:

1. **Deterministic checks** (free, no API calls) for pipeline-breaking defects.
   These need an absolute *rate* -- "leaks the delimiter in 3 of 20 runs" -- which
   a relative comparison can never give you, and they'd read as a tie if both
   summaries were equally broken.
2. **Blind pairwise judgment** for everything subjective: which summary is
   better, whether claims are supported by the source, and two product-visible
   flags. Subjective checks like marketing tone are deliberately *not* scored
   separately -- a head-to-head handles them better and cheaper.

The judge sees the source text, so faithfulness is always measured. Order is
randomized and model identities stripped, so position and identity bias can't
favor either side.

Candidate models are called through Vercel so their latency shares a network
path. The judge is called against OpenAI directly -- it isn't a candidate, so
there's nothing to hold constant, and a direct call is one less hop on the
comparisons the decision rests on. That means this needs OPENAI_API_KEY in
addition to the Vercel key, unless you pass --no-judge.

Usage
-----
    python -m vdl_tools.shared_tools.web_summarization.model_benchmark --limit 20

    # or from a CSV with source/subpath/text columns, no database needed
    python -m vdl_tools.shared_tools.web_summarization.model_benchmark --csv pages.csv

Requires ``[vercel] api_key`` in config.ini (or ``AI_GATEWAY_API_KEY`` in env).
"""

import argparse
import json
import os
import pathlib as pl
import random
import time

import pandas as pd
import tiktoken
from openai import OpenAI

# Capture the real key BEFORE the placeholder below, otherwise a missing key
# turns into a confusing 401 from the judge instead of a clear startup error.
_REAL_OPENAI_KEY = os.getenv("OPENAI_API_KEY")

# openai_api_utils builds a module-level OpenAI client at import time and raises
# without a key -- and make_page_text imports it transitively. Candidate models
# all run through Vercel and never touch that client, so a placeholder is enough
# to get through the import.
os.environ.setdefault("OPENAI_API_KEY", "unused-by-this-benchmark")

from vdl_tools.shared_tools.openai.openai_api_utils import contains_i_am
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.text_cleaning import (
    check_for_repeating_sequences,
    clean_scraped_text,
)
from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text
from vdl_tools.shared_tools.web_summarization.website_summarization_cache_psql import (
    GENERIC_ORG_WEBSITE_PROMPT_TEXT,
)

BASE_URL = "https://ai-gateway.vercel.sh/v1"

# Everything routes through Vercel -- including the baseline -- so latency
# comparisons share a network path instead of pitting Vercel against direct
# OpenAI. Prices are USD per 1M tokens; verify against your Vercel dashboard,
# which is authoritative and where actual spend shows up.
# Keys are benchmark labels, not necessarily model ids: an entry may set
# "model_id" to call the same underlying model under a different configuration,
# and "reasoning" to set reasoning.effort for that variant. That's how
# hy3-nothink compares against hy3 without needing two separate runs.
MODELS = {
    "openai/gpt-4.1-mini": {"in": 0.40, "out": 1.60, "context": 1_047_576, "baseline": True},
    "openai/gpt-oss-120b": {"in": 0.10, "out": 0.50, "context": 128_000},
    # Best summaries measured, and unusable: 89% of its output tokens are hidden
    # reasoning, giving 61% wasted spend and ~10x the latency of every other
    # model for a 233-token summary.
    #
    # That reasoning CANNOT be switched off through Vercel. Verified against four
    # mechanisms: reasoning.effort (accepted, no effect at any level), hy3's
    # native reasoning_effort="no_think" (rejected 400 by the gateway's enum
    # validation), extra_body enable_thinking=False (forwarded, ignored), and
    # max_output_tokens (the model spends the whole budget thinking and returns
    # empty output). The gateway does not pass provider-native parameters
    # through. A "hy3-nothink" variant was tried and removed -- it produced
    # results identical to plain hy3.
    "tencent/hy3": {"in": 0.14, "out": 0.58, "context": 262_144},
    # Same weights, pinned to the one provider that serves hy3 without reasoning.
    # Measured on a real 6,475-token org: 0 reasoning tokens vs ~2,500 on novita,
    # 9.4s vs 52.7s, and a LONGER visible summary (325 vs 180 tokens) -- so it is
    # not truncating. Open question this exists to answer: hy3's 0-fabrication
    # result was measured WITH reasoning, so non-reasoning hy3 may not hold it.
    "tencent/hy3-deepinfra": {
        "model_id": "tencent/hy3", "provider": "deepinfra",
        "in": 0.14, "out": 0.58, "context": 262_144,
    },
    # Was "inclusionai/ling-3.0-flash-free", retired from the gateway mid-project
    # (a run 404'd on every call). Replaced by a paid variant plus a smaller
    # "-tiny-free". Model slugs are not stable: pin what you benchmark, and
    # re-check the catalog before trusting a saved model list.
    "inclusionai/ling-3.0-flash": {"in": 0.06, "out": 0.18, "context": 256_000},
    # 7.9B MoE with only 1.3B active per token -- by far the smallest model here,
    # and what now occupies the free slot ling-3.0-flash-free used to hold.
    # Tests where quality breaks as models get smaller.
    "inclusionai/ling-3.0-tiny-free": {"in": 0.0, "out": 0.0, "context": 256_000},

    # Round 2. Selection rule earned the hard way: non-reasoning by construction
    # or shipping a separate non-reasoning slug, so none can repeat hy3's problem
    # of unavoidable thinking overhead. All four verified at 0 reasoning tokens.
    "alibaba/qwen3-next-80b-a3b-instruct": {"in": 0.15, "out": 1.20, "context": 131_072},
    "deepseek/deepseek-v4-flash": {"in": 0.20, "out": 0.40, "context": 1_000_000},
    # The non-reasoning sibling of v4-flash (its pair is deepseek-v3.2-thinking).
    # v4-flash was the best round-2 result but spends ~370 tokens thinking per
    # call, and that cannot be switched off through Vercel: reasoning.effort,
    # the documented `thinking: disabled`, and max_output_tokens all fail --
    # the last by consuming the whole budget on reasoning and returning nothing.
    # v3.2 measures 0 reasoning tokens at 1.7s, so it sidesteps the problem.
    "deepseek/deepseek-v3.2": {"in": 0.28, "out": 0.42, "context": 128_000},
    "zai/glm-4.7-flash": {"in": 0.07, "out": 0.40, "context": 200_000},
    "mistral/ministral-8b": {"in": 0.15, "out": 0.15, "context": 128_000},
}


def refresh_prices_from_gateway(client) -> int:
    """Overwrite MODELS pricing and context from the gateway's own catalog.

    Hardcoded prices go stale and are easy to get wrong -- JUDGE_PRICE was 5x off
    for a while, silently inflating every cost estimate. The /models endpoint
    reports pricing per token and the real context window, so treat it as the
    source of truth and keep the literals above only as an offline fallback.
    """
    updated = 0
    try:
        for entry in client.models.list().data:
            cfg = MODELS.get(entry.id)
            if not cfg:
                continue
            pricing = getattr(entry, "pricing", None) or {}
            if isinstance(pricing, dict) and pricing.get("input") is not None:
                # Reported per token; MODELS stores per million.
                cfg["in"] = float(pricing["input"]) * 1_000_000
                cfg["out"] = float(pricing.get("output", 0)) * 1_000_000
                updated += 1
            ctx = getattr(entry, "context_window", None)
            if ctx:
                cfg["context"] = int(ctx)
    except Exception as e:
        logger.warning("Could not refresh prices from gateway (%s); using hardcoded values", type(e).__name__)
    return updated

# A stronger, neutral model -- not a candidate, so nothing judges its own work.
# Called against OpenAI directly rather than through Vercel: one less hop on the
# calls the decision actually rests on. Note the slug has no "openai/" prefix,
# which is a Vercel routing convention, not part of the model name.
# luna is the cheapest of the three gpt-5.6 tiers (sol > terra > luna), which
# OpenAI positions for high-volume work rather than careful judgment. It does
# reason -- a sweep showed effort is honored, ~2x tokens at high vs medium -- and
# its verdicts held up when spot-checked against source. Judge v2 asks more of it
# though, so terra (gpt-5.6-terra, $2/$12) is the upgrade if verdicts look shaky.
JUDGE_MODEL = "gpt-5.6-luna"
# "high", not "medium": an effort sweep on this model showed medium (62 reasoning
# tokens) is indistinguishable from none (63), while high roughly doubles it
# (124). Only high actually engages the model differently, and the extra output
# tokens cost cents per run.
JUDGE_EFFORT = "high"
# Direct-OpenAI list price. A 2x input / 1.5x output surcharge applies above 272K
# input tokens, which --judge-source-tokens (40K default) keeps us well under.
JUDGE_PRICE = {"in": 0.20, "out": 1.20}

JUDGE_PROMPT = """You compare two summaries of the same organization, both written from the same scraped website text. You will see the SOURCE TEXT, then SUMMARY A and SUMMARY B.

Work in order: assess each summary on its own, then compare, then pick a winner. Do not decide the winner first and justify it afterwards.

Faithfulness rubric (per summary). The axis is CONFIDENCE, not harm — confident synthesis is valuable, guessing is not:
- "faithful": Every claim is either stated in the source, or an inference so strongly supported by the source that it is effectively certain. Condensing, generalizing, and restating in different words are all fine. An organization that describes its member network, grant programs, and charitable mission is a nonprofit even if the source never uses that word — calling it one is faithful, not inferred.
- "unsupported_inference": States something plausible but genuinely uncertain — a reader would be misinformed if it turned out to be wrong. A nonprofit label on an entity whose type is actually ambiguous belongs here; the same label on an obvious charity does not.
- "fabricated": States a specific with no origin in the source — a place, date, number, proper name, or quantity that does not appear there and cannot be derived from it. A date differing from the source's date, or a region the source never mentions, is fabricated regardless of how plausible it sounds.

Facet coverage. These summaries feed an automated taxonomy mapper that may only assign a category when the summary states language specific enough to act on — it is forbidden from inferring. Count a facet as covered only when the summary gives SPECIFIC, MAPPABLE language, not a passing mention. "Works in education" is not a sector statement a taxonomy can use; "administers the state's NC Pre-K program for children from birth to five" is. Judge each facet against that bar:
- activities: the concrete things the organization does, named specifically.
- beneficiaries: who it serves or affects, identified rather than implied.
- sector: the domain, specific enough to distinguish it from neighbouring fields.
- geography: named places or defined regions, not "various locations".
- mechanism: how it operates — grants, research, advocacy, direct service, manufacturing, convening.

Impact, judged separately from faithfulness — an error can be real and still not matter. Apply one test: **would an automated taxonomy assign this organization a different category, or would a reader act differently?**
- "material": yes to either. The error changes the classification, or changes a decision someone would make about the organization.
- "none": no to both. The error is real but the organization would be classified identically and understood the same way. Opening hours, a hedged count, a job title, a slightly imprecise date.
Do not mark "material" merely because you flagged the claim — you have already established the claim is unsupported, and this is a separate question about consequence. Report "none" when the summary is fully faithful.

Strictness:
- The organization's NAME is not evidence. "Riverbend Land Trust" does not establish that it does land conservation; the summary text must say so.
- **Invention vs over-generalization.** If every element of a claim appears in the source and only its scope or subject is stretched, that is "unsupported_inference", not "fabricated". Reserve "fabricated" for an element that is absent from the source entirely, or for a specific that contradicts it.
- A location or category that the source's own contents make effectively certain is faithful, even if never named — the same standard applied to organization type applies to geography and sector.
- The source may be truncated, so a missing detail is not automatically fabricated. But a specific with no plausible origin anywhere in the source is fabricated, not inferred.
- Sources sometimes contradict themselves. If the summary follows one of two conflicting statements in the source, that is faithful — the summary is not responsible for the source's inconsistency.
- Judge only what the summaries say. Do not reward a summary for being longer.

Worked examples, drawn from real cases and verified against source text:

{"source_text": "...projects in West Virginia, California, and Texas...", "summary": "...implemented in multiple U.S. states (including West Virginia, California, Texas, and Louisiana)...", "faithfulness": "fabricated", "impact": "material"}

{"source_text": "You can save threatened rainforest and help support a 1.6 million acre national park in Namibia, Africa, through our Adopt an Acre program", "summary": "...Adopt an Acre protects rainforest in Namibia...", "faithfulness": "fabricated", "impact": "material"}

{"source_text": "...partners include local watershed councils and a university host site...", "summary": "...partners include the U.S. Forest Service...", "faithfulness": "fabricated", "impact": "material"}

{"source_text": "Monday, Wednesday & Friday 9:30-12:30 ... Tuesday, Thursday 3:30-6:30 ... Saturday, Sunday 9:30-11:30", "summary": "...a market that operates five days a week...", "faithfulness": "fabricated", "impact": "none"}

{"source_text": "ClimateWells West Virginia methane and hydrogen sulfide well closure project has earned an A rating from BeZero Carbon", "summary": "...ClimateWells' methodology has earned high-integrity ratings, such as an 'A' rating from BeZero Carbon...", "faithfulness": "unsupported_inference", "impact": "material"}

{"source_text": "...operators across the United States and Canada...", "summary": "...scaling its impact across the United States and Canada...", "faithfulness": "unsupported_inference", "impact": "material"}

{"source_text": "...our 2,850 partner organizations including schools nationwide...", "summary": "...partners with over 2,850 organizations...", "faithfulness": "unsupported_inference", "impact": "none"}

{"source_text": "...celebrates the thirtieth anniversary of our founding... 2025-2028 strategic plan...", "summary": "...Since its founding in the early 1990s...", "faithfulness": "unsupported_inference", "impact": "none"}

{"source_text": "...has fostered partnerships for more than 30 years...", "summary": "...founded more than 30 years ago...", "faithfulness": "unsupported_inference", "impact": "none"}

{"source_text": "...serves 1.1K+ institutions...", "summary": "...over 1,100 partner institutions...", "faithfulness": "faithful", "impact": "none"}

{"source_text": "...network of member organizations, affiliates, and grantees... administers grant programs...", "summary": "...a national nonprofit organization...", "faithfulness": "faithful", "impact": "none"}

Choosing a winner. Weigh, in this order:
1. Faithfulness — a fabricated specific outweighs any writing quality.
2. Facet coverage — more mappable facets is more useful downstream.
3. Clarity and neutrality — objective description, not promotional copy.
Declare "tie" whenever the two are genuinely close. Ties are expected and useful; do not force a winner to seem decisive.

The order of A and B is random and carries no information. Be impartial."""

FACETS = ["activities", "beneficiaries", "sector", "geography", "mechanism"]
FAITHFULNESS_LEVELS = ["faithful", "unsupported_inference", "fabricated"]
IMPACT_LEVELS = ["none", "material"]

# Derived in code rather than asked of the model: categorical judgment is what
# models are reliable at, while a numeric rating has no stable basis and drifts
# between calls. Deriving it here also means re-weighting is a dict edit and a
# recompute over existing CSVs, not another full judging pass.
#
# Two dimensions because type and consequence are independent: a market's
# opening days can be flatly contradicted (fabricated) and still change nothing
# a reader would act on, while a stretched credential claim invents nothing yet
# misrepresents the organization.
SEVERITY = {
    ("faithful", "none"): 0.0,
    ("faithful", "material"): 0.0,
    ("unsupported_inference", "none"): 0.5,
    ("unsupported_inference", "material"): 1.0,
    ("fabricated", "none"): 1.0,
    ("fabricated", "material"): 3.0,
}


def severity_of(faithfulness, impact) -> float:
    """Look up severity, defaulting unknown impact to the harsher reading."""
    if not isinstance(faithfulness, str):
        return 0.0
    key = (faithfulness, impact if impact in IMPACT_LEVELS else "material")
    return SEVERITY.get(key, 0.0)


def _side_schema(side: str) -> dict:
    """Per-summary assessment fields for one side of the comparison.

    Ordering matters: structured output emits fields in declaration order, so
    each `_notes` field is generated before the verdict it supports. That's what
    makes this chain-of-thought rather than a label with a story attached.
    """
    return {
        f"{side}_faithfulness_notes": {
            "type": "string",
            "description": f"Check summary {side.upper()}'s specific claims against the source. Reason before labelling.",
        },
        f"{side}_faithfulness": {
            "type": "string",
            "enum": FAITHFULNESS_LEVELS,
            "description": "Verdict following the faithfulness rubric.",
        },
        f"{side}_impact": {
            "type": "string",
            "enum": IMPACT_LEVELS,
            "description": (
                "Would an automated taxonomy assign a different category, or would a reader "
                "act differently? 'material' if yes to either, 'none' if no to both. "
                "Most errors are 'none'. 'none' if fully faithful."
            ),
        },
        f"{side}_unsupported_claims": {
            "type": "string",
            "description": "The specific unsupported or fabricated claims, quoted. Empty string if faithful.",
        },
        f"{side}_facet_notes": {
            "type": "string",
            "description": "Which facets are explicitly stated, and which are missing. Reason before listing.",
        },
        f"{side}_facets_covered": {
            "type": "array",
            "items": {"type": "string", "enum": FACETS},
            "description": "Facets explicitly stated in the summary text.",
        },
    }


JUDGE_SCHEMA = {
    "type": "object",
    "properties": {
        **_side_schema("a"),
        **_side_schema("b"),
        "comparison_notes": {
            "type": "string",
            "description": (
                "Weigh the assessments above in order: faithfulness, then facet "
                "coverage, then clarity. End with a one-sentence conclusion."
            ),
        },
        "winner": {
            "type": "string",
            "enum": ["A", "B", "tie"],
            "description": "Which summary is better overall, following the weighting order. 'tie' when genuinely close.",
        },
    },
    "required": (
        list(_side_schema("a")) + list(_side_schema("b")) + ["comparison_notes", "winner"]
    ),
    "additionalProperties": False,
}

# Approximate: tiktoken has no encoding for non-OpenAI models, so counts for
# hy3/ling/gpt-oss are estimates. Good enough for relative comparison; trust the
# Vercel dashboard for real billing.
ENCODING = tiktoken.get_encoding("o200k_base")


def count_tokens(text: str) -> int:
    return len(ENCODING.encode(text or ""))


def truncate(text: str, max_tokens: int) -> str:
    tokens = ENCODING.encode(text or "")
    if len(tokens) <= max_tokens:
        return text
    return ENCODING.decode(tokens[:max_tokens])


def get_client() -> OpenAI:
    key = os.getenv("AI_GATEWAY_API_KEY")
    if not key:
        for path in (None, pl.Path.cwd() / "config.ini", pl.Path.cwd().parent / "config.ini"):
            try:
                cfg = get_configuration(path)
                if cfg and "vercel" in cfg:
                    key = cfg["vercel"]["api_key"]
                    break
            except Exception:
                continue
    if not key:
        raise SystemExit(
            "No Vercel key. Set AI_GATEWAY_API_KEY or add [vercel] api_key to config.ini"
        )
    return OpenAI(api_key=key, base_url=BASE_URL, max_retries=2)


def get_judge_client() -> OpenAI:
    """Judge client, pointed at OpenAI directly (no base_url override)."""
    key = _REAL_OPENAI_KEY
    if not key:
        # config.ini has no [openai] section today, but honor it if one is added.
        for path in (None, pl.Path.cwd() / "config.ini", pl.Path.cwd().parent / "config.ini"):
            try:
                cfg = get_configuration(path)
                if cfg and "openai" in cfg:
                    key = cfg["openai"]["openai_api_key"]
                    break
            except Exception:
                continue
    if not key:
        raise SystemExit(
            "The judge calls OpenAI directly and needs a real key.\n"
            "  export OPENAI_API_KEY=sk-...\n"
            "Or run with --no-judge to skip the pairwise comparison."
        )
    return OpenAI(api_key=key, max_retries=2)


def load_pages_from_db(limit: int, min_pages: int) -> pd.DataFrame:
    """Pull scraped pages for orgs that have enough content to be worth summarizing."""
    from sqlalchemy import select, func
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
        WebPagesScraped,
    )

    with get_session() as session:
        # Pick orgs first, then fetch all their pages -- summarization is
        # per-org, so a partial page set would misrepresent the input size.
        org_q = (
            select(WebPagesScraped.home_url)
            .where(WebPagesScraped.parsed_html.isnot(None))
            .group_by(WebPagesScraped.home_url)
            .having(func.count(WebPagesScraped.cleaned_key) >= min_pages)
            .limit(limit)
        )
        orgs = [r[0] for r in session.execute(org_q).all()]
        if not orgs:
            raise SystemExit(
                f"No orgs found with >= {min_pages} scraped pages. "
                "Try --min-pages 1, or use --csv."
            )

        rows = session.execute(
            select(
                WebPagesScraped.home_url,
                WebPagesScraped.subpath,
                WebPagesScraped.parsed_html,
                WebPagesScraped.page_type,
            ).where(WebPagesScraped.home_url.in_(orgs))
        ).all()

    return pd.DataFrame(
        [
            {"source": r[0], "subpath": r[1], "text": r[2], "type": r[3]}
            for r in rows
            if r[2]
        ]
    )


# Hostname normalization, mirroring extract_website_name: drop scheme, drop www,
# drop path, lowercase. Applied to both sides so project websites join to
# scraped home_urls despite formatting differences.
def _norm_sql(col: str) -> str:
    return (
        f"lower(split_part(regexp_replace(regexp_replace({col}, "
        f"'^https?://', ''), '^www\\.', ''), '/', 1))"
    )


def discover_strata(session) -> dict[str, str]:
    """Map project family -> the largest `organization` snapshot schema.

    Project schemas are dashboard snapshots (ed_tracker_v2_586b82,
    ed_tracker_v2_a82068, ...), several per project. Hardcoding a hash would go
    stale on the next dashboard build, so pick the biggest per family instead.
    """
    from collections import defaultdict
    from sqlalchemy import text

    schemas = [
        r[0] for r in session.execute(text("""
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = 'organization' AND table_schema <> 'public'
        """)).all()
    ]
    families = defaultdict(list)
    for sch in schemas:
        families[sch.split("_v2_")[0] if "_v2_" in sch else sch.rsplit("_", 1)[0]].append(sch)

    chosen = {}
    for family, candidates in families.items():
        best, best_n = None, -1
        for sch in candidates:
            try:
                n = session.execute(text(f'SELECT COUNT(*) FROM "{sch}".organization')).scalar()
            except Exception:
                session.rollback()
                continue
            if n > best_n:
                best, best_n = sch, n
        if best:
            chosen[family] = best
    return chosen


def load_pages_stratified(limit: int, min_pages: int, seed: int) -> pd.DataFrame:
    """Sample evenly across (project, data_source) strata.

    An unstratified sample is dominated by whichever project scraped most, and
    Giving Tuesday nonprofits read very differently from Crunchbase startups --
    so a model that handles one well may not handle the other. Sampling evenly
    and labelling each org lets the report break results down per stratum.
    """
    from sqlalchemy import text
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    with get_session() as session:
        strata = discover_strata(session)
        if not strata:
            raise SystemExit("No project organization tables found; use --csv or drop --stratify.")

        # Enumerate populated strata first so allocation reflects what exists.
        found = []
        for family, schema in sorted(strata.items()):
            rows = session.execute(text(f"""
                SELECT COALESCE(data_source, '(none)'), COUNT(*)
                FROM "{schema}".organization
                WHERE website IS NOT NULL AND website <> ''
                GROUP BY 1 HAVING COUNT(*) >= 10
            """)).all()
            for data_source, n in rows:
                found.append((family, schema, data_source, n))

        # Project tables hold only orgs that PASSED the relevance filter
        # (enrichment_pipeline run_pipeline: prediction_relevant == 1, then
        # summarize). But summarization also runs BEFORE relevance, inside
        # prepare_for_relevance_model, for orgs whose description is missing or
        # too short -- and those include orgs later rejected as irrelevant:
        # junk sites, parked domains, wrong entities. Sampling only from project
        # tables would exclude exactly the population where refusals and
        # hallucination happen, so carry it as its own stratum.
        found.append((None, None, "unattributed (pre-relevance / rejected)", None))

        per_stratum = max(1, limit // max(len(found), 1))
        print(f"\nsampling {per_stratum} org(s) from each of {len(found)} strata:")

        selected = []
        for family, schema, data_source, n_total in found:
            if schema is None:
                # Anti-join: scraped orgs absent from every project's org table.
                union = " UNION ".join(
                    f'SELECT {_norm_sql("website")} AS k FROM "{s}".organization '
                    f"WHERE website IS NOT NULL AND website <> ''"
                    for s in strata.values()
                )
                rows = session.execute(text(f"""
                    WITH known AS ({union}),
                    scraped AS (
                        SELECT {_norm_sql('home_url')} AS k, MIN(home_url) AS home_url
                        FROM web_pages_scraped WHERE parsed_html IS NOT NULL
                        GROUP BY 1 HAVING COUNT(*) >= :min_pages
                    )
                    SELECT s.home_url FROM scraped s
                    WHERE NOT EXISTS (SELECT 1 FROM known WHERE known.k = s.k)
                    ORDER BY md5(s.home_url || :seed)
                    LIMIT :n
                """), {"min_pages": min_pages, "seed": str(seed), "n": per_stratum}).all()
                print(f"  {data_source:<40} {len(rows):>3}")
                for (home_url,) in rows:
                    selected.append({"source": home_url, "stratum": data_source})
                continue

            rows = session.execute(text(f"""
                WITH scraped AS (
                    SELECT {_norm_sql('home_url')} AS k, MIN(home_url) AS home_url,
                           COUNT(*) AS n_pages
                    FROM web_pages_scraped WHERE parsed_html IS NOT NULL
                    GROUP BY 1 HAVING COUNT(*) >= :min_pages
                )
                SELECT s.home_url
                FROM "{schema}".organization o
                JOIN scraped s ON s.k = {_norm_sql('o.website')}
                WHERE o.website <> ''
                  AND COALESCE(o.data_source, '(none)') = :ds
                -- Deterministic pseudo-random pick: stable across runs for a
                -- given seed, without ORDER BY random() re-shuffling each time.
                GROUP BY s.home_url
                ORDER BY md5(s.home_url || :seed)
                LIMIT :n
            """), {"min_pages": min_pages, "ds": data_source, "seed": str(seed), "n": per_stratum}).all()

            label = f"{family}/{data_source}"
            print(f"  {label:<40} {len(rows):>3} of {n_total:,}")
            for (home_url,) in rows:
                selected.append({"source": home_url, "stratum": label})

        if not selected:
            raise SystemExit("Stratified sampling matched no orgs with scraped pages.")

        # An org can appear in two projects (drawdown and oneearth both draw on
        # Candid), so the same URL may be selected twice. Keep the first label
        # rather than the last, so a run is reproducible for a given seed --
        # this is why 6 strata x N can yield slightly fewer than 6N orgs.
        strata_by_url = {}
        for r in selected:
            strata_by_url.setdefault(r["source"], r["stratum"])

        # The judge's worked examples are drawn from these orgs, so judging them
        # measures recall of answers it was handed. Held out here as well as in
        # --rejudge, otherwise a fresh stratified run silently reintroduces them.
        contaminated = [
            u for u in strata_by_url if any(e in u.lower() for e in EXAMPLE_ORGS)
        ]
        for u in contaminated:
            del strata_by_url[u]
        if contaminated:
            print(f"  holding out {len(contaminated)} org(s) used in the judge's examples")
        # = ANY(:urls) takes a plain list; an IN clause would need expanding
        # bindparams for the same result.
        page_rows = session.execute(text("""
            SELECT home_url, subpath, parsed_html, page_type
            FROM web_pages_scraped
            WHERE home_url = ANY(:urls) AND parsed_html IS NOT NULL
        """), {"urls": list(strata_by_url)}).all()

    return pd.DataFrame([
        {"source": r[0], "subpath": r[1], "text": r[2], "type": r[3],
         "stratum": strata_by_url.get(r[0], "?")}
        for r in page_rows if r[2]
    ])


def load_pages_for_sources(sources) -> pd.DataFrame:
    """Fetch pages for an explicit list of orgs, for --rejudge.

    Re-deriving the source rather than storing it keeps 40K-token blobs out of
    the results CSV. Packing is deterministic, so the same orgs reproduce the
    same input the judge saw originally -- verified against saved full_tokens.
    """
    from sqlalchemy import text
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    with get_session() as session:
        rows = session.execute(text("""
            SELECT home_url, subpath, parsed_html, page_type
            FROM web_pages_scraped
            WHERE home_url = ANY(:urls) AND parsed_html IS NOT NULL
        """), {"urls": list(sources)}).all()
    return pd.DataFrame(
        [{"source": r[0], "subpath": r[1], "text": r[2], "type": r[3]} for r in rows if r[2]]
    )


# Orgs whose cases appear in JUDGE_PROMPT's worked examples. Rejudging these
# measures recall of answers the judge was handed, not its judgment -- so a
# subsampled rejudge holds them out and evaluates on unseen orgs only.
EXAMPLE_ORGS = [
    "climatewells",          # Louisiana, US/Canada scope, BeZero attribution
    "savenature",            # rainforest in Namibia, over 2,850, founded 30+ years
    "deschuteslandtrust",    # invented U.S. Forest Service partner
    "skagitgleaners",        # five days a week vs seven-day schedule
    "estuaries",             # early 1990s from thirtieth anniversary, nonprofit
    "caresolace",            # over 1,100 from "1.1K+"
]


# Judge-produced columns are dropped when reloading a previous run: they were
# written by whatever rubric was current then, and keeping them would silently
# mix old verdicts into a new report.
STALE_JUDGE_COLUMNS = [
    "unfaithful", "not_english", "mentions_website",
    "faithfulness", "impact", "severity", "facets_covered", "facet_count",
    "unsupported_claims",
]


def load_for_rejudge(
    path: str, max_input_tokens: int, limit: int = None, seed: int = 0,
    keep_models: list[str] = None,
) -> tuple[pd.DataFrame, list[dict], list[str]]:
    """Reload summaries from a previous run so only the judging is redone.

    Rubric changes are only attributable if the text being judged holds still.
    Models are not cached, so a fresh run would change the summaries and the
    rubric at once -- this holds the summaries fixed and re-runs the judge alone.
    """
    summaries = pd.read_csv(path)
    missing = {"model", "source", "summary"} - set(summaries.columns)
    if missing:
        raise SystemExit(f"{path} is missing required columns: {sorted(missing)}")

    summaries = summaries.drop(columns=[c for c in STALE_JUDGE_COLUMNS if c in summaries])
    if "error" not in summaries:
        summaries["error"] = None

    if keep_models:
        # Rejudging every saved model is often waste -- some are already ruled
        # out on grounds a quality number can't rescue. The baseline must stay
        # regardless, since every comparison is against it.
        baseline = next((m for m, c in MODELS.items() if c.get("baseline")), None)
        keep = set(keep_models) | ({baseline} if baseline else set())
        dropped = sorted(set(summaries["model"].unique()) - keep)
        summaries = summaries[summaries["model"].isin(keep)]
        if dropped:
            print(f"skipping {len(dropped)} saved model(s): {', '.join(dropped)}")

    sources = sorted(summaries["source"].dropna().unique())
    if limit:
        # Hold out orgs the prompt's examples were drawn from: scoring well on
        # those would measure recall, not judgment.
        held_out = [s for s in sources if any(e in s.lower() for e in EXAMPLE_ORGS)]
        eligible = [s for s in sources if s not in held_out]
        if held_out:
            print(f"holding out {len(held_out)} org(s) used in the prompt's examples")

        if limit < len(eligible):
            # Deterministic by org, so two rubric revisions run at the same
            # --limit and --seed cover identical orgs and are diffable. All
            # models for a selected org are kept; dropping one would break the
            # pairwise structure.
            import hashlib

            eligible = sorted(
                eligible, key=lambda s: hashlib.md5(f"{s}{seed}".encode()).hexdigest()
            )[:limit]
        sources = eligible
        summaries = summaries[summaries["source"].isin(sources)]
        print(f"subsampled to {len(sources)} unseen orgs (seed {seed})")
    pages_df = load_pages_for_sources(sources)
    if pages_df.empty:
        raise SystemExit("No scraped pages found for the sources in that file.")

    # Carry stratum labels across so per-stratum reporting still works.
    strata = (
        summaries.dropna(subset=["stratum"]).drop_duplicates("source").set_index("source")["stratum"]
        if "stratum" in summaries else pd.Series(dtype=str)
    )
    if not strata.empty:
        pages_df["stratum"] = pages_df["source"].map(strata)

    inputs = build_inputs(pages_df, max_input_tokens)

    # The scrape cache is mutable; if pages changed since the original run the
    # judge would be comparing old summaries against new source without saying so.
    if "full_tokens" in summaries:
        saved = summaries.drop_duplicates("source").set_index("source")["full_tokens"].to_dict()
        drifted = [
            i["source"] for i in inputs
            if i["source"] in saved and abs(i["full_tokens"] - saved[i["source"]]) > 1
        ]
        if drifted:
            logger.warning(
                "Source text changed since the saved run for %d org(s), e.g. %s. "
                "Those comparisons judge old summaries against new source.",
                len(drifted), drifted[:3],
            )

    models = sorted(summaries["model"].dropna().unique())
    print(f"\nrejudging {len(summaries)} saved summaries: {len(inputs)} orgs x {len(models)} models")
    print(f"  models: {', '.join(models)}")
    return summaries, inputs, models


def build_inputs(pages_df: pd.DataFrame, max_input_tokens: int) -> list[dict]:
    """Clean, pack, and truncate each org's pages into one prompt input.

    Packing happens once and the result is shared by every model, so quality
    differences are attributable to the model rather than to different inputs.
    Truncation is applied uniformly for the same reason -- and keeps large orgs
    from blowing past the smallest candidate's context window.
    """
    pages_df = pages_df.copy()
    pages_df["text"] = pages_df["text"].apply(clean_scraped_text)

    inputs = []
    for source, group in pages_df.groupby("source"):
        packed = make_group_text(GENERIC_ORG_WEBSITE_PROMPT_TEXT, group)
        if not packed:
            logger.warning("No usable text for %s, skipping", source)
            continue

        full_tokens = count_tokens(packed)
        inputs.append(
            {
                "source": source,
                "text": truncate(packed, max_input_tokens),
                "n_pages": len(group),
                "full_tokens": full_tokens,
                "truncated": full_tokens > max_input_tokens,
                # Present only under --stratify; carried through to results so
                # quality can be broken down by project and data source.
                "stratum": group["stratum"].iloc[0] if "stratum" in group else None,
            }
        )
    return inputs


# Bounds for "this output is unusable", NOT the production constants in
# make_page_text: MIN_SUMMARY_LENGTH there is context-window headroom reserved
# for the response, and MAX_SUMMARY_LENGTH is unused. Borrowing them as validity
# limits would flag almost every normal summary.
MIN_USEFUL_TOKENS = 40
MAX_RUNAWAY_TOKENS = 800

# check_for_repeating_sequences scores (count * seq_len) / total_chars against a
# 20% threshold. That's calibrated for long scraped pages; on a short string any
# repeated pair clears it trivially, so only apply it where it's meaningful.
MIN_CHARS_FOR_REPETITION_CHECK = 400


def deterministic_checks(summary: str) -> dict:
    """Pipeline-breaking defects, detected for free.

    These need an absolute rate rather than a relative verdict: a pairwise judge
    scores two equally-broken summaries as a tie, which tells you nothing about
    how often a model emits output the downstream pipeline can't use.
    """
    summary = summary or ""
    stripped = summary.strip()
    n_tokens = count_tokens(stripped)

    has_repetition = False
    if len(stripped) >= MIN_CHARS_FOR_REPETITION_CHECK:
        try:
            has_repetition, _ = check_for_repeating_sequences(stripped)
        except (IndexError, ZeroDivisionError):
            # The helper indexes most_common(1)[0] and divides by length.
            has_repetition = False

    return {
        "is_empty": not stripped,
        # The page delimiter from make_group_text leaking into output.
        "has_delimiter": "----" in summary,
        # Reuses the production refusal heuristic: "I'm sorry, but the text..."
        "is_refusal": contains_i_am(stripped) if stripped else False,
        "has_repetition": has_repetition,
        "length_out_of_bounds": bool(
            stripped and not (MIN_USEFUL_TOKENS <= n_tokens <= MAX_RUNAWAY_TOKENS)
        ),
        "summary_tokens": n_tokens,
    }


DETERMINISTIC_FLAGS = [
    "is_empty", "has_delimiter", "is_refusal", "has_repetition", "length_out_of_bounds",
]
# Per-summary columns contributed by the judge. Not booleans any more, so they
# are reported separately from the deterministic flags rather than summed with them.
JUDGE_COLUMNS = ["faithfulness", "impact", "severity", "facets_covered", "facet_count", "unsupported_claims"]


def summarize(
    client: OpenAI, model: str, text: str, reasoning: str = None, provider: str = None
) -> tuple[str, float, dict]:
    """One summarization call, timed. Mirrors the production call shape."""
    kwargs = {
        "model": model,
        "input": [
            {"role": "system", "content": GENERIC_ORG_WEBSITE_PROMPT_TEXT},
            {"role": "user", "content": text},
        ],
    }
    if reasoning:
        kwargs["reasoning"] = {"effort": reasoning}
    if provider:
        # Pinning the upstream provider, not just the model. The same model id
        # can behave very differently depending on who serves it: hy3 spends
        # ~2500 tokens reasoning on novita and *zero* on deepinfra, at 52s vs 9s,
        # for the same request. Vercel's per-provider endpoint listing shows
        # deepinfra declaring no reasoning parameter for hy3 at all.
        kwargs["extra_body"] = {"providerOptions": {"gateway": {"only": [provider]}}}

    start = time.perf_counter()
    response = client.responses.create(**kwargs)
    elapsed = time.perf_counter() - start

    usage = {}
    # Which upstream provider actually served this. The same model id can behave
    # very differently by provider, so recording it makes a result reproducible
    # rather than dependent on whatever routing happened that day. Note the field
    # is snake_case here, unlike the camelCase used in error payloads.
    meta = getattr(response, "provider_metadata", None) or {}
    if isinstance(meta, dict):
        usage["provider"] = ((meta.get("gateway") or {}).get("routing") or {}).get("resolvedProvider")
    if getattr(response, "usage", None):
        usage = {
            **usage,
            "input_tokens": getattr(response.usage, "input_tokens", None),
            "output_tokens": getattr(response.usage, "output_tokens", None),
            # Confirms the effort setting took effect rather than being dropped:
            # a "none" variant still reporting thousands of reasoning tokens
            # means the parameter was ignored and the comparison is meaningless.
            "reasoning_tokens": getattr(
                getattr(response.usage, "output_tokens_details", None), "reasoning_tokens", 0
            ) or 0,
        }
    return response.output_text, elapsed, usage


def compare(
    judge_client: OpenAI, judge_model: str, source_text: str, summary_a: str, summary_b: str
) -> tuple[dict, dict]:
    """One blind pairwise judgment, with faithfulness against the source."""
    response = judge_client.responses.create(
        model=judge_model,
        reasoning={"effort": JUDGE_EFFORT},
        input=[
            {"role": "system", "content": JUDGE_PROMPT},
            {
                "role": "user",
                "content": (
                    f"SOURCE TEXT:\n{source_text}\n\n"
                    f"=====\n\nSUMMARY A:\n{summary_a}\n\n"
                    f"=====\n\nSUMMARY B:\n{summary_b}"
                ),
            },
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "comparison",
                "strict": True,
                "schema": JUDGE_SCHEMA,
            }
        },
    )
    usage = {}
    if getattr(response, "usage", None):
        usage = {
            "input_tokens": getattr(response.usage, "input_tokens", 0) or 0,
            "output_tokens": getattr(response.usage, "output_tokens", 0) or 0,
            # Reasoning tokens bill as output; surfacing them shows the effort
            # setting actually took effect rather than being silently ignored.
            "reasoning_tokens": getattr(
                getattr(response.usage, "output_tokens_details", None), "reasoning_tokens", 0
            ) or 0,
        }
    # What the API says it ran, not what we asked for -- a requested slug can be
    # aliased or resolved to a dated snapshot, and the run should record which.
    usage["model_reported"] = getattr(response, "model", None)
    return json.loads(response.output_text), usage


def _is_rate_limit(exc) -> bool:
    """Rate limiting is the one error worth counting separately.

    It's an operational signal (how much concurrency a model tolerates) rather
    than a quality one, and free-tier models are where it's expected to bite.
    """
    return type(exc).__name__ == "RateLimitError" or "429" in str(exc)[:200]


def run_summaries(client, inputs, models, args) -> pd.DataFrame:
    from concurrent.futures import ThreadPoolExecutor

    records = []
    for model in models:
        cfg = MODELS[model]
        # A label may map to another model id with a different configuration.
        model_id = cfg.get("model_id", model)
        effort = cfg.get("reasoning")
        provider = cfg.get("provider")
        bits = [f"model_id={model_id}"] if model_id != model else []
        if effort: bits.append(f"reasoning={effort}")
        if provider: bits.append(f"provider={provider}")
        suffix = f"  ({', '.join(bits)})" if bits else ""
        workers = max(1, args.workers)
        conc = f"  [{workers} workers]" if workers > 1 else ""
        print(f"\n--- {model} ---{suffix}{conc}")

        def _one(item):
            """Run one summarization, returning the exception rather than raising.

            Errors are captured per-item so a single failure (or a 429) doesn't
            abort the pool and discard the rest of the model's results.
            """
            try:
                return item, summarize(client, model_id, item["text"], effort, provider), None
            except Exception as e:
                return item, None, e

        wall_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # .map preserves input order, so output stays reproducible even
            # though the calls themselves are concurrent.
            results = list(pool.map(_one, inputs))
        wall = time.perf_counter() - wall_start

        n_429 = sum(1 for _, _, e in results if e is not None and _is_rate_limit(e))
        if workers > 1:
            ok_n = sum(1 for _, r, _ in results if r)
            print(f"  wall {wall:.1f}s for {len(inputs)} orgs "
                  f"({ok_n / wall:.2f} org/s)" + (f"  429s: {n_429}" if n_429 else ""))

        for i, (item, result, exc) in enumerate(results, 1):
            if exc is not None:
                tag = "RATE_LIMIT" if _is_rate_limit(exc) else type(exc).__name__
                print(f"  [{i}/{len(inputs)}] {item['source'][:45]:<45} ERROR {tag}")
                records.append({
                    "model": model, "source": item["source"],
                    "error": f"{type(exc).__name__}: {str(exc)[:200]}", "summary": "",
                    "rate_limited": _is_rate_limit(exc),
                })
                continue
            summary, elapsed, usage = result

            in_tok = usage.get("input_tokens") or count_tokens(item["text"])
            out_tok = usage.get("output_tokens") or count_tokens(summary)
            cost = (in_tok * cfg["in"] + out_tok * cfg["out"]) / 1_000_000
            checks = deterministic_checks(summary)
            n_flags = sum(1 for k in DETERMINISTIC_FLAGS if checks[k])

            reasoning_tok = usage.get("reasoning_tokens", 0) or 0
            print(
                f"  [{i}/{len(inputs)}] {elapsed:5.2f}s {in_tok:>7,}in {out_tok:>4}out "
                f"{reasoning_tok:>5}think ${cost:.5f} {n_flags} flag"
            )
            records.append({
                "model": model, "source": item["source"], "error": None,
                "stratum": item.get("stratum"),
                "reasoning_tokens": reasoning_tok,
                "provider": usage.get("provider"),
                "n_pages": item["n_pages"], "full_tokens": item["full_tokens"],
                "truncated": item["truncated"], "latency_s": round(elapsed, 3),
                "input_tokens": in_tok, "output_tokens": out_tok, "cost_usd": cost,
                "summary": summary, **checks,
            })
    return pd.DataFrame(records)


def run_comparisons(judge_client, summaries: pd.DataFrame, inputs, baseline, args):
    """Blind pairwise: baseline vs each candidate, on every org."""
    from concurrent.futures import ThreadPoolExecutor

    by_source = {i["source"]: i for i in inputs}
    rng = random.Random(args.seed)
    candidates = [m for m in summaries["model"].unique() if m != baseline]
    workers = max(1, args.workers)

    rows, judge_cost = [], 0.0
    for cand in candidates:
        conc = f"  [{workers} workers]" if workers > 1 else ""
        print(f"\n--- judging {baseline} vs {cand} ---{conc}")

        # Built sequentially before any concurrency so the RNG draws -- and thus
        # which model lands on side A -- stay identical for a given seed no
        # matter how many workers run the calls.
        tasks = []
        for source in by_source:
            base_row = summaries[(summaries["model"] == baseline) & (summaries["source"] == source)]
            cand_row = summaries[(summaries["model"] == cand) & (summaries["source"] == source)]
            if base_row.empty or cand_row.empty:
                continue
            base_sum = base_row.iloc[0]["summary"]
            cand_sum = cand_row.iloc[0]["summary"]
            if not (base_sum or "").strip() or not (cand_sum or "").strip():
                continue

            # Randomize which side each model lands on so position bias can't
            # systematically favor either.
            baseline_is_a = rng.random() < 0.5
            a, b = (base_sum, cand_sum) if baseline_is_a else (cand_sum, base_sum)
            tasks.append((source, a, b, baseline_is_a))

        def _judge(task):
            source, a, b, _ = task
            try:
                return task, compare(
                    judge_client,
                    args.judge_model,
                    truncate(by_source[source]["text"], args.judge_source_tokens),
                    a,
                    b,
                ), None
            except Exception as e:
                return task, None, e

        wall_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # .map preserves order, so printed output and saved rows stay
            # reproducible even though the calls overlap.
            results = list(pool.map(_judge, tasks))
        wall = time.perf_counter() - wall_start

        n_429 = sum(1 for _, _, e in results if e is not None and _is_rate_limit(e))
        if workers > 1:
            done = sum(1 for _, r, _ in results if r)
            print(f"  wall {wall:.1f}s for {len(tasks)} comparisons "
                  f"({done / wall:.2f}/s)" + (f"  429s: {n_429}" if n_429 else ""))

        for i, ((source, a, b, baseline_is_a), result, exc) in enumerate(results, 1):
            if exc is not None:
                tag = "RATE_LIMIT" if _is_rate_limit(exc) else type(exc).__name__
                print(f"  [{i}] {source[:45]:<45} ERROR {tag}")
                continue
            verdict, usage = result

            judge_cost += (
                usage.get("input_tokens", 0) * JUDGE_PRICE["in"]
                + usage.get("output_tokens", 0) * JUDGE_PRICE["out"]
            ) / 1_000_000

            # Translate the blind A/B verdict back to model names.
            win = verdict["winner"]
            if win == "tie":
                winner = "tie"
            elif (win == "A") == baseline_is_a:
                winner = baseline
            else:
                winner = cand
            bp, cp = ("a", "b") if baseline_is_a else ("b", "a")

            # Report the resolved judge identity once, from the API's own
            # response -- the startup line only echoes what was requested.
            if not rows:
                print(f"  (judge resolved to: {usage.get('model_reported')}, "
                      f"{usage.get('reasoning_tokens', 0)} reasoning tokens on first call)")

            b_faith = verdict[f"{bp}_faithfulness"]
            c_faith = verdict[f"{cp}_faithfulness"]
            print(
                f"  [{i}] {source[:42]:<42} -> {winner.split('/')[-1][:18]:<18} "
                f"{b_faith[:4]}/{c_faith[:4]}"
            )
            rows.append({
                "source": source, "baseline": baseline, "candidate": cand,
                "judge_model_reported": usage.get("model_reported"),
                "judge_reasoning_tokens": usage.get("reasoning_tokens", 0),
                "winner": winner,
                "comparison_notes": verdict["comparison_notes"],
                "baseline_faithfulness": b_faith,
                "candidate_faithfulness": c_faith,
                "baseline_impact": verdict[f"{bp}_impact"],
                "candidate_impact": verdict[f"{cp}_impact"],
                "baseline_unsupported_claims": verdict[f"{bp}_unsupported_claims"],
                "candidate_unsupported_claims": verdict[f"{cp}_unsupported_claims"],
                "baseline_facets_covered": ",".join(verdict[f"{bp}_facets_covered"]),
                "candidate_facets_covered": ",".join(verdict[f"{cp}_facets_covered"]),
                "baseline_faithfulness_notes": verdict[f"{bp}_faithfulness_notes"],
                "candidate_faithfulness_notes": verdict[f"{cp}_faithfulness_notes"],
                "baseline_facet_notes": verdict[f"{bp}_facet_notes"],
                "candidate_facet_notes": verdict[f"{cp}_facet_notes"],
            })
    return pd.DataFrame(rows), judge_cost


def merge_judge_flags(summaries: pd.DataFrame, comparisons: pd.DataFrame, baseline: str) -> pd.DataFrame:
    """Attach per-summary judge assessments back onto the summaries table.

    The baseline appears in one comparison per candidate, so it gets several
    verdicts per org. Take the worst faithfulness rather than the majority -- if
    any judging pass caught a fabrication it happened, and averaging it away
    would understate the risk. Facets use the first verdict, since coverage is a
    property of the text and shouldn't vary between passes.
    """
    summaries = summaries.copy()
    for col in JUDGE_COLUMNS:
        summaries[col] = None
    if comparisons.empty:
        return summaries

    for idx, row in summaries.iterrows():
        if row["model"] == baseline:
            sel = comparisons[comparisons["source"] == row["source"]]
            prefix = "baseline"
        else:
            sel = comparisons[
                (comparisons["source"] == row["source"])
                & (comparisons["candidate"] == row["model"])
            ]
            prefix = "candidate"
        if sel.empty:
            continue

        # Rank by severity of the (faithfulness, impact) pair, not faithfulness
        # alone: a fabricated-but-inconsequential error should not outrank a
        # material one just because its type label is harsher.
        graded = [
            (severity_of(f, i), f, i, c)
            for f, i, c in zip(
                sel[f"{prefix}_faithfulness"],
                sel[f"{prefix}_impact"],
                sel[f"{prefix}_unsupported_claims"],
            )
            if isinstance(f, str)
        ]
        if graded:
            sev, worst, worst_impact, claim = max(graded, key=lambda t: t[0])
            summaries.at[idx, "faithfulness"] = worst
            summaries.at[idx, "impact"] = worst_impact
            summaries.at[idx, "severity"] = sev
            summaries.at[idx, "unsupported_claims"] = claim if isinstance(claim, str) else ""

        facets = sel.iloc[0][f"{prefix}_facets_covered"]
        facets = facets if isinstance(facets, str) else ""
        summaries.at[idx, "facets_covered"] = facets
        summaries.at[idx, "facet_count"] = len([f for f in facets.split(",") if f])
    return summaries


def _binomial_p(wins: int, decisive: int) -> float:
    """Two-sided binomial test of wins vs losses against a 50/50 null.

    Ties are excluded: they carry no directional information, and counting them
    in the denominator would drag every win rate toward 50% regardless of how
    decisively the model won the comparisons it did win.
    """
    from math import comb

    if decisive == 0:
        return 1.0
    pmf = [comb(decisive, i) * 0.5 ** decisive for i in range(decisive + 1)]
    lower = sum(pmf[: wins + 1])
    upper = sum(pmf[wins:])
    return min(1.0, 2 * min(lower, upper))


def _flags_true(series) -> "pd.Series":
    """Boolean mask of True values, treating None/NaN as False.

    Judge flags are object-dtype (True/False/None), so .fillna(False).astype(bool)
    triggers pandas' downcasting FutureWarning. .eq(True) gives the same result
    without it.
    """
    return series.eq(True)


def _flag_count(frame, flags) -> int:
    return sum(int(_flags_true(frame[f]).sum()) for f in flags if f in frame)


def write_report(summaries, comparisons, out_dir: pl.Path, baseline: str, judge_cost: float):
    ok = summaries[summaries["error"].isna()]
    if ok.empty:
        print("\nNo successful runs.")
        return

    print(
        f"\n{'model':<34} {'p50 lat':>8} {'p90 lat':>8} {'$/1k orgs':>10} "
        f"{'visible':>8} {'think':>7} {'sev':>6} {'facets':>7} {'defects':>8} {'err':>4}"
    )
    for model, grp in ok.groupby("model"):
        n_err = int(summaries[(summaries["model"] == model) & summaries["error"].notna()].shape[0])
        # Deterministic flags stay a simple count; judge output is no longer
        # boolean, so severity and facet coverage get their own columns.
        defects = _flag_count(grp, DETERMINISTIC_FLAGS) / len(grp)
        # Visible vs think makes the reasoning overhead legible: two models can
        # bill the same while one delivers far less usable text.
        think = grp["reasoning_tokens"].median() if "reasoning_tokens" in grp else 0
        sev = pd.to_numeric(grp.get("severity"), errors="coerce").mean() if "severity" in grp else float("nan")
        fac = pd.to_numeric(grp.get("facet_count"), errors="coerce").mean() if "facet_count" in grp else float("nan")
        print(
            f"{model:<34} {grp['latency_s'].quantile(0.5):>7.2f}s "
            f"{grp['latency_s'].quantile(0.9):>7.2f}s "
            f"{grp['cost_usd'].mean() * 1000:>9.2f} "
            f"{grp['summary_tokens'].median():>8.0f} {think:>7.0f} "
            f"{sev:>6.2f} {fac:>6.1f}/5 {defects:>8.2f} {n_err:>4}"
        )

    base_cost = ok[ok["model"] == baseline]["cost_usd"].mean() * 1000 if baseline in ok["model"].values else None
    if base_cost:
        print(f"\ncost vs baseline (${base_cost:.2f}/1k orgs):")
        for model, grp in ok.groupby("model"):
            if model == baseline:
                continue
            c = grp["cost_usd"].mean() * 1000
            print(f"  {model:<34} {'free' if c <= 0 else f'{base_cost / c:>5.1f}x cheaper'}")

    # Severity distribution rather than a single unfaithful rate: a fabricated
    # date and a safe inference are both "unfaithful" but not the same risk.
    if "faithfulness" in ok and ok["faithfulness"].notna().any():
        print(f"\nfaithfulness:\n  {'model':<34}" + "".join(f"{lvl[:13]:>15}" for lvl in FAITHFULNESS_LEVELS))
        for model, grp in ok.groupby("model"):
            cells = "".join(
                f"{int((grp['faithfulness'] == lvl).sum()):>7}/{len(grp):<7}"
                for lvl in FAITHFULNESS_LEVELS
            )
            print(f"  {model:<34}{cells}")

    # Predicts taxonomy recall directly: the mapper may not assign a category
    # the summary never states, so an uncovered facet is an unmappable one.
    if "facets_covered" in ok and ok["facets_covered"].notna().any():
        print(f"\nfacet coverage (fraction of orgs where explicitly stated):")
        print(f"  {'model':<34}" + "".join(f"{f[:12]:>14}" for f in FACETS))
        for model, grp in ok.groupby("model"):
            cells = ""
            for facet in FACETS:
                hit = grp["facets_covered"].fillna("").astype(str).str.split(",").apply(lambda xs: facet in xs)
                cells += f"{hit.mean():>14.2f}"
            print(f"  {model:<34}{cells}")

    firing = [f for f in DETERMINISTIC_FLAGS if f in ok and _flags_true(ok[f]).any()]
    if firing:
        print(f"\ndeterministic defects:\n  {'model':<34}" + "".join(f"{f[:11]:>13}" for f in firing))
        for model, grp in ok.groupby("model"):
            cells = "".join(f"{int(_flags_true(grp[f]).sum()):>6}/{len(grp):<6}" for f in firing)
            print(f"  {model:<34}{cells}")
    else:
        print("\ndeterministic defects: none fired")

    # The point of stratifying: a model can be fine on Crunchbase startups and
    # weak on Giving Tuesday nonprofits, which an aggregate hides.
    if "stratum" in ok and ok["stratum"].notna().any() and "severity" in ok:
        print(f"\nper-stratum mean severity:\n  {'stratum':<40}", end="")
        models_sorted = sorted(ok["model"].unique())
        print("".join(f"{m.split('/')[-1][:14]:>16}" for m in models_sorted))
        for stratum, sgrp in ok.groupby("stratum"):
            cells = ""
            for model in models_sorted:
                mg = sgrp[sgrp["model"] == model]
                if mg.empty:
                    cells += f"{'-':>16}"
                else:
                    cells += f"{pd.to_numeric(mg['severity'], errors='coerce').mean():>16.2f}"
            print(f"  {stratum:<40}{cells}")

    if not comparisons.empty:
        print(f"\nblind pairwise vs {baseline} (judge effort={JUDGE_EFFORT}):")
        print(f"  {'candidate':<34}{'W-L-T':>10}{'win%':>7}{'p':>9}   verdict")
        for cand, grp in comparisons.groupby("candidate"):
            wins = int((grp["winner"] == cand).sum())
            losses = int((grp["winner"] == baseline).sum())
            ties = int((grp["winner"] == "tie").sum())
            decisive = wins + losses
            p = _binomial_p(wins, decisive)
            rate = wins / decisive if decisive else 0.0
            # Reported per candidate rather than as a blanket caveat: at a given
            # n some comparisons are decisive and others aren't, and saying
            # "directional" across all of them buries the ones that are real.
            verdict = (
                "significant" if p < 0.01 else
                "significant (p<.05)" if p < 0.05 else
                "inconclusive at this n"
            )
            print(f"  {cand:<34}{f'{wins}-{losses}-{ties}':>10}{rate:>7.0%}{p:>9.4f}   {verdict}")
        print("\n  win% and p exclude ties; p is a two-sided binomial test against 50/50.")
        print(f"  judge cost: ${judge_cost:.2f}")

    lines = ["# Summaries by model\n"]
    for source, grp in ok.groupby("source"):
        lines.append(f"\n## {source}\n")
        verdicts = comparisons[comparisons["source"] == source] if not comparisons.empty else pd.DataFrame()
        for _, row in grp.iterrows():
            defect_flags = " ".join(f for f in DETERMINISTIC_FLAGS if f in row and row.get(f) is True)
            bits = []
            if row.get("faithfulness") and row["faithfulness"] != "faithful":
                bits.append(str(row["faithfulness"]))
            if row.get("facets_covered") is not None and str(row.get("facets_covered")):
                bits.append(f"facets: {row['facets_covered']}")
            if defect_flags:
                bits.append(defect_flags)
            lines.append(f"\n**{row['model']}** ({row['latency_s']}s)")
            if bits:
                lines.append(" — " + " | ".join(bits))
            lines.append(f"\n\n{row['summary']}\n")
            if str(row.get("unsupported_claims") or "").strip():
                lines.append(f"\n> unsupported: {row['unsupported_claims']}\n")
        for _, v in verdicts.iterrows():
            # comparison_notes can run long; the report shows the tail, which is
            # where the prompt asks for a one-sentence conclusion.
            note = str(v.get("comparison_notes") or "")
            note = note if len(note) <= 300 else "…" + note[-300:]
            lines.append(f"\n> judge ({v['candidate'].split('/')[-1]}): **{v['winner'].split('/')[-1]}** — {note}\n")
    (out_dir / "summaries.md").write_text("".join(lines))


def run_rejudge(args):
    """Re-run only the judging over summaries saved by a previous run.

    No candidate models are called, so the text being judged is identical across
    rubric revisions and any change in verdict is attributable to the rubric.
    """
    summaries, inputs, models = load_for_rejudge(
        args.rejudge, args.max_input_tokens, args.limit_rejudge, args.seed, args.models
    )

    baseline = next((m for m, c in MODELS.items() if c.get("baseline")), None)
    if baseline not in models:
        raise SystemExit(
            f"Baseline {baseline} is not among the saved models ({models}); "
            "pairwise comparison needs it."
        )
    judge_client = get_judge_client()
    print(f"judge: {args.judge_model} (direct OpenAI, effort={JUDGE_EFFORT})")

    comparisons, judge_cost = run_comparisons(judge_client, summaries, inputs, baseline, args)
    summaries = merge_judge_flags(summaries, comparisons, baseline)

    out_dir = pl.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summaries.to_csv(out_dir / "benchmark_results.csv", index=False)
    comparisons.to_csv(out_dir / "pairwise_verdicts.csv", index=False)
    write_report(summaries, comparisons, out_dir, baseline, judge_cost)
    print(f"\nWrote results to {out_dir}/")


def run(args):
    if args.rejudge:
        return run_rejudge(args)

    client = get_client()
    n = refresh_prices_from_gateway(client)
    if n:
        print(f"pricing refreshed from gateway for {n} model(s)")

    if args.csv:
        pages_df = pd.read_csv(args.csv)
        missing = {"source", "subpath", "text"} - set(pages_df.columns)
        if missing:
            raise SystemExit(f"CSV missing required columns: {sorted(missing)}")
        sources = pages_df["source"].drop_duplicates().head(args.limit)
        pages_df = pages_df[pages_df["source"].isin(sources)]
    elif args.stratify:
        pages_df = load_pages_stratified(args.limit, args.min_pages, args.seed)
    else:
        pages_df = load_pages_from_db(args.limit, args.min_pages)

    inputs = build_inputs(pages_df, args.max_input_tokens)
    if not inputs:
        raise SystemExit("No usable orgs after packing.")
    print(f"\n{len(inputs)} orgs, {len(pages_df)} pages total")

    token_counts = sorted(i["full_tokens"] for i in inputs)
    mid = token_counts[len(token_counts) // 2]
    print(f"packed tokens/org: min {token_counts[0]:,}  median {mid:,}  max {token_counts[-1]:,}")
    for model, cfg in MODELS.items():
        over = sum(1 for t in token_counts if t > cfg["context"])
        if over:
            print(f"  ! {over}/{len(token_counts)} orgs exceed {model} ({cfg['context']:,} ctx)")

    models = args.models or list(MODELS)
    unknown = [m for m in models if m not in MODELS]
    if unknown:
        raise SystemExit(f"Unknown model(s): {unknown}")

    baseline = next((m for m, c in MODELS.items() if c.get("baseline")), None)

    # Resolve the judge before spending anything on summarization: a missing
    # OpenAI key should fail immediately, not after paying for 80 calls.
    judge_client = None
    if args.no_judge:
        pass
    elif baseline not in models:
        print(f"\n(skipping pairwise -- baseline {baseline} not in --models)")
    elif len(models) < 2:
        print("\n(skipping pairwise -- need the baseline plus at least one candidate)")
    else:
        judge_client = get_judge_client()
        print(f"judge: {args.judge_model} (direct OpenAI, effort={JUDGE_EFFORT})")

    summaries = run_summaries(client, inputs, models, args)

    comparisons, judge_cost = pd.DataFrame(), 0.0
    if judge_client:
        comparisons, judge_cost = run_comparisons(judge_client, summaries, inputs, baseline, args)
        summaries = merge_judge_flags(summaries, comparisons, baseline)
    out_dir = pl.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summaries.to_csv(out_dir / "benchmark_results.csv", index=False)
    if not comparisons.empty:
        comparisons.to_csv(out_dir / "pairwise_verdicts.csv", index=False)

    write_report(summaries, comparisons, out_dir, baseline, judge_cost)
    print(f"\nWrote results to {out_dir}/")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20, help="number of orgs (default 20)")
    parser.add_argument("--min-pages", type=int, default=3, help="min scraped pages per org (default 3)")
    parser.add_argument(
        "--stratify", action="store_true",
        help="sample evenly across (project, data_source) strata -- ed_tracker/Giving Tuesday, "
             "drawdown/Crunchbase.com, oneearth_loc/Candid.org, etc. -- instead of taking "
             "whatever the database returns first",
    )
    parser.add_argument("--csv", help="CSV with source/subpath/text columns instead of the database")
    parser.add_argument(
        "--rejudge", metavar="RESULTS_CSV",
        help="re-judge summaries saved by a previous run (path to its benchmark_results.csv) "
             "instead of generating new ones. Holds the summaries fixed so a rubric change is "
             "the only thing that varies; costs judge tokens only",
    )
    parser.add_argument(
        "--limit-rejudge", type=int, metavar="N",
        help="with --rejudge, subsample N orgs deterministically (by --seed). The same seed "
             "picks the same orgs, so two rubric revisions run at the same N are diffable",
    )
    parser.add_argument("--models", nargs="+", help=f"subset of: {' '.join(MODELS)}")
    parser.add_argument(
        "--max-input-tokens", type=int, default=100_000,
        help="truncate packed text to this, uniformly (default 100k, under gpt-oss-120b's 128k)",
    )
    parser.add_argument(
        "--judge-source-tokens", type=int, default=40_000,
        help="source text given to the judge for faithfulness (default 40k; caps judge cost)",
    )
    parser.add_argument(
        "--judge-model", default=JUDGE_MODEL,
        help=f"judge model, called against OpenAI directly (default {JUDGE_MODEL})",
    )
    parser.add_argument("--no-judge", action="store_true", help="skip pairwise comparison")
    parser.add_argument(
        "--workers", type=int, default=1, metavar="N",
        help="concurrent API calls (default 1). Serial keeps per-call latency clean for "
             "model comparison; raise it to measure throughput and find where models start "
             "rate-limiting. Your pipeline's summarize_scraped_df defaults to 5",
    )
    parser.add_argument("--seed", type=int, default=0, help="seed for A/B randomization")
    # data_out is the project's existing gitignored output convention (py2mappr
    # uses it), so results don't show up as untracked noise in git status.
    parser.add_argument(
        "--out-dir", default="data_out/summarization_benchmark",
        help="output directory (default data_out/summarization_benchmark, gitignored)",
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
