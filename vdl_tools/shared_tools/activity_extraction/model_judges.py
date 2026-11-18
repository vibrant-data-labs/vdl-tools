"""
Judged comparison: open-weight extractions against the gpt-5.4-mini baseline.
=============================================================================

Runs three LLM judges over the 153-organization seed-42 cohort, for the baseline
and for each candidate model the probe extracted. Alignment is the judge built for
the activity-extraction prompt in oneearth ``activity_extraction/`` (ported
2026-11-07 from ``origin/main`` 3788d56); precision and recall are versions of
theirs written here and calibrated on a blind human review of 80 verdicts:

``precision`` (v3.2)
    ``gpt-6-sol`` first checks whether each extracted activity is concrete work,
    current, and done by this organization, then labels it ``line_of_work``, ``minor_item``,
    ``not_an_activity`` or ``unsupported``; says whether it is a ``duplicate`` of an
    earlier one or ``should_merge`` into it under the extraction prompt's granularity
    rule; and lists each unsupported field claim with its support (inferred or
    fabricated) and impact (material or none). Only material issues count as errors;
    severity is derived from the two categories here, not asked as a score. It also
    rates each activity's prominence and recurrence from the source without seeing the
    extraction model's, and the two are compared here.
``recall`` (v2)
    ``gpt-6-sol`` lists the lines of work the source describes that the list misses,
    naming the nearest listed activity and how it differs. Menu labels, link titles,
    communications, perks and audience or site differences do not count.
``alignment``
    ``gpt-5.4-mini`` at medium effort groups the baseline's list (A) and the
    candidate's (B) into same / merge_split / only_a / only_b. Content agreement is
    the share of activities with a counterpart; granularity agreement the share in
    one-to-one pairs. The noise floor is the same judge on two baseline runs: the
    ``agree_v1``-``agree_v3`` rows already in the cache, re-scored here with no calls.

Identity with the original judges
---------------------------------
The alignment prompt (and recall v1's, kept to read its stored verdicts) is loaded
by id from the ``prompt`` table, so the instructions are byte-identical to the
originals, schema tag included. Their response models are ported verbatim, and at
start-up each schema fingerprint is checked against the ``[response schema <Model> #<fingerprint>]`` tag in the stored
prompt; a mismatch stops the run. Inputs are rendered with the original formatting
functions and passed through the same control-character ``scrub``. The prompts
written here carry the same kind of tag, so a change to their schema or text is a
new prompt row rather than an overwrite.

What is reused and what is new
------------------------------
The existing precision verdicts on the cohort judged the output of an earlier
extraction prompt (``activity_extraction_general_v1``: 152 of 153 organizations
list different activities from the baseline row), and only one cohort organization
has a recall verdict, so the baseline is judged afresh like the candidates. Only the descriptions on record are taken from those older precision
inputs, which is where the original judge got them. Every new call is keyed
``<JUDGE_RUN>_<run>|<org id>``, a prefix no existing row uses, so nothing cached is
overwritten and a rerun reads its own rows back. The baseline keeps the
``BASELINE_JUDGE_RUN`` prefix: its extraction never changes, so its verdicts carry over.

Source text is the frozen ``input_text`` the baseline extraction received, the
same string the original ``rebuild_sources`` builds, without depending on today's
page cache.

Usage
-----
    python -m vdl_tools.shared_tools.activity_extraction.model_judges --dry-run
    python -m vdl_tools.shared_tools.activity_extraction.model_judges --judges alignment
    python -m vdl_tools.shared_tools.activity_extraction.model_judges --judges precision \\
        --org-ids 02-0332297 927f8767-5a75-490f-8d65-3e610f59920a
    python -m vdl_tools.shared_tools.activity_extraction.model_judges

Reports go to ``data_out/activity_extraction_judges/`` (gitignored).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select, text

from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.general import GeneralActivityProfile
from vdl_tools.shared_tools.database_cache.database_models.prompt import PromptResponse
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.tools.logger import logger

# Part of every new given_id; bump it to judge again from scratch. bench_v1 judged
# the 2026-11-07 runs (16k output cap, DeepSeek V4 Flash, Nemotron without reasoning).
JUDGE_RUN = "bench_v2"
# The baseline's extraction is fixed, so its bench_v1 verdicts still apply.
BASELINE_JUDGE_RUN = "bench_v1"

# The routes the judged comparison covers by default. 2026-11-12: DeepSeek V4.1 Flash
# replaced V4 Flash, at its provider default (docs.baseten.co, model-apis/reasoning).
# 2026-11-13: GLM 5.3 Flash (most errors) and Nemotron 3.5 Super (no recall gain) were
# dropped; their runs stay in the cache, reachable with --routes
# baseten:glm-5.3-flash baseten:nemotron-3.5-super@high.
CANDIDATE_ROUTES = ("baseten:deepseek-v4.1-flash",)
BASELINE_RUN = "baseline"

# The originals' judge models. Precision and recall send no reasoning setting, so
# their request hash is empty; alignment runs at medium effort.
SOL = "gpt-6-sol"
ALIGN_MODEL = "gpt-5.4-mini"
ALIGN_REASONING = {"effort": "medium"}

# USD per input and output token, for the dry-run estimate, from MODEL_DATA. Inputs
# over 272K tokens cost 2x/1.5x, which no cohort input reaches. Reasoning tokens
# bill as output.
JUDGE_PRICES = {
    model: (MODEL_DATA[model]["input_cost_per_token"], MODEL_DATA[model]["output_cost_per_token"])
    for model in (SOL, ALIGN_MODEL)
}

# Families of existing alignment rows that compare two baseline runs (the noise floor).
NOISE_FLOOR_FAMILIES = ("agree_v1", "agree_v2", "agree_v3")

# Bootstrap draws for the paired confidence intervals.
BOOTSTRAP_DRAWS = 2000

JudgeName = Literal["precision", "recall", "alignment"]
JUDGES: tuple[JudgeName, ...] = ("precision", "recall", "alignment")


# --- Response models, verbatim from oneearth (field order is part of the schema) ---


class AlignmentGroup(BaseModel):
    """One group of items from the two lists."""

    a: list[int] = Field(description="Item numbers from list A")
    b: list[int] = Field(description="Item numbers from list B")
    relation: Literal["same", "merge_split", "only_a", "only_b"]
    note: str


class ListAlignment(BaseModel):
    """The alignment of two activity lists for one organization."""

    groups: list[AlignmentGroup]


class ActivityVerdict(BaseModel):
    """The judge's verdict on one extracted activity."""

    activity_index: int = Field(description="The activity's number in the list")
    label: Literal["line_of_work", "minor_item", "not_an_activity", "duplicate", "unsupported"]
    duplicate_of: int | None = Field(description="Number of the earlier activity, for duplicates")
    prominence: Literal["core", "secondary", "minor"]
    reason: str = Field(description="One sentence pointing at what in the source decides it")


class PrecisionVerdicts(BaseModel):
    """Verdicts on every activity extracted for one organization."""

    verdicts: list[ActivityVerdict]
    site_identity: Literal["same_organization", "different_entity", "unclear"]
    site_identity_reason: str


class MissedActivity(BaseModel):
    """One line of work the source describes and the extracted list does not cover."""

    name: str = Field(description="Short noun phrase naming the work")
    kind: Literal["line_of_work", "minor_item"]
    prominence: Literal["core", "secondary", "minor"]
    evidence: str = Field(description="Verbatim phrase from the source, at most 25 words")
    reason: str = Field(description="Why no listed activity covers it, naming the nearest")


class RecallVerdict(BaseModel):
    """Every missed line of work for one organization; empty when the list is complete."""

    missed: list[MissedActivity]


# --- Precision judge v2 (written here, 2026-11-09) ---
#
# v1 checked only name, what happens and who carries it out; aim, sector, methods
# and geography went unjudged. Its duplicate rule also split on "where it happens",
# which the extraction prompt says is no reason to split, so lists cut finer than
# the prompt asks passed as fully precise. v2 shows every field, applies the
# extraction prompt's own granularity rule, and records repeats and unsupported
# fields apart from the label, so a real line of work with an invented detail
# ("embellished") is counted separately from work the source never describes.

ActivityField = Literal[
    "activity", "what_happens", "who_carries_it_out", "aim", "sector", "methods", "geography"
]
ACTIVITY_FIELDS: tuple[str, ...] = ActivityField.__args__


class ActivityVerdictV2(BaseModel):
    """The v2 judge's verdict on one extracted activity."""

    activity_index: int = Field(description="The activity's number in the list")
    label: Literal["line_of_work", "minor_item", "not_an_activity", "unsupported"]
    overlap: Literal["none", "duplicate", "should_merge"]
    same_work_as: int | None = Field(
        description="Number of the earlier activity, for duplicate and should_merge; else null"
    )
    field_notes: str = Field(description="What each unsupported field claims, or 'none'")
    unsupported_fields: list[ActivityField] = Field(
        description="Fields that claim something the source does not support; empty if none"
    )
    prominence: Literal["core", "secondary", "minor"]
    reason: str = Field(description="One sentence pointing at what in the source decides the label")


class PrecisionVerdictsV2(BaseModel):
    """v2 verdicts on every activity extracted for one organization."""

    verdicts: list[ActivityVerdictV2]
    site_identity: Literal["same_organization", "different_entity", "unclear"]
    site_identity_reason: str


PRECISION_V2_NAME = "activity_extraction_precision_judge_v2"

# The v1 prompt (fe735140...) with the changes above; the granularity paragraph
# repeats the extraction prompt's rule in its own words.
PRECISION_V2_INSTRUCTIONS = """\
You are auditing a structured record of what an organization does. An extraction model read text \
scraped from the organization's website and produced a numbered list of the organization's \
distinct activities. Your job is to decide, for each listed activity, whether it belongs on that \
list, whether it repeats an earlier activity, and whether each of its fields is supported by the \
source.

You will receive the organization's name, the source text the extraction model read (webpage URLs \
and page text, delineated by lines of `----` characters), the description on record for the \
organization (from a separate database, possibly out of date), and then the numbered list of \
extracted activities. Each activity gives its name, what happens, who carries it out, its aim, \
sector, methods, geography, and a quote the extraction model offered as evidence.

An activity belongs on the list when it is a concrete program, product, service, project, or line \
of work the organization runs: something with a described method, audience, customer, site, or \
output behind it, that the organization does as part of its work.

1. LABEL EACH ACTIVITY WITH EXACTLY ONE OF
- line_of_work — a concrete program, product, service, or line of work the organization runs, \
supported by the source.
- minor_item — real and supported by the source, but a one-off or incidental item rather than a \
line of work: a single event or fundraiser, one campaign or announcement, a side offering \
mentioned once in passing.
- not_an_activity — not something the organization does as work. This includes content the \
organization publishes (individual blog posts, essays, articles, news items, recipes) treated as \
if the topic were the organization's work; mission or values language; organizational facts \
(history, staff, board, partners, awards, locations); donation or membership appeals; a variant, \
flavor, size, or pricing tier of a product already listed. A publishing, media, or education \
program that is itself the organization's work IS a line of work; the subject of one article is \
not.
- unsupported — the source does not say the organization does this work, or describes materially \
different work. When the work is real but some of its details are not supported, do not use this \
label: label the work for what it is, and list the faulty fields under 3.

2. SAY WHETHER IT REPEATS AN EARLIER ACTIVITY
The extraction model was told to keep one record per line of work. A line of work is defined by \
three things: the kind of work (its method — doing it, funding it, training others, advocating, \
researching, selling a product), who carries it out, and what it is aimed at. Programs, projects, \
sites, events, or products that share all three are ONE activity, even when the source names them \
separately. Activities are separate only when they differ in kind of work, in who carries it out, \
in what they are aimed at, or in the sector they serve; different locations, seasons, or \
audiences of the same kind are not reasons to split. A company's product lines that serve the \
same purpose for the same customers are one activity.
- none — no earlier activity covers the same line of work.
- duplicate — the same work as an earlier activity, described again.
- should_merge — a separate program, project, site, event, product, audience, or slice of the \
same line of work as an earlier activity, which the rule above puts in one record.
For duplicate and should_merge, give the number of the earlier activity in same_work_as; \
otherwise null. Only point back to an earlier number. Judge the overlap whatever the label.

3. CHECK EACH FIELD AGAINST THE SOURCE
List in unsupported_fields every field that claims something the source does not support, and \
say what each one claims in field_notes. When every field is supported, leave the list empty and \
write "none". "Not stated", "no specific aim stated", and saying that the source is ambiguous are \
always supported.
- activity, what_happens — an action, method, step, customer, partner, or scale the source does \
not state, or a verb that is stronger than or different from the source's.
- who_carries_it_out — says the organization does the work itself where the source says it \
funds, trains, advises, or otherwise enables others to do it, or the reverse; or settles a \
doer-or-funder question that the source leaves open.
- aim — an outcome or subject the source does not give.
- sector — contradicts the source. A plain description of the field the work sits in is supported.
- methods — a technology, practice, or approach the source does not name for this work.
- geography — a place the source does not name. The state or country containing a place the \
source names is supported; a place inferred from the organization's scope or from general \
knowledge is not.

ALSO RATE PROMINENCE, for every activity, as the source presents it
- core — central to what the organization is and does.
- secondary — a real part of its work, but not central.
- minor — peripheral, mentioned briefly.

RULES
- Judge from the source text only. Do not use outside knowledge of the organization.
- A thin source is not a reason to reject an activity the source does support.
- Give a one-sentence reason for each label, pointing at what in the source decides it.
- Return one verdict for every listed activity, in list order.

ALSO REPORT, FOR THE WEBSITE AS A WHOLE
- site_identity — Compare the source text with the description on record. "same_organization" \
when the website is plausibly the same organization, even if its work has changed, grown, or is \
described differently. "different_entity" when the website plainly belongs to something else: a \
different business or organization, an unrelated blog, a parked, for-sale, or resold domain. \
"unclear" when the source is too thin to tell or the record is missing. The activity verdicts \
above are still judged against the source text, whatever this answer is.
- site_identity_reason — one sentence.
"""


# --- Precision judge v3 and recall judge v2 (written here, 2026-11-10) ---
#
# Calibrated on 35 human answers from the blind review sample: the human flagged 3 of
# 22 activities where v2 flagged 17, and confirmed few of recall v1's misses. v2 had no
# notion of consequence: an extra "in person" counted as much as a partner's program
# credited to the organization. v3 follows the summarization benchmark's judge: each
# faulty field gets a support level (inferred or fabricated) and an impact (material
# or none), asked as categories; severity is derived in code, not asked as a number.
# Mission language, an ended program and a partner's work are label questions, settled
# first by the work check. Recall v2 adds the cases recall v1 over-reported: menu
# labels and link titles as evidence, communications and perks as work, and audience
# or site differences as reasons to split.


class FieldIssue(BaseModel):
    """One field claim the source does not support."""

    field: ActivityField
    claim: str = Field(description="The unsupported words, quoted from the field")
    support: Literal["inferred", "fabricated"]
    impact: Literal["material", "none"]


class ActivityVerdictV3(BaseModel):
    """The v3 judge's verdict on one extracted activity; notes come before the verdicts."""

    activity_index: int = Field(description="The activity's number in the list")
    work_check: str = Field(
        description="Is this concrete work, current, and done by this organization? Point at the source"
    )
    label: Literal["line_of_work", "minor_item", "not_an_activity", "unsupported"]
    overlap: Literal["none", "duplicate", "should_merge"]
    same_work_as: int | None = Field(
        description="Number of the earlier activity, for duplicate and should_merge; else null"
    )
    field_notes: str = Field(description="Check each field's claims against the source, or 'none'")
    field_issues: list[FieldIssue] = Field(description="Unsupported claims; empty if none")
    prominence: Literal["core", "secondary", "minor"]


class PrecisionVerdictsV3(BaseModel):
    """v3 verdicts on every activity extracted for one organization."""

    verdicts: list[ActivityVerdictV3]
    site_identity: Literal["same_organization", "different_entity", "unclear"]
    site_identity_reason: str


# v3.1: the goal rule replaced by a face-value concrete-work test (2026-11-10).
PRECISION_V3_NAME = "activity_extraction_precision_judge_v3_1"

PRECISION_V3_INSTRUCTIONS = """\
You are auditing a structured record of what an organization does. An extraction model read text \
scraped from the organization's website and produced a numbered list of the organization's \
distinct activities. The list will be used to classify the organization's work against \
taxonomies and to divide its funding between its activities. Your job is to decide, for each \
listed activity, whether it belongs on that list, whether it repeats an earlier activity, and \
whether any of its details are unsupported in a way that matters.

You will receive the organization's name, the source text the extraction model read (webpage URLs \
and page text, delineated by lines of `----` characters), the description on record for the \
organization (from a separate database, possibly out of date), and then the numbered list of \
extracted activities. Each activity gives its name, what happens, who carries it out, its aim, \
sector, methods, geography, and a quote the extraction model offered as evidence.

Work in order for each activity: the work check, then the label, the overlap, the field notes, \
and the field issues. Do not decide a verdict first and justify it afterwards.

1. WORK CHECK
Answer one question from the source: is this concrete work, current, and done by this \
organization?
- Concrete: the source says the organization does this work and names what the work is: a \
program, service, product, project, or a specific kind of work (making grants, policy work, \
monitoring, training). Take what the source says the organization does at face value, including \
work it lists among its goals or objectives; do not try to verify that the work happens. Mission, \
values, or aim statements that name no concrete work ("promote the principles of soil health", \
"build a sustainable future", "foster partnerships") are not activities.
- Current: the source does not say the work has ended. Work the source says was discontinued, \
with nothing continuing, is not current; work that continues in another form is.
- This organization: the organization does, funds, or enables the work itself, rather than a \
partner it raises money for, a network it belongs to, or an agency whose program it promotes.

2. LABEL EACH ACTIVITY WITH EXACTLY ONE OF
- line_of_work — concrete, current work of this organization: a program, product, service, or \
line of work.
- minor_item — real, but a one-off or incidental item rather than a line of work: a single event \
or fundraiser, one campaign or announcement, a side offering mentioned once in passing.
- not_an_activity — not something the organization does as work: mission, values, or aim \
language that names no concrete work, content the organization publishes (individual \
blog posts, articles, news items) treated as if its topic were the organization's work, \
organizational facts (history, staff, board, partners, awards, locations), donation or membership \
appeals and their perks, a variant or pricing tier of a product already listed. A publishing, \
media, or education program that is itself the organization's work IS a line of work.
- unsupported — the source does not say the organization does this work, describes materially \
different work, presents it as ended with nothing continuing, or shows another organization \
doing it. When the work is real but some details are not supported, do not use this label: \
label the work for what it is, and record the details under 4.

3. SAY WHETHER IT REPEATS AN EARLIER ACTIVITY
The extraction model was told to keep one record per line of work. A line of work is defined by \
three things: the kind of work (its method — doing it, funding it, training others, advocating, \
researching, selling a product), who carries it out, and what it is aimed at. Programs, projects, \
sites, events, or products that share all three are ONE activity, even when the source names them \
separately. Activities are separate only when they differ in kind of work, in who carries it out, \
in what they are aimed at, or in the sector they serve; different locations, seasons, or \
audiences of the same kind are not reasons to split. A company's product lines that serve the \
same purpose for the same customers are one activity.
- none — no earlier activity covers the same line of work.
- duplicate — the same work as an earlier activity, described again.
- should_merge — a separate program, project, site, event, product, audience, or slice of the \
same line of work as an earlier activity, which the rule above puts in one record.
For duplicate and should_merge, give the number of the earlier activity in same_work_as; \
otherwise null. Only point back to an earlier number. Judge the overlap whatever the label.

4. CHECK EACH FIELD AGAINST THE SOURCE
In field_notes, check the claims in what_happens, who_carries_it_out, aim, sector, methods, \
geography, and the activity name against the source. Then list each unsupported claim in \
field_issues with its field, the claim quoted, its support, and its impact. When every claim is \
supported, write "none" and leave the list empty.

Supported, and NOT an issue:
- what the source states, condensed, generalized, or reworded;
- an inference the source's own contents make effectively certain. A place is supported when the \
source shows the organization's work happening there (an organization whose every program is in \
Oakland works in Oakland), and the state or country containing a named place is supported;
- "not stated", "no specific aim stated", or saying the source is ambiguous;
- a detail the field leaves out. A missing detail is never an issue.

Support, for each issue:
- inferred — plausible but uncertain: every element appears in the source but its scope, subject, \
or strength is stretched (a stronger verb, a program placed under the wrong activity, a place \
from the organization's general scope applied to this work).
- fabricated — an element absent from the source entirely, or contradicting it: a place, partner, \
method, number, or format the source never mentions.

Impact, for each issue, judged separately from support — an issue can be real and still not \
matter. Apply one test: would fixing the claim change the answer to the work check (concrete, \
current, this organization; or who does the work versus funds or enables it), or change how this \
activity would be classified (a different kind of work, sector, beneficiary, or a method or place \
that would add or remove a category)?
- material — yes. The claim changes who is credited with the work, whether it is concrete or \
current, or its classification.
- none — no. The activity would be understood and classified the same way: a format (online or \
in person), a qualifier (kiln drying, one-on-one, branded), a supporting partner, a county \
inside a named city's region, a detail from an earlier version of a program that continues.
Most issues are "none". Do not mark "material" because you found an issue; that is a separate \
question about consequence.

Worked examples (claim, what the source says, verdict):
- what_happens "hosts online and in-person speaker events"; the source marks one talk "Join us \
online" and gives no format for the others → fabricated, none.
- geography "Oakland" for a time bank; every program on the site is in Oakland → supported, no \
issue.
- who_carries_it_out "carries it out through its own staff"; the source calls the organization \
"the non-profit partner" of a county park whose staff run the programs → inferred, material.
- what_happens "publishes a bibliography"; the source says it organized one and lists it as \
"Coming someday" → inferred, none.
- what_happens describes free seed packets "provided" in a pollinator project; the source says \
that project was discontinued and the organization now helps people plant pollinator plants as \
part of another program → inferred, none.
- methods "invasive species control" under easement stewardship; the source points easement \
owners to state cost-share funds for invasive control, and the organization removes invasives \
at its own preserve → inferred, none.
- geography "California" for restoration work the source places in Oregon → fabricated, material.
- "Create an internship program in organic farming", listed among a farm's objectives → taken at \
face value: a real activity, not_an_activity only if it named no concrete work.
- "Promote the principles of soil health" as the only basis for an advocacy activity → \
not_an_activity: an aim with no concrete work named.

ALSO RATE PROMINENCE, for every activity, as the source presents it
- core — central to what the organization is and does.
- secondary — a real part of its work, but not central.
- minor — peripheral, mentioned briefly.

RULES
- Judge from the source text only. Do not use outside knowledge of the organization.
- A thin source is not a reason to reject an activity the source does support.
- Return one verdict for every listed activity, in list order.

ALSO REPORT, FOR THE WEBSITE AS A WHOLE
- site_identity — Compare the source text with the description on record. "same_organization" \
when the website is plausibly the same organization, even if its work has changed, grown, or is \
described differently. "different_entity" when the website plainly belongs to something else: a \
different business or organization, an unrelated blog, a parked, for-sale, or resold domain. \
"unclear" when the source is too thin to tell or the record is missing. The activity verdicts \
above are still judged against the source text, whatever this answer is.
- site_identity_reason — one sentence.
"""

# v3.2 (2026-11-12): v3.1 plus a recurrence rating, so the extraction model's
# recurrence can be compared with the judge's in code, as prominence already is. The
# judge never sees the model's prominence or recurrence, so it cannot anchor on them.
# Everything else in the prompt is v3.1's, to keep the other verdicts where they were.


class ActivityVerdictV3_2(ActivityVerdictV3):
    """A v3 verdict with the judge's own recurrence rating, asked after prominence."""

    recurrence: Literal["ongoing", "recurring", "one_off", "past"]


class PrecisionVerdictsV3_2(BaseModel):
    """v3.2 verdicts on every activity extracted for one organization."""

    verdicts: list[ActivityVerdictV3_2]
    site_identity: Literal["same_organization", "different_entity", "unclear"]
    site_identity_reason: str


PRECISION_V3_2_NAME = "activity_extraction_precision_judge_v3_2"

_PROMINENCE_V3 = """\
ALSO RATE PROMINENCE, for every activity, as the source presents it
- core — central to what the organization is and does.
- secondary — a real part of its work, but not central.
- minor — peripheral, mentioned briefly.
"""
# Recurrence definitions are the extraction prompt's own (general.py, item 11).
_PROMINENCE_AND_RECURRENCE = """\
ALSO RATE PROMINENCE AND RECURRENCE, for every activity, as the source presents it
Prominence:
- core — central to what the organization is and does.
- secondary — a real part of its work, but not central.
- minor — peripheral, mentioned briefly.
Recurrence:
- ongoing — a continuing program, product, or service.
- recurring — something repeated on a cycle (an annual event, seasonal camps).
- one_off — a single event, campaign, or project.
- past — the source presents it as completed or no longer running.
"""
PRECISION_V3_2_INSTRUCTIONS = PRECISION_V3_INSTRUCTIONS.replace(
    _PROMINENCE_V3, _PROMINENCE_AND_RECURRENCE
)


class MissedActivityV2(BaseModel):
    """One line of work recall v2 says the list misses; the comparison comes before the kind."""

    name: str = Field(description="Short noun phrase naming the work")
    evidence: str = Field(description="Verbatim phrase from the source, at most 25 words")
    nearest_activity: int | None = Field(description="Number of the closest listed activity")
    difference: str = Field(
        description="How it differs from that activity in kind of work, doer, aim, or sector"
    )
    kind: Literal["line_of_work", "minor_item"]
    prominence: Literal["core", "secondary", "minor"]


class RecallVerdictV2(BaseModel):
    """Every missed line of work for one organization; empty when the list is complete."""

    missed: list[MissedActivityV2]


RECALL_V2_NAME = "activity_extraction_recall_judge_v2_1"  # same change as v3.1

# The v1 prompt (23feaa42...) with the evidence and covered rules tightened.
RECALL_V2_INSTRUCTIONS = """\
You are auditing a structured record of what an organization does. An extraction model read text \
scraped from the organization's website and produced a numbered list of the organization's \
distinct activities. Your job is to find the lines of work the source describes that the list \
MISSES.

You will receive the organization's name, the source text the extraction model read (webpage URLs \
and page text, delineated by lines of `----` characters), and then the numbered list of \
extracted activities, each with what happens, who carries it out, its aim, and its sector.

WHAT COUNTS AS AN ACTIVITY (the extraction model's own definition)
An activity is a concrete program, product, service, project, or line of work the organization \
itself runs: something with a described method, audience, customer, site, or output behind it. \
These are NOT activities: mission, values, or aim language that names no concrete work; individual pieces of published content (blog posts, articles, news \
items) treated as if their topic were the organization's work; newsletters and other \
communications unless the source describes a publishing program; news about work done by \
someone else; donation, membership, or volunteer-sign-up appeals and their perks or discounts; \
lists of partners, funders, or clients; organizational facts (history, staff, awards, \
locations); sales policies and product variants, sizes, or pricing tiers; work the source \
presents as completed or ended.

EVIDENCE
Report only work the source DESCRIBES. A navigation menu entry, link title, page heading, button, \
or dated event listing with no sentence describing the work is not evidence that the \
organization does it, however suggestive the label.

GRANULARITY (the extraction model's own rule)
A line of work is defined by its kind of work (doing it, funding it, training others, advocating, \
researching, selling a product), who carries it out, and what it is aimed at. Programs, projects, \
sites, events, or products that share all three are ONE activity, even when the source names them \
separately. A different audience, customer group, site, season, or named program of the same \
kind of work is not a reason to split: weatherizing houses of worship is the same line of work as \
weatherizing homes. A component, tool, or app that comes with a listed service or product is \
part of it. A company's product lines that serve the same purpose for the same customers are one \
activity.

WHAT TO REPORT
List each line of work in the source that no listed activity covers.
- An item is COVERED, and must not be reported, when a listed activity names it, or when it \
shares kind of work, doer, and aim with a listed activity and so would be merged into it under \
the rule above, even if the listed activity's description does not name it.
- Report an item as missed only when it differs from every listed activity in kind of work, in \
who carries it out, in what it is aimed at, or in the sector it serves. Name the nearest listed \
activity and the difference; if you cannot name a concrete difference, it is covered.
- Do not report anything the source does not say the organization itself does.
- Judge from the source text only. Do not use outside knowledge.
- If the list is complete, return an empty list. An empty list is a common, correct answer; do \
not search for marginal items to fill it.

FOR EACH MISSED ITEM
- name — a short noun phrase naming the work (at most 8 words).
- evidence — a verbatim phrase from the source, at most 25 words, showing the organization doing \
this work.
- nearest_activity — the number of the closest listed activity, or null when the list is empty.
- difference — one sentence on how it differs from that activity in kind of work, doer, aim, or \
sector.
- kind — "line_of_work" for a concrete, continuing program, product, service, or line of work; \
"minor_item" for a real but one-off or incidental item (a single event or fundraiser, one \
campaign, a side offering mentioned once).
- prominence — as the source presents it: "core" (central to what the organization does), \
"secondary" (a real part of its work, not central), or "minor" (peripheral, mentioned briefly).
"""


def tagged_prompt(instructions: str, model: type[BaseModel]) -> str:
    """Instructions plus the schema tag, in the originals' form, so a schema change is a new prompt."""
    return f"{instructions}\n\n[response schema {model.__name__} #{schema_fingerprint(model)}]"


@dataclass(frozen=True)
class Judge:
    """One judge: its prompt (stored, or written here), response model and request settings.

    The ported judges load their prompt by ``prompt_id``; a judge written here gives
    ``instructions`` instead, and its prompt row is created from them on first use.
    """

    name: JudgeName
    prompt_id: str | None
    response_model: type[BaseModel]
    model: str
    reasoning: dict | None = None
    instructions: str | None = None
    prompt_name: str = ""

    def kwargs(self) -> dict:
        """Request kwargs as the originals sent them; text_format is not hashed."""
        out = {"text_format": self.response_model}
        if self.reasoning:
            out["reasoning"] = self.reasoning
        return out

    def prompt_str(self) -> str | None:
        """The full prompt for a judge written here; None for a stored one."""
        return tagged_prompt(self.instructions, self.response_model) if self.instructions else None


# The colleagues' precision judge. No longer called, but its stored inputs hold the
# descriptions on record, and its stored outputs seed the dry-run estimate.
PRECISION_V1 = Judge("precision", "fe735140-6951-5550-a9e3-cb3872e86bdc", PrecisionVerdicts, SOL)

# Earlier versions, kept to read their stored verdicts (the 2026-11-09 full run).
PRECISION_V2 = Judge(
    "precision",
    None,
    PrecisionVerdictsV2,
    SOL,
    instructions=PRECISION_V2_INSTRUCTIONS,
    prompt_name=PRECISION_V2_NAME,
)
RECALL_V1 = Judge("recall", "23feaa42-1062-5671-843b-59bef85996e5", RecallVerdict, SOL)
# The 2026-11-11 full run (data_out/activity_extraction_judges/v3_1).
PRECISION_V3_1 = Judge(
    "precision",
    None,
    PrecisionVerdictsV3,
    SOL,
    instructions=PRECISION_V3_INSTRUCTIONS,
    prompt_name=PRECISION_V3_NAME,
)

JUDGE_SPECS: dict[JudgeName, Judge] = {
    "precision": Judge(
        "precision",
        None,
        PrecisionVerdictsV3_2,
        SOL,
        instructions=PRECISION_V3_2_INSTRUCTIONS,
        prompt_name=PRECISION_V3_2_NAME,
    ),
    "recall": Judge(
        "recall",
        None,
        RecallVerdictV2,
        SOL,
        instructions=RECALL_V2_INSTRUCTIONS,
        prompt_name=RECALL_V2_NAME,
    ),
    "alignment": Judge(
        "alignment",
        probe.ALIGNMENT_PROMPT_ID,
        ListAlignment,
        ALIGN_MODEL,
        ALIGN_REASONING,
    ),
}


def schema_fingerprint(model: type[BaseModel]) -> str:
    """The tag the originals append to the prompt: md5 of the sorted JSON schema, 12 chars."""
    schema_json = json.dumps(model.model_json_schema(), sort_keys=True)
    return hashlib.md5(schema_json.encode("utf-8")).hexdigest()[:12]


def check_fingerprint(prompt_str: str, model: type[BaseModel]) -> None:
    """Stop unless the stored prompt was built for exactly this response model."""
    expected = f"[response schema {model.__name__} #{schema_fingerprint(model)}]"
    if not prompt_str.rstrip().endswith(expected):
        raise SystemExit(
            f"{model.__name__} does not match its stored judge prompt (want {expected!r}); "
            "the ported schema or the pydantic version differs from the original"
        )


_ILLEGAL_CHARS = dict.fromkeys(list(range(0x00, 0x09)) + [0x0B, 0x0C] + list(range(0x0E, 0x20)))


def scrub(value: str) -> str:
    """Delete control characters, keeping tab, newline and carriage return (as the originals)."""
    return value.translate(_ILLEGAL_CHARS)


# --- Inputs ---


@dataclass
class Run:
    """One extraction run over the cohort: the baseline or a candidate route."""

    key: str
    profiles: dict[str, GeneralActivityProfile]
    invalid: list[str] = field(default_factory=list)


def run_key(route_label: str) -> str:
    """A given_id-safe name for a route: 'baseten:glm-5.3-flash' -> 'baseten-glm-5.3-flash'."""
    return route_label.replace(":", "-")


def load_runs(session, orgs: list[probe.OrgInput], routes: list[str]) -> list[Run]:
    """The baseline run plus each candidate's stored schema-mode extractions."""
    runs = [
        Run(
            BASELINE_RUN,
            {o.org_id: GeneralActivityProfile.model_validate_json(o.baseline_text) for o in orgs},
        )
    ]
    baseline_prompt = probe._prompt_text(session)
    prompt_str, pairs, text_format = probe.build_requests("schema", orgs, baseline_prompt)
    for label in routes:
        route = probe.ROUTES[label]
        cache = probe.make_cache(session, route, "schema", prompt_str)
        ids = [g for g, _ in pairs]
        rows = probe.cached_rows(session, cache, ids, probe.request_kwargs(route, text_format))
        run = Run(run_key(label), {})
        for org in orgs:
            row = rows.get(probe.given_id("schema", org.org_id))
            try:
                run.profiles[org.org_id] = GeneralActivityProfile.model_validate_json(
                    row.response_text
                )
            except (AttributeError, TypeError, ValidationError, ValueError):
                run.invalid.append(org.org_id)
        if run.invalid:
            logger.warning("%s: %d orgs have no valid extraction", label, len(run.invalid))
        runs.append(run)
    return runs


_DESCRIPTION = re.compile(
    r"\n\n========\nDESCRIPTION ON RECORD\n========\n\n(.*?)\n\n========\nEXTRACTED ACTIVITIES\n",
    re.S,
)


def tracker_descriptions(session, sample_file: str) -> dict[str, str]:
    """Descriptions from the tracker schema a sample file was drawn from (``source_schema``)."""
    meta = json.loads(Path(sample_file).read_text())
    schema = meta["source_schema"]
    if not re.fullmatch(r"[a-z0-9_]+", schema):
        raise SystemExit(f"Unexpected schema name {schema!r}")
    rows = session.execute(
        text(f"select uid, description from {schema}.organization where uid = any(:ids)"),
        {"ids": [o["org_id"] for o in meta["orgs"]]},
    ).all()
    return {uid: d for uid, d in rows if d}


def load_descriptions(session, org_ids: list[str]) -> dict[str, str]:
    """The description on record each org was judged with, from the original precision inputs."""
    rows = session.execute(
        text(
            "select given_id, input_text from prompt_response "
            "where prompt_id = :p and model_name = :m and given_id = any(:ids)"
        ),
        {"p": PRECISION_V1.prompt_id, "m": SOL, "ids": org_ids},
    ).all()
    out = {}
    for org_id, input_text in rows:
        match = _DESCRIPTION.search(input_text or "")
        if match:
            out[org_id] = match.group(1)
    return out


def org_name(input_text: str) -> str:
    """The organization name on the first line of an extraction input."""
    first = input_text.split("\n", 1)[0]
    return first.removeprefix("ORGANIZATION: ")


# --- Renderers: recall and alignment verbatim in output from oneearth; precision is the v2
# layout (the v1 list had name, what happens, who and evidence only). Numbers are 1-based. ---


def render_precision_list(profile: GeneralActivityProfile) -> str:
    """The numbered list the precision judge sees: every field, evidence last (v2 layout)."""
    return "\n\n".join(
        "\n".join(
            [
                f"{i}. {a.activity}",
                f"   What happens: {a.what_happens}",
                f"   Who carries it out: {a.who_carries_it_out}",
                f"   Aim: {a.aim}",
                f"   Sector: {a.sector}",
                f"   Methods: {a.methods}",
                f"   Geography: {a.geography}",
                f"   Evidence quoted by the extraction model: {a.evidence}",
            ]
        )
        for i, a in enumerate(profile.activities, start=1)
    )


def render_recall_list(profile: GeneralActivityProfile | None) -> str:
    """The numbered list the recall judge sees: name, what happens, who (no evidence)."""
    if profile is None or not profile.activities:
        return "(no activities listed)"
    return "\n\n".join(
        "\n".join(
            [
                f"{i}. {a.activity}",
                f"   What happens: {a.what_happens}",
                f"   Who carries it out: {a.who_carries_it_out}",
            ]
        )
        for i, a in enumerate(profile.activities, start=1)
    )


def render_recall_list_v2(profile: GeneralActivityProfile | None) -> str:
    """Recall v2's list: v1's fields plus aim and sector, which its covered rule turns on."""
    if profile is None or not profile.activities:
        return "(no activities listed)"
    return "\n\n".join(
        "\n".join(
            [
                f"{i}. {a.activity}",
                f"   What happens: {a.what_happens}",
                f"   Who carries it out: {a.who_carries_it_out}",
                f"   Aim: {a.aim}",
                f"   Sector: {a.sector}",
            ]
        )
        for i, a in enumerate(profile.activities, start=1)
    )


def render_alignment_list(profile: GeneralActivityProfile) -> str:
    """One list for the alignment prompt: number, name, what happens, who carries it out."""
    return "\n".join(
        f"{i}. {a.activity} — {a.what_happens} Who: {a.who_carries_it_out}"
        for i, a in enumerate(profile.activities, start=1)
    )


def judge_inputs(
    judge: JudgeName,
    run: Run,
    orgs: list[probe.OrgInput],
    descriptions: dict[str, str],
    baseline: Run,
) -> list[tuple[str, str]]:
    """The (given_id, input) pairs one judge sees for one run, in the originals' format.

    Precision skips orgs with no activities (the original iterates the activity
    table); recall judges every org with source text; alignment needs activities on
    both sides and is undefined for the baseline itself.
    """
    pairs = []
    for org in orgs:
        profile = run.profiles.get(org.org_id)
        prefix = BASELINE_JUDGE_RUN if run.key == BASELINE_RUN else JUDGE_RUN
        gid = f"{prefix}_{run.key}|{org.org_id}"
        source = org.input_text
        if judge == "precision":
            if profile is None or not profile.activities:
                continue
            body = (
                f"{source}\n\n========\nDESCRIPTION ON RECORD\n========\n\n"
                f"{descriptions.get(org.org_id) or 'none on record'}\n\n"
                f"========\nEXTRACTED ACTIVITIES\n========\n\n{render_precision_list(profile)}"
            )
        elif judge == "recall":
            if org.org_id in run.invalid:
                continue
            render = (
                render_recall_list_v2
                if JUDGE_SPECS["recall"].response_model is RecallVerdictV2
                else render_recall_list
            )
            body = f"{source}\n\n========\nEXTRACTED ACTIVITIES\n========\n\n{render(profile)}"
        else:
            base = baseline.profiles.get(org.org_id)
            if run.key == BASELINE_RUN or not (profile and profile.activities):
                continue
            if not (base and base.activities):
                continue
            body = (
                f"ORGANIZATION: {org_name(source)}\n\nLIST A\n{render_alignment_list(base)}"
                f"\n\nLIST B\n{render_alignment_list(profile)}"
            )
        pairs.append((gid, scrub(body)))
    return pairs


# --- Calling and reading the judges ---


def make_judge_cache(session, judge: Judge):
    """The shared cache for one judge, on its stored prompt row or one made from its text."""
    cache = probe.DeadlineCache(
        session=session,
        prompt_id=judge.prompt_id,
        prompt_str=judge.prompt_str(),
        prompt_name=judge.prompt_name,
        model=judge.model,
        filter_by_model=True,
    )
    check_fingerprint(cache.prompt.prompt_str, judge.response_model)
    return cache


def stored_judge_rows(session, cache, judge: Judge, given_ids: list[str]) -> dict:
    """Every stored row for these judge keys, failures included."""
    request_hash = PromptResponse.create_request_hash(cache._request_kwargs(judge.kwargs()))
    stmt = select(PromptResponse).where(
        PromptResponse.prompt_id == cache.prompt.id,
        PromptResponse.given_id.in_(given_ids),
        PromptResponse.model_name == judge.model,
        PromptResponse.request_hash == request_hash,
    )
    return {row.given_id: row for row in session.execute(stmt).scalars()}


def parse_verdict(row, model: type[BaseModel]):
    """The validated verdict in a stored row, or None for a failure or bad output."""
    if row is None or row.num_errors or not row.response_text:
        return None
    try:
        return model.model_validate_json(row.response_text)
    except ValidationError:
        return None


# --- Scoring ---


def score_alignment(n_a: int, n_b: int, alignment: ListAlignment) -> dict:
    """Per-org agreement, with the original's repair of invalid and missing indices.

    Out-of-range or repeated indices are dropped; a group with an empty side becomes
    only_a/only_b; an only_* group with both sides, or a 'same' group with more than
    one item on a side, becomes merge_split; indices the judge left out are added as
    unmatched. Numbers are 1-based, as shown to the judge.
    """
    a_ids, b_ids = set(range(1, n_a + 1)), set(range(1, n_b + 1))
    seen_a, seen_b, groups = set(), set(), []
    for g in alignment.groups:
        ga = [i for i in g.a if i in a_ids and i not in seen_a]
        gb = [i for i in g.b if i in b_ids and i not in seen_b]
        seen_a |= set(ga)
        seen_b |= set(gb)
        if ga or gb:
            relation = g.relation
            if not ga or not gb:
                relation = "only_a" if ga else "only_b"
            elif relation in ("only_a", "only_b") or (
                relation == "same" and (len(ga) > 1 or len(gb) > 1)
            ):
                relation = "merge_split"
            groups.append((ga, gb, relation))
    missing_a, missing_b = a_ids - seen_a, b_ids - seen_b
    groups += [([i], [], "only_a") for i in missing_a]
    groups += [([], [i], "only_b") for i in missing_b]
    n = n_a + n_b
    in_same = sum(len(ga) + len(gb) for ga, gb, r in groups if r == "same")
    in_both = sum(len(ga) + len(gb) for ga, gb, r in groups if ga and gb)
    return {
        "n_a": n_a,
        "n_b": n_b,
        "n_merge_split": sum(r == "merge_split" for _, _, r in groups),
        "n_only_a": sum(r == "only_a" for _, _, r in groups),
        "n_only_b": sum(r == "only_b" for _, _, r in groups),
        "content_agreement": round(in_both / n, 3) if n else 1.0,
        "granularity_agreement": round(in_same / n, 3) if n else 1.0,
        "repaired": bool(missing_a or missing_b),
    }


_LIST_ITEM = re.compile(r"^(\d+)\. ", re.M)


def list_lengths(alignment_input: str) -> tuple[int, int]:
    """Item counts of LIST A and LIST B in an alignment input, for re-scoring old rows."""
    _, rest = alignment_input.split("\nLIST A\n", 1)
    list_a, list_b = rest.split("\n\nLIST B\n", 1)
    return len(_LIST_ITEM.findall(list_a)), len(_LIST_ITEM.findall(list_b))


def noise_floor(session) -> pd.DataFrame:
    """Per-org agreement between two baseline runs, re-scored from the cached agree_* rows."""
    judge = JUDGE_SPECS["alignment"]
    rows = session.execute(
        text(
            "select given_id, input_text, response_text from prompt_response "
            "where prompt_id = :p and model_name = :m and num_errors is null "
            "and split_part(given_id, '|', 1) = any(:families)"
        ),
        {"p": judge.prompt_id, "m": judge.model, "families": list(NOISE_FLOOR_FAMILIES)},
    ).all()
    out = []
    for gid, input_text, response_text in rows:
        family, org_id = gid.split("|", 1)
        n_a, n_b = list_lengths(input_text)
        alignment = ListAlignment.model_validate_json(response_text)
        out.append({"run": family, "org_id": org_id, **score_alignment(n_a, n_b, alignment)})
    return pd.DataFrame(out)


# Outcomes counted as precision errors. should_merge is reported beside them, not
# among them: a finer cut can still map to the right taxonomy nodes, and whether it
# costs anything depends on the consumer (it skews per-activity funding shares).
# v3 counts a field issue as an error only when it is material ("material_issue");
# "minor_issue" is real work with only immaterial issues and is reported, not counted.
# "embellished" is v2's outcome (any unsupported field), kept to read the v2 run.
ERROR_OUTCOMES = ("not_an_activity", "unsupported", "duplicate", "material_issue", "embellished")
UNJUDGED = ("judge_failed", "missing_verdict")

# Severity per field issue, derived here rather than asked of the judge, as in the
# summarization benchmark: models are steadier choosing categories than numbers, and
# re-weighting is an edit here plus a recount, with no new calls. A wrong label or a
# duplicate weighs as much as a fabricated, material detail.
SEVERITY = {
    ("inferred", "none"): 0.5,
    ("inferred", "material"): 1.0,
    ("fabricated", "none"): 1.0,
    ("fabricated", "material"): 3.0,
}
LABEL_SEVERITY = 3.0


def precision_outcome(verdict: ActivityVerdictV2 | ActivityVerdictV3) -> str:
    """One outcome per activity: an error label first, then a repeat, then field issues."""
    if verdict.label in ("not_an_activity", "unsupported"):
        return verdict.label
    if verdict.overlap == "duplicate":
        return "duplicate"
    if isinstance(verdict, ActivityVerdictV2):
        return "embellished" if verdict.unsupported_fields else verdict.label
    if any(issue.impact == "material" for issue in verdict.field_issues):
        return "material_issue"
    if verdict.field_issues:
        return "minor_issue"
    return verdict.label


def activity_severity(verdict: ActivityVerdictV3) -> float:
    """The activity's worst issue: a wrong label or a duplicate, else its worst field issue."""
    if verdict.label in ("not_an_activity", "unsupported") or verdict.overlap == "duplicate":
        return LABEL_SEVERITY
    return max((SEVERITY[(i.support, i.impact)] for i in verdict.field_issues), default=0.0)


PROMINENCE_RANK = {"minor": 0, "secondary": 1, "core": 2}


# Pairs that say the work has ended on one side and is running on the other. One-off
# versus past is left out: a completed single event fits both definitions (review
# item 45, 2026-11-12).
_CURRENCY_MISMATCH = {frozenset({"past", "ongoing"}), frozenset({"past", "recurring"})}


def recurrence_mismatch(model: str, judge: str) -> str:
    """'none', 'material' when one side calls running work past, else 'minor'.

    Whether work is current decides whether it is classified as the organization's
    work at all; ongoing versus recurring versus one-off changes nothing downstream.
    """
    if model == judge:
        return "none"
    return "material" if frozenset({model, judge}) in _CURRENCY_MISMATCH else "minor"


def _issue_fields(v: ActivityVerdictV3, material: bool) -> str:
    """The fields with an issue, or with a material one, in the judge's order."""
    names = [i.field for i in v.field_issues if not material or i.impact == "material"]
    return "; ".join(dict.fromkeys(names))


def precision_rows(run: Run, verdicts: dict[str, PrecisionVerdictsV3 | None]) -> list[dict]:
    """One row per extracted activity with its outcome and severity.

    An activity the judge skipped is 'missing_verdict'; every activity of an org
    whose judge call failed is 'judge_failed'.
    """
    out = []
    for org_id, verdict in verdicts.items():
        profile = run.profiles[org_id]
        by_index = {v.activity_index: v for v in verdict.verdicts} if verdict else {}
        for i, activity in enumerate(profile.activities, start=1):
            v = by_index.get(i)
            if v:
                outcome = precision_outcome(v)
            else:
                outcome = "missing_verdict" if verdict else "judge_failed"
            out.append(
                {
                    "run": run.key,
                    "org_id": org_id,
                    "activity_index": i,
                    "label": v.label if v else None,
                    "outcome": outcome,
                    "severity": activity_severity(v) if v else None,
                    "should_merge": v.overlap == "should_merge" if v else None,
                    "same_work_as": v.same_work_as if v else None,
                    "unsupported_fields": _issue_fields(v, material=False) if v else None,
                    "material_fields": _issue_fields(v, material=True) if v else None,
                    "site_identity": verdict.site_identity if verdict else None,
                    "model_prominence": activity.prominence,
                    "judge_prominence": v.prominence if v else None,
                    "model_recurrence": activity.recurrence,
                    "judge_recurrence": getattr(v, "recurrence", None),
                }
            )
    return out


def recall_rows(run: Run, verdicts: dict[str, RecallVerdict | None]) -> list[dict]:
    """One row per judged org: activities listed and missed counts by kind."""
    out = []
    for org_id, verdict in verdicts.items():
        profile = run.profiles.get(org_id)
        missed = verdict.missed if verdict else []
        out.append(
            {
                "run": run.key,
                "org_id": org_id,
                "judged": verdict is not None,
                "n_activities": len(profile.activities) if profile else 0,
                "missed_line_of_work": sum(m.kind == "line_of_work" for m in missed),
                "missed_minor_item": sum(m.kind == "minor_item" for m in missed),
            }
        )
    return out


def paired_ci(diffs: np.ndarray, seed: int = 0) -> tuple[float, float]:
    """95% bootstrap interval for the mean of per-org paired differences."""
    if len(diffs) == 0:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    means = rng.choice(diffs, size=(BOOTSTRAP_DRAWS, len(diffs)), replace=True).mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def report(
    precision: pd.DataFrame, recall: pd.DataFrame, alignment: pd.DataFrame, floor: pd.DataFrame
) -> str:
    """Markdown report: per run, then each candidate's paired difference from the baseline."""
    lines = ["# Judged comparison against the gpt-5.4-mini baseline", ""]
    if len(precision):
        judged = precision[~precision["outcome"].isin(UNJUDGED)]
        lines += [
            "## Precision (gpt-6-sol, judge v3.2), share of judged activities",
            "",
            "Errors are not_an_activity, unsupported, duplicate and material_issue (real work "
            "with a detail that changes who is credited, whether it is concrete or current, or its "
            "classification). minor_issue (real work whose only unsupported details change "
            "nothing) and should_merge (a finer cut of an earlier activity's line of work) are "
            "reported apart from the errors. Severity per activity: 0 clean; 0.5 inferred and "
            "immaterial; 1 inferred and material, or fabricated and immaterial; 3 fabricated and "
            "material, a wrong label, or a duplicate.",
            "",
            "| run | orgs | activities | errors | not_an_activity | unsupported | duplicate "
            "| material_issue | minor_issue | minor_item | should_merge | mean severity "
            "| judge failed |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for run, g in precision.groupby("run", sort=False):
            j = judged[judged["run"] == run]
            share = j["outcome"].value_counts(normalize=True)
            cells = " | ".join(
                f"{share.get(k, 0):.1%}"
                for k in (
                    "not_an_activity",
                    "unsupported",
                    "duplicate",
                    "material_issue",
                    "minor_issue",
                    "minor_item",
                )
            )
            errors = j["outcome"].isin(ERROR_OUTCOMES).mean() if len(j) else float("nan")
            merge = j["should_merge"].astype(bool).mean() if len(j) else float("nan")
            severity = j["severity"].mean() if len(j) else float("nan")
            failed = g["outcome"].isin(UNJUDGED).mean()
            lines.append(
                f"| {run} | {g['org_id'].nunique()} | {len(g)} | {errors:.1%} | {cells} "
                f"| {merge:.1%} | {severity:.2f} | {failed:.1%} |"
            )
        for column, title in (
            ("material_fields", "Fields with a material issue"),
            ("unsupported_fields", "Fields with any issue"),
        ):
            lines += [
                "",
                f"### {title}, share of judged activities",
                "",
                "| run | " + " | ".join(ACTIVITY_FIELDS) + " |",
                "|---|" + "---|" * len(ACTIVITY_FIELDS),
            ]
            for run, j in judged.groupby("run", sort=False):
                faults = j[column].fillna("").str.split("; ")
                cells = " | ".join(
                    f"{faults.map(lambda f: name in f).mean():.1%}" for name in ACTIVITY_FIELDS
                )
                lines.append(f"| {run} | {cells} |")
        lines += _rating_section(judged)
        lines.append("")
    if len(recall):
        lines += [
            "## Recall (gpt-6-sol, judge v2.1), per org",
            "",
            "| run | orgs judged | missed line_of_work (mean) | missed minor_item (mean) "
            "| orgs missing no line_of_work |",
            "|---|---|---|---|---|",
        ]
        for run, g in recall[recall["judged"]].groupby("run", sort=False):
            lines.append(
                f"| {run} | {len(g)} | {g['missed_line_of_work'].mean():.2f} "
                f"| {g['missed_minor_item'].mean():.2f} "
                f"| {(g['missed_line_of_work'] == 0).mean():.0%} |"
            )
        lines.append("")
    if len(alignment) or len(floor):
        lines += [
            "## Alignment with the baseline (gpt-5.4-mini, medium), mean per org",
            "",
            "| comparison | orgs | content agreement | granularity agreement |",
            "|---|---|---|---|",
        ]
        for run, g in floor.groupby("run", sort=True):
            lines.append(
                f"| noise floor: two baseline runs ({run}) | {len(g)} "
                f"| {g['content_agreement'].mean():.3f} | {g['granularity_agreement'].mean():.3f} |"
            )
        for run, g in alignment.groupby("run", sort=False):
            lines.append(
                f"| baseline vs {run} | {len(g)} | {g['content_agreement'].mean():.3f} "
                f"| {g['granularity_agreement'].mean():.3f} |"
            )
        lines.append("")
    lines += _paired_section(precision, recall)
    return "\n".join(lines)


def _rating_section(judged: pd.DataFrame) -> list[str]:
    """The extraction model's prominence and recurrence against the judge's blind ratings."""
    if "judge_recurrence" not in judged:
        return []
    rated = judged.dropna(subset=["judge_prominence", "judge_recurrence"])
    if not len(rated):
        return []
    lines = [
        "",
        "### Prominence and recurrence, extraction model vs the judge's own rating",
        "",
        "The judge rates both from the source without seeing the model's values. A recurrence "
        "mismatch is material when one side calls ongoing or recurring work past (it decides "
        "whether the work is current), minor otherwise; one-off versus past is minor, since a "
        "completed single event fits both.",
        "",
        "| run | activities | prominence agrees | model rates higher | model rates lower "
        "| two steps apart | recurrence agrees | recurrence material | recurrence minor |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for run, g in rated.groupby("run", sort=False):
        step = g["model_prominence"].map(PROMINENCE_RANK) - g["judge_prominence"].map(
            PROMINENCE_RANK
        )
        mismatch = [
            recurrence_mismatch(m, j) for m, j in zip(g["model_recurrence"], g["judge_recurrence"])
        ]
        share = pd.Series(mismatch).value_counts(normalize=True)
        lines.append(
            f"| {run} | {len(g)} | {(step == 0).mean():.1%} | {(step > 0).mean():.1%} "
            f"| {(step < 0).mean():.1%} | {(step.abs() == 2).mean():.1%} "
            f"| {share.get('none', 0):.1%} | {share.get('material', 0):.1%} "
            f"| {share.get('minor', 0):.1%} |"
        )
    return lines


def _paired_rows(per_org: pd.DataFrame, metric: str, fmt: str) -> list[str]:
    """One table row per candidate: mean per-org difference from the baseline, with its CI."""
    rows = []
    for run in [c for c in per_org.columns if c != BASELINE_RUN]:
        pair = per_org[[BASELINE_RUN, run]].dropna()
        diffs = (pair[run] - pair[BASELINE_RUN]).to_numpy(dtype=float)
        lo, hi = paired_ci(diffs)
        rows.append(
            f"| {run} | {metric} | {len(pair)} | {diffs.mean():+{fmt}} "
            f"| [{lo:+{fmt}}, {hi:+{fmt}}] |"
        )
    return rows


def _paired_section(precision: pd.DataFrame, recall: pd.DataFrame) -> list[str]:
    """Candidate minus baseline, per org on the orgs both have, with 95% bootstrap CIs."""
    if not len(precision) and not len(recall):
        return []
    lines = [
        "## Paired differences from the baseline (candidate − baseline, per org, 95% CI)",
        "",
        "| run | metric | orgs | mean difference | 95% CI |",
        "|---|---|---|---|---|",
    ]
    if len(precision):
        judged = precision[~precision["outcome"].isin(UNJUDGED)].assign(
            error=lambda d: d["outcome"].isin(ERROR_OUTCOMES),
            merge=lambda d: d["should_merge"].astype(bool),
        )
        per_org = judged.groupby(["run", "org_id"])[["error", "merge"]].mean()
        lines += _paired_rows(per_org["error"].unstack(0), "error share", ".3f")
        lines += _paired_rows(per_org["merge"].unstack(0), "should_merge share", ".3f")
    if len(recall):
        missed = recall[recall["judged"]].pivot(
            index="org_id", columns="run", values="missed_line_of_work"
        )
        lines += _paired_rows(missed, "missed line_of_work per org", ".2f")
    lines.append("")
    return lines


# --- Driver ---


def estimate_tokens(pairs: list[tuple[str, str]], prompt_str: str) -> int:
    """Rough input tokens: about 4 characters per token, prompt included per call."""
    return sum(len(body) + len(prompt_str) for _, body in pairs) // 4


def typical_output_tokens(session, prompt_id: str, model: str) -> float:
    """Median output tokens (reasoning included) of a prompt's stored calls; 0 when none."""
    values = (
        session.execute(
            text(
                "select ((response_full #>> '{}')::jsonb -> 'usage' ->> 'output_tokens')::float "
                "from prompt_response where prompt_id = :p and model_name = :m "
                "and num_errors is null limit 500"
            ),
            {"p": prompt_id, "m": model},
        )
        .scalars()
        .all()
    )
    values = [v for v in values if v is not None]
    return float(np.median(values)) if values else 0.0


def estimate_cost(judge: Judge, input_tokens: int, n_calls: int, output_per_call: float) -> float:
    """List-price USD for the calls still to make."""
    price_in, price_out = JUDGE_PRICES[judge.model]
    return input_tokens * price_in + n_calls * output_per_call * price_out


def run(args: argparse.Namespace) -> None:
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    unknown = set(args.routes) - set(probe.ROUTES)
    if unknown:
        raise SystemExit(f"Unknown routes {sorted(unknown)}")
    with get_session() as session:
        if args.org_file:
            orgs = probe.load_orgs(session, probe.sample_org_ids(args.org_file))
        else:
            # load_cohort caps at the cohort size, so this takes all 153 organizations.
            orgs = probe.load_cohort(session, n_orgs=10_000)
        if args.org_ids:
            missing = set(args.org_ids) - {o.org_id for o in orgs}
            if missing:
                raise SystemExit(f"Not in the cohort: {sorted(missing)}")
            orgs = [o for o in orgs if o.org_id in set(args.org_ids)]
        logger.info("Cohort: %d orgs", len(orgs))
        runs = load_runs(session, orgs, args.routes)
        baseline = runs[0]
        descriptions = load_descriptions(session, [o.org_id for o in orgs])
        if args.org_file:
            # Organizations outside the cohort have no original precision input;
            # their tracker record holds the same kind of description.
            descriptions = {**tracker_descriptions(session, args.org_file), **descriptions}
        logger.info("Descriptions on record found for %d of %d orgs", len(descriptions), len(orgs))
        frames = {"precision": [], "recall": [], "alignment": []}
        total_cost = 0.0
        for name in args.judges:
            judge = JUDGE_SPECS[name]
            cache = make_judge_cache(session, judge)
            output_per_call = typical_output_tokens(session, cache.prompt.id, judge.model)
            if not output_per_call and name in ("precision", "recall"):
                # No verdicts of this version stored yet: the v1 judge's outputs, which
                # the new versions' extra fields exceed.
                v1 = PRECISION_V1 if name == "precision" else RECALL_V1
                output_per_call = typical_output_tokens(session, v1.prompt_id, SOL)
            for r in runs:
                pairs = judge_inputs(name, r, orgs, descriptions, baseline)
                if not pairs:
                    continue
                ids = [g for g, _ in pairs]
                before = stored_judge_rows(session, cache, judge, ids)
                done = sum(1 for g in ids if g in before and not before[g].num_errors)
                todo_pairs = [p for p in pairs if p[0] not in before or before[p[0]].num_errors]
                tokens = estimate_tokens(todo_pairs, cache.prompt.prompt_str)
                cost = estimate_cost(judge, tokens, len(todo_pairs), output_per_call)
                total_cost += cost
                print(
                    f"  {name:10s} {r.key:30s} {done}/{len(ids)} stored, "
                    f"{len(todo_pairs)} to call, ~{tokens:,} input tokens, ~${cost:.2f}"
                )
                if args.dry_run:
                    continue
                todo = len(todo_pairs)
                if todo:
                    cache.bulk_get_cache_or_run(
                        pairs,
                        read_from_cache=True,
                        write_to_cache=True,
                        max_workers=args.workers,
                        max_errors=args.max_errors,
                        n_per_commit=20,
                        **judge.kwargs(),
                    )
                rows = stored_judge_rows(session, cache, judge, ids)
                verdicts = {
                    g.split("|", 1)[1]: parse_verdict(rows.get(g), judge.response_model)
                    for g in ids
                }
                if name == "precision":
                    frames["precision"] += precision_rows(r, verdicts)
                elif name == "recall":
                    frames["recall"] += recall_rows(r, verdicts)
                else:
                    for org_id, alignment in verdicts.items():
                        if alignment is None:
                            continue
                        n_a = len(baseline.profiles[org_id].activities)
                        n_b = len(r.profiles[org_id].activities)
                        frames["alignment"].append(
                            {"run": r.key, "org_id": org_id, **score_alignment(n_a, n_b, alignment)}
                        )
        floor = noise_floor(session) if "alignment" in args.judges else pd.DataFrame()
        if len(floor):
            floor = floor[floor["org_id"].isin({o.org_id for o in orgs})]
        print(f"  estimated cost of calls still to make: ~${total_cost:.2f} at list price")
        if args.dry_run:
            session.rollback()
            return
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = {name: pd.DataFrame(rows) for name, rows in frames.items()}
    for name, frame in {**tables, "noise_floor": floor}.items():
        if len(frame):
            frame.to_csv(out_dir / f"{name}.csv", index=False)
    text_report = report(tables["precision"], tables["recall"], tables["alignment"], floor)
    (out_dir / "judged_comparison.md").write_text(text_report + "\n")
    print(text_report)
    print(f"\nWrote {out_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--judges",
        nargs="+",
        default=list(JUDGES),
        choices=JUDGES,
        help="Judges to run (default: all)",
    )
    parser.add_argument(
        "--routes",
        nargs="+",
        default=list(CANDIDATE_ROUTES),
        help="Candidate routes to judge alongside the baseline",
    )
    parser.add_argument(
        "--org-ids",
        nargs="+",
        default=None,
        help="Judge only these cohort organizations (default: all 153)",
    )
    parser.add_argument(
        "--org-file",
        default=None,
        help="Judge the organizations in this sample file instead of the cohort",
    )
    parser.add_argument("--workers", type=int, default=8, help="Parallel judge calls (default: 8)")
    parser.add_argument(
        "--max-errors",
        type=int,
        default=2,
        help="Attempts per judge call across reruns (default: 2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count stored and needed calls with token estimates; call nothing",
    )
    parser.add_argument(
        "--out-dir",
        default="data_out/activity_extraction_judges",
        help="Report directory (default: data_out/activity_extraction_judges)",
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
