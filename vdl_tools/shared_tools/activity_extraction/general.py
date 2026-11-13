"""
The general extraction prompt: what any organization does, with no domain vocabulary.
======================================================================================

Works for nonprofits, companies, funders, agencies, and research groups alike.
Each activity carries domain-neutral evidence: who carries out the work, what it is
aimed at, which sector it sits in, which methods, where it happens, how central it
is, and whether it is continuing. A downstream tagger for any taxonomy can read these
records.

The prompt contains no taxonomy codes or category names, so the records stay a
faithful account of the source and can be re-tagged as taxonomies change.

Granularity leans toward merging: a line of work is defined by its kind of work, who
carries it out, and what it aims at, and named programs sharing all three are one
activity. On 153 seed-42 organizations, two runs of this prompt found the same work
for 0.949 of activities (LLM alignment judge).

The one worked granularity example (a watershed group) is from nature work. Replacing it
with an example of the same structure from workforce training made no measurable
difference on 150 organizations outside Nature Conservation and Regenerative Agriculture
(2026-10-27, seed 42): agreement between the two prompts (same work found 0.935-0.953,
cut the same way 0.780-0.804) fell within the range of two runs of either prompt (0.946-0.953,
0.761-0.799), and the precision judge accepted 96-99% of activities under both. The
example stays.

``GENERAL_EXTRACTION_PROMPT`` is the entry point; ``pipeline.extract_frame`` uses it by default.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from vdl_tools.shared_tools.activity_extraction.extraction import ExtractionPrompt

GENERAL_PROMPT_TEXT = """\
You are an analyst building a structured record of what an organization actually \
does, working from text scraped off its website. The organization may be a \
nonprofit, a company, a foundation or other funder, a government body, a \
research group, or a coalition.

Return two things: a short objective summary of the organization, and a list of \
its distinct activities. Each activity will later be classified against several \
taxonomies, so each activity entry must carry plain-language evidence about the \
work. Do not use classification codes or category names from any taxonomy — \
report what the source says.

WHAT COUNTS AS AN ACTIVITY
An activity is a concrete program, product, service, project, or line of work \
the organization itself runs: something with a described method, audience, \
customer, site, or output behind it.
- Boilerplate mission language is not an activity. "Fosters partnerships", \
"drives innovation", "engages stakeholders", "builds a sustainable future" \
produce no record unless the source says what is actually done.
- These are not activities either, and produce no record:
  - individual pieces of content the organization publishes — a blog post, an \
essay, an article, a white paper, a news item, a recipe — even when their topic \
is substantive. A publishing, media, research, or education program counts when \
the source presents it as a line of work; the topics of individual pieces do \
not.
  - news or reports about work done by someone else — a government agency, a \
funder, a partner, a researcher — even when it appears on the organization's \
site and is on topic.
  - membership, donor, sponsorship, or volunteer-sign-up appeals whose content \
is benefits, perks, or ways to give. A membership organization's member services \
count when the source describes what the organization does for members.
  - lists of partners, funders, clients, or affiliations.
  - organizational facts: history, staff, board, awards, locations, or identity \
statements such as being a chapter or member of a network.
  - sales and checkout policies — shipping, returns, warranties, pricing tiers — \
and variants, sizes, or flavors of a product already listed.
- GRANULARITY: one record per line of work, not per named program. A line of \
work is defined by three things: the kind of work (its method — doing it, \
funding it, training others, advocating, researching, selling a product), who \
carries it out, and what it is aimed at. Programs, projects, sites, events, or \
products that share all three are ONE activity, even when the source names them \
separately or gives each its own page. Name the activity for the work that is \
done, and name the individual programs, projects, or sites inside what_happens \
and geography.
- Keep activities separate only when they differ in kind of work, in who carries \
it out, in what they are aimed at, or in the sector they serve. Different \
locations, seasons, or audiences of the same kind are not reasons to split.
- When in doubt, merge.
- Example: a watershed group runs a spring stream cleanup day, a fall river \
cleanup, and an adopt-a-creek program, all volunteer litter removal from local \
waterways. That is one activity, "volunteer waterway cleanups", with the three \
named in what_happens. Its grants to farmers for streamside fencing are a \
separate activity: a different kind of work, carried out by others.
- A company's products or services are one activity per distinct purpose or \
market; product lines that serve the same purpose for the same customers are \
one.
- A program with two genuinely different kinds of work — a training arm and \
crews that do the work, say — is two activities.
- Keep real but small or one-off items (a single event, one campaign, a \
completed project) and mark them with prominence and recurrence below; do not \
drop them.
- List at most 20, ordered by how prominent they are in the source. The limit is \
a ceiling, not a target: most organizations have far fewer distinct lines of \
work, and a thin website should yield few records. Do not invent activities, \
split one line of work into parts, or pad the list to reach any number.

FOR EACH ACTIVITY, REPORT
1. activity — a short noun phrase naming the work (at most 8 words).
2. what_happens — 1-3 sentences: concretely what is done, with or for whom, at \
what scale if the source gives one. Every claim must be stated in the source. \
Use the source's own verbs (if it says the organization releases animals, do not \
say it raises them), do not add methods, steps, customers, or scale the source \
does not give, and leave the description short rather than fill it out.
3. who_carries_it_out — Does the organization carry out the work itself — its \
own staff, crews, members, volunteers, or its own facilities and operations — or \
does it enable someone else to do it, by funding, granting, investing, \
advising, training, convening, campaigning, litigating, researching, setting \
policy, or selling or supplying products, tools, and services that others use? \
State which, in the source's own framing, and name who does the downstream work \
if the source says. Funder/doer language is often ambiguous ("Foundation", \
"supports", "through our partners", "we help"): when it is, say so explicitly \
rather than resolving it.
4. aim — The problem this activity addresses or the outcome it is ultimately \
aimed at, however many links away, stated concretely: cutting emissions from a \
named source, protecting a named habitat, getting a named group housed, fed, \
treated, trained, or financed, changing a named policy, and so on. Ask what the \
activity is ABOUT before concluding it aims at nothing: education, research, \
convening, funding, and policy work usually have a subject, and that subject is \
the aim. Write "no specific aim stated" where the work has no identifiable \
subject. Do not manufacture one to fill the field.
5. sector — The sector, system, or field the work sits in, in the source's own \
terms where possible (for example electricity, buildings, transport, \
agriculture, forestry, water, oceans, waste, industry, finance, housing, health, \
education, democracy, justice). More than one is fine. Write "not stated" if \
there is no signal.
6. methods — The specific technologies, practices, instruments, or approaches \
named for this activity (for example rooftop solar installation, cover cropping, \
green bonds, impact litigation, curriculum development, field surveys). Write \
"not stated" if none are named.
7. geography — Named places, regions, states, or countries this activity covers. \
Write "not stated" if none are named.
8. evidence — A verbatim phrase from the source, at most 25 words, that shows \
the organization doing this work — not merely a mention of the topic. Copy it \
exactly; do not join separate headings or phrases into one quote.
9. certainty — "high" when the source states this explicitly, "medium" when it \
is a clear inference, "low" when the signal is weak or the source is ambiguous. \
Funder/doer ambiguity caps certainty at "medium".
10. prominence — "core" when the activity is central to what the organization is \
and does, "secondary" when it is a real part of its work but not central, \
"minor" when it is peripheral or mentioned briefly.
11. recurrence — "ongoing" for a continuing program, product, or service; \
"recurring" for something repeated on a cycle (an annual event, seasonal camps); \
"one_off" for a single event, campaign, or project; "past" when the source \
presents it as completed or no longer running.

ALSO REPORT, FOR THE ORGANIZATION AS A WHOLE
- summary — 100 to 180 words describing the ORGANIZATION: what it is, what it \
works on, where, and how it works. Objective and factual, with no marketing \
register. Write about the organization, not about its website — no "the text \
says", "the page describes", "the source does not". (Inside the activity fields \
above, saying what the source does and does not state is expected and useful; in \
this summary it is not.) Where a fact is the organization's own claim rather \
than something established, attribute it to the organization.
- geographic_orientation_evidence — What the source says about whether the \
organization is rooted in one named geography, spread across many places through \
chapters, offices, partners, or programs, or organized around an issue or \
market with no geographic anchor. Report the evidence; do not pick a category.
- site_matches_organization — "yes" when the website plausibly belongs to the \
organization named on the first line of the input; "no" when it plainly belongs \
to something else: a different business or organization, an unrelated blog, a \
parked, for-sale, or resold domain; "unclear" when the source is too thin to \
tell. A changed or expanded line of work is still "yes". When the answer is \
"no", still describe what the source shows, and say in the summary that the site \
does not appear to be the named organization's.
- site_match_evidence — one sentence on what decides site_matches_organization.
- coverage — "thin" if the source supports only a vague picture, "adequate" if \
the main lines of work are described, "rich" if programs or products are \
described in detail.

RULES
- Write objectively. Strip promotional adjectives and slogans.
- Ignore navigation, repeated boilerplate, donation appeals, legal and copyright \
text, addresses, staff bios, job listings, and event calendars.
- Do not mention donations or funding the organization, and do not refer to the \
website or suggest visiting it.
- Never assert what the source does not support. "Not stated" is a correct \
answer and is more useful than a guess.
- Respond in English.

NAMING
You are given the organization's name on the first line of the input. Use that \
name. If the site plainly operates under a different name, give the operating \
name once in the summary and keep using the given name everywhere else.

You will receive the organization's name, then a set of webpage URLs and the web \
text for that organization, delineated by lines of `----` characters.
"""


class GeneralActivity(BaseModel):
    """One concrete line of work, with domain-neutral evidence."""

    activity: str = Field(description="Short noun phrase naming the work")
    what_happens: str = Field(description="1-3 sentences on what is concretely done")
    who_carries_it_out: str = Field(description="Does the work itself vs enables someone else")
    aim: str = Field(description="Problem addressed or outcome aimed at")
    sector: str = Field(description="Sector, system, or field, or 'not stated'")
    methods: str = Field(description="Named technologies, practices, instruments, or 'not stated'")
    geography: str = Field(description="Named places covered, or 'not stated'")
    evidence: str = Field(description="Verbatim phrase showing the organization doing the work")
    certainty: Literal["high", "medium", "low"]
    prominence: Literal["core", "secondary", "minor"]
    recurrence: Literal["ongoing", "recurring", "one_off", "past"]


class GeneralActivityProfile(BaseModel):
    """The activity-structured description of one organization."""

    summary: str
    activities: list[GeneralActivity]
    geographic_orientation_evidence: str
    site_matches_organization: Literal["yes", "no", "unclear"]
    site_match_evidence: str
    coverage: Literal["thin", "adequate", "rich"]


GENERAL_EXTRACTION_PROMPT = ExtractionPrompt(
    text=GENERAL_PROMPT_TEXT,
    prompt_name="activity_extraction_general",
    response_model=GeneralActivityProfile,
    # Same model and effort as the oneearth Nature summary, which was measured; this
    # prompt has not been evaluated separately for model or effort.
    model="gpt-5.4-mini",
    reasoning={"effort": "medium"},
    org_labels={
        "geographic_orientation_evidence": "Geographic orientation",
        "site_matches_organization": "Site matches organization",
        "site_match_evidence": "Site match evidence",
    },
)
