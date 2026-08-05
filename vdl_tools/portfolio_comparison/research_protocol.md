# Entity-Resolution Research Protocol

Rules for any human or agent doing web research on review-queue rows.
Spawned research agents MUST receive this protocol verbatim in their prompt.
Born from real misses in the one-small-planet pilot (Loa Carbon/Aether
Diamonds false rebrand; Global Solutions Tracker unverified inference).

## Asymmetric rigor

A wrong match poisons every downstream stage; a missed match costs one
Tier-2 lookup. Therefore:

- **reject_all** for obviously-unrelated fuzzy noise may stay quick: different
  domain AND different business is sufficient, but you must ALSO check the
  baseline for the org under alternate identities before concluding absence.
- **Any positive claim** (accept_candidate, baseline_match_found) gets the
  full protocol below. When in doubt, return `unsure` with the conflict
  spelled out — never round up to a match.

## The domain-anchor test (required for every positive claim)

A baseline match is valid only if the baseline record's CURRENT identity
anchors — its domain and description — still point at the customer's org.

- Old domain 301-redirects to the customer's current domain → continuous
  identity, strong evidence (verify the redirect yourself: `curl -sIL`).
- Old domain is dead, parked, or re-registered by an unrelated firm → the
  domain proves nothing about historical ownership (Balance Ocean lesson:
  dropped domains get re-registered).
- Old brand was SOLD and lives on elsewhere → matching enriches the wrong
  company even if the legal entity is continuous. Reject.

## "Former name" claims are leads, not evidence

Data vendors (PitchBook, Crunchbase, Clodura, aggregator SEO pages) conflate
three cases: (a) rebrand with continuous identity, (b) founder's successor
venture, (c) pivot where the old brand detached. Distinguish them:

- Read the founder's own bio: "currently X… previously founded Y" signals
  successor ventures (Loa Carbon lesson).
- Look for evidence the two orgs COEXIST: both with live sites, separate
  funding events, or separate filings after the alleged rename → refuted.
- One vendor page repeating another vendor's field is one source, not two.

## Verification standard

- Two INDEPENDENT sources for any positive claim — the org's own site,
  regulatory/IRS filings, funder portfolio pages, or news coverage naming
  both identities. Aggregators corroborate; they do not establish.
- Fetch and read the pages you cite. A page you did not open is not a
  source; verify it actually says what you claim (Global Solutions Tracker
  lesson: the cited page never named the initiative).
- Adversarial pass before submitting any positive verdict: spend one search
  actively trying to REFUTE it. Only claims that survive go out.
- Every claim in your note must be attributable to a listed source.

## Verdict semantics

- `accept_candidate` / `baseline_match_found`: survived the domain-anchor
  test, two independent sources, and the adversarial pass.
- `unsure`: sources disagree, or the only support is a vendor field. Say
  exactly what conflicts. Unsure is a respectable answer; a confident
  wrong answer is the only failure mode.
- `reject_all`: candidates refuted AND absence from the baseline checked
  under alternate names, former names, and domains.
