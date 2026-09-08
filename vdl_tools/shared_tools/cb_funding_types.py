"""
Crunchbase funding-type vocabulary: the ONE place that owns round-type slugs,
their display names, and the named round groups.

Two vocabularies exist for the same thing and used to be mixed up downstream:

  * raw slugs   - what the Crunchbase API returns in ``investment_type``
                  (``series_a``, ``seed``, ``grant``, ...) and the stage labels
                  ``complete_stage_from_type`` produces (``early_stage_venture``,
                  ``grant_only``, ...)
  * display     - what the CFT shows people (``Series A``, ``Seed``, ``Grant``,
                  ``Venture (Unknown Stage)``, ...)

The pipeline assigns display names ONCE, in ``prepare_crunchbase`` (Crunchbase
rows) and ``combine_cb_cd`` (rows from other sources), and keeps the raw values
next to them in sibling ``... Raw`` columns. Everything that classifies an org
(``cb_funding_calculations``) works on raw slugs and calls ``as_raw`` so it
also tolerates display names. Do not re-map funding types anywhere else.

This replaces the former ``shared-data-clean/data/keywords/funding_types_mapping.xlsx``
(its ``api_funding_type`` -> ``Cleaned_Funding_Type`` / ``Funding_Stage_Category_2``
/ ``Grant_Venture`` / ``Funding_Stage_Ordinal`` columns are the tuples below) and
the dead ``fundingDict`` / ``stageDict`` in ``common_dicts``.
"""

# ---------------------------------------------------------------------------
# Column names the pipeline writes (import these rather than retyping strings)
# ---------------------------------------------------------------------------
FUNDING_TYPES_COL = 'Funding Types'            # display names, chronological, duplicates kept
FUNDING_TYPES_RAW_COL = 'Funding Types Raw'    # raw slugs, same order (aligned with Funding Types Dates)
FUNDING_TYPES_DATES_COL = 'Funding Types Dates'
FUNDING_STAGE_COL = 'Funding Stage'            # display name of the org's overall stage
FUNDING_STAGE_RAW_COL = 'Funding Stage Raw'    # stage label as computed (see complete_stage_from_type)
LAST_FUNDING_TYPE_COL = 'Last_Funding_Type'    # display name of the most recent round


# ---------------------------------------------------------------------------
# Round types: one row per raw Crunchbase ``investment_type`` slug
#   slug: (display name, stage group, capital type, ordinal)
# stage group  = the coarse stage bucket used for stage charts
# capital type = Philanthropy / Venture / Post Venture / Other
# ordinal      = rough chronological order of a company's life (None = not staged)
# ---------------------------------------------------------------------------
ROUND_TYPES = {
    'grant':                 ('Grant',                   'Grant',         'Philanthropy', 0),
    'pre_seed':              ('Pre-Seed',                'Seed',          'Venture',      1),
    'convertible_note':      ('Convertible Note',        'Seed',          'Venture',      1),
    'seed':                  ('Seed',                    'Seed',          'Venture',      2),
    'angel':                 ('Angel',                   'Seed',          'Venture',      2),
    'initial_coin_offering': ('ICO',                     'Early Venture', 'Venture',      2),
    'equity_crowdfunding':   ('Equity Crowdfunding',     'Seed',          'Venture',      2),
    'product_crowdfunding':  ('Product Crowdfunding',    'Seed',          'Venture',      2),
    'series_a':              ('Series A',                'Early Venture', 'Venture',      3),
    'series_b':              ('Series B',                'Early Venture', 'Venture',      4),
    # a venture round whose stage was not disclosed (~13% of rounds); two slugs, one display name
    'series_unknown':        ('Venture (Unknown Stage)', 'Early Venture', 'Venture',      5),
    'undisclosed':           ('Venture (Unknown Stage)', 'Early Venture', 'Venture',      5),
    'series_c':              ('Series C',                'Late Venture',  'Venture',      6),
    'series_d':              ('Series D',                'Late Venture',  'Venture',      6),
    'series_e':              ('Series E',                'Late Venture',  'Venture',      6),
    'series_f':              ('Series F',                'Late Venture',  'Venture',      6),
    'series_g':              ('Series G',                'Late Venture',  'Venture',      6),
    'series_h':              ('Series H',                'Late Venture',  'Venture',      6),
    'series_i':              ('Series I',                'Late Venture',  'Venture',      6),
    'series_j':              ('Series J',                'Late Venture',  'Venture',      6),
    'private_equity':        ('Private Equity',          'Post Venture',  'Post Venture', 8),
    'post_ipo_debt':         ('Post-IPO Debt',           'Post Venture',  'Post Venture', 10),
    'post_ipo_secondary':    ('Post-IPO Secondary',      'Post Venture',  'Post Venture', 10),
    'post_ipo_equity':       ('Post-IPO Equity',         'Post Venture',  'Post Venture', 10),
    'secondary_market':      ('Secondary Market',        'Post Venture',  'Post Venture', 10),
    'corporate_round':       ('Corporate Round',         'Post Venture',  'Other',        None),
    'debt_financing':        ('Debt',                    'Debt',          'Other',        None),
    'non_equity_assistance': ('Non-Equity',              'Non-Equity',    'Other',        None),
}

# Stage labels that are NOT round types: values of the Crunchbase ``funding_stage``
# field and the labels ``complete_stage_from_type`` returns. Display names for
# these are kept exactly as the old xlsx had them, including two oddities worth
# knowing: a grant-only or debt-only company displays as "Non-Equity".
STAGE_LABELS = {
    'early_stage_venture':   'Early Venture',
    'late_stage_venture':    'Late Venture',
    'unknown_venture_stage': 'Venture (Unknown Stage)',
    'ipo':                   'IPO',
    'm_and_a':               'M&A',
    'grant_only':            'Non-Equity',
    'debt_only':             'Non-Equity',
    'unknown':               'Non-Equity',
    'Philanthropy':          'Philanthropy',   # non-profits (Candid, and CB non_profit)
}

# slug -> display, for round types and stage labels alike
SLUG_TO_DISPLAY = {slug: row[0] for slug, row in ROUND_TYPES.items()} | STAGE_LABELS
SLUG_TO_STAGE_GROUP = {slug: row[1] for slug, row in ROUND_TYPES.items()}
SLUG_TO_CAPITAL_TYPE = {slug: row[2] for slug, row in ROUND_TYPES.items()}
SLUG_TO_ORDINAL = {slug: row[3] for slug, row in ROUND_TYPES.items()}

# display -> slug. Where several slugs share a display name the FIRST one listed
# above wins (e.g. "Venture (Unknown Stage)" -> series_unknown, "Non-Equity" ->
# non_equity_assistance), so display -> raw -> display round-trips but raw ->
# display -> raw may not. Keep the raw column when you need the exact slug.
DISPLAY_TO_SLUG = {}
for _slug, _display in SLUG_TO_DISPLAY.items():
    DISPLAY_TO_SLUG.setdefault(_display, _slug)

RAW_SLUGS = set(SLUG_TO_DISPLAY)
DISPLAY_NAMES = set(DISPLAY_TO_SLUG)

# display name -> stage group, for scripts that hold display names (the old
# xlsx's Cleaned_Funding_Type -> Funding_Stage_Category_2 lookup)
DISPLAY_TO_STAGE_GROUP = {row[0]: row[1] for row in ROUND_TYPES.values()} | {
    'Early Venture': 'Early Venture',
    'Late Venture': 'Late Venture',
    'M&A': 'Post Venture',
    'IPO': 'Post Venture',
}


# ---------------------------------------------------------------------------
# Named round groups (raw slugs). Used by cb_funding_calculations and
# venture_backed_flag; re-exported from cb_funding_calculations for old imports.
# ---------------------------------------------------------------------------
PRE_SEED_STAGES = {'angel', 'pre_seed', 'convertible_note', 'product_crowdfunding'}
SEED_STAGES = {'equity_crowdfunding', 'initial_coin_offering', 'seed'}
OTHER = {'grant', 'debt_financing', 'non_equity_assistance'}
IPO_STATES = {'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary'}
POST_IPO_TYPES = ['ipo', 'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary']

DISCLOSED_STAGES_ORDERED = [
    'grant', 'equity_crowdfunding', 'initial_coin_offering', 'angel', 'pre_seed', 'seed',
    'series_a', 'series_b', 'series_c', 'series_d', 'series_e', 'series_f',
    'series_g', 'series_h', 'series_i', 'series_j', 'corporate_round', 'secondary_market',
    'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary',
]
EARLY_VENTURE_ROUNDS = {'series_a', 'series_b'}
LATE_VENTURE_ROUNDS = set(DISCLOSED_STAGES_ORDERED[
    DISCLOSED_STAGES_ORDERED.index('series_c'):DISCLOSED_STAGES_ORDERED.index('post_ipo_equity')])
# series_unknown counts as a venture round - it's a venture round whose stage wasn't disclosed
VENTURE_ROUNDS = LATE_VENTURE_ROUNDS | EARLY_VENTURE_ROUNDS | {'seed', 'series_unknown'}
# Round types that make an org "venture-backed" on their own (see
# climate_landscape/venture_backed_flag.py). Grants are handled separately there:
# grant-only + for-profit counts as venture-backed; a grant next to any other
# round type does not, and grant + non-profit does not.
VENTURE_BACKED_ROUNDS = VENTURE_ROUNDS | PRE_SEED_STAGES
POST_IPO = set(DISCLOSED_STAGES_ORDERED[DISCLOSED_STAGES_ORDERED.index('post_ipo_equity'):])
UNDISCLOSED_STAGES = {'undisclosed', 'series_unknown'}

# round slug -> internal stage label (used for round-level stage charts)
ROUND_TO_STAGE = {}
for _stage in PRE_SEED_STAGES:
    ROUND_TO_STAGE[_stage] = 'pre_seed'
for _stage in SEED_STAGES:
    ROUND_TO_STAGE[_stage] = 'seed'
for _stage in EARLY_VENTURE_ROUNDS:
    ROUND_TO_STAGE[_stage] = 'early_stage_venture'
for _stage in LATE_VENTURE_ROUNDS:
    ROUND_TO_STAGE[_stage] = 'late_stage_venture'
for _stage in IPO_STATES:
    ROUND_TO_STAGE[_stage] = 'ipo'
for _stage in OTHER:
    ROUND_TO_STAGE[_stage] = _stage
ROUND_TO_STAGE['private_equity'] = 'private_equity'
# venture round with undisclosed stage - ~13% of crunchbase rounds, too common to leave unmapped
ROUND_TO_STAGE['series_unknown'] = 'unknown_venture_stage'


# ---------------------------------------------------------------------------
# Converters
# ---------------------------------------------------------------------------

def _is_missing(value):
    """True for None / NaN / empty string."""
    return value is None or value == '' or value != value


def to_display(value):
    """Raw slug(s) -> display name(s). Lists map element-wise and keep order and
    duplicates; unknown values pass through unchanged (e.g. 'Philanthropy')."""
    if isinstance(value, (list, tuple)):
        return [SLUG_TO_DISPLAY.get(v, v) for v in value]
    if _is_missing(value):
        return value
    return SLUG_TO_DISPLAY.get(value, value)


def as_raw(value):
    """Return raw slug(s) whether given slugs or display names.

    Lists map element-wise keeping order and duplicates. Raises ValueError on a
    value that is neither a known slug nor a known display name, so a typo or
    a new Crunchbase round type fails loudly instead of silently not matching.
    """
    if isinstance(value, (list, tuple)):
        return [as_raw(v) for v in value]
    if _is_missing(value):
        return value
    value = str(value).strip()
    if value in RAW_SLUGS:
        return value
    if value in DISPLAY_TO_SLUG:
        return DISPLAY_TO_SLUG[value]
    raise ValueError(
        f"{value!r} is not a known Crunchbase funding type (slug or display name); "
        "add it to cb_funding_types.ROUND_TYPES / STAGE_LABELS"
    )


def unknown_slugs(values):
    """The set of values that are not known raw slugs (for a run-time warning on
    a rounds table). Missing values are ignored."""
    return {v for v in set(values) if not _is_missing(v) and v not in RAW_SLUGS}
