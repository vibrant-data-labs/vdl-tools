"""ID Mapping File schema — single source of truth for columns and enums.

Every downstream artifact joins through the ID Mapping File, so its shape is
defined once here and validated wherever the file is written.
"""

ENTITY_TYPES = {"for_profit", "nonprofit", "unknown"}
DISPOSITIONS = {"invested", "passed"}
SOURCES = {"crunchbase", "nzi"}

MATCH_METHODS = {
    "url_exact",
    "name_exact",
    "name_fuzzy",
    "api_search",
    "web_research",
    "customer_provided",
    "manual",
}

STATUSES = {
    "auto_matched",
    "needs_review",
    "vdl_reviewed",
    "customer_review",
    "unmatched_final",
}
TERMINAL_STATUSES = {"auto_matched", "vdl_reviewed", "unmatched_final"}

OUT_OF_UNIVERSE_REASONS = {
    "not_climate",
    "not_us",
    "not_venture",
    "excluded_by_landscape_filter",
    "coverage_gap",
}

ID_MAPPING_COLUMNS = [
    "customer_row_id",
    "customer_name",
    "customer_url",
    "customer_ein",
    "entity_type",
    "disposition",
    "in_universe",
    "matched_id",
    "matched_source",
    "matched_name",
    "matched_url",
    "match_method",
    "confidence",
    "status",
    "out_of_universe_reason",
    "decided_by",
    "decided_at",
    "notes",
    # Text-sufficiency accounting (drives readiness in text-objective
    # engagements): which text sources a row has, whether it can proceed to
    # enrichment, and any description the customer supplied directly.
    "text_sources",
    "enrichment_ready",
    "customer_description",
    # LinkedIn identity found beyond the customer URL (e.g. Coresignal
    # last-resort search) — queryable text source of last resort.
    "linkedin_url",
    # Supplementary source identities: each source's id for this org
    # regardless of which source won the primary match (text objective
    # mixes sources; domain-confirmed only; never replaces matched_id).
    "nzi_id",
    "cb_id",
]

_ENUM_COLUMNS = {
    "entity_type": ENTITY_TYPES,
    "disposition": DISPOSITIONS,
    "match_method": MATCH_METHODS,
    "status": STATUSES,
    "out_of_universe_reason": OUT_OF_UNIVERSE_REASONS,
}


def validate_id_mapping(df):
    """Raise ValueError describing every schema violation in ``df`` at once."""
    problems = []
    missing = [c for c in ID_MAPPING_COLUMNS if c not in df.columns]
    if missing:
        problems.append(f"missing columns: {missing}")
    for col, allowed in _ENUM_COLUMNS.items():
        if col not in df.columns:
            continue
        bad = set(df[col].dropna().unique()) - allowed
        if bad:
            problems.append(f"invalid {col} values: {sorted(bad)}")
    if problems:
        raise ValueError("ID Mapping File schema violations: " + "; ".join(problems))
    return df
