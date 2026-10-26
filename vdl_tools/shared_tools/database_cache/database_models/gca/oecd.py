"""OECD Private Philanthropy for Development — source, project and org tables.

Source dataflow: ``OECD.DCD.FSD:DSD_PPFD@DF_PPFD(1.0)`` (SDMX). The CSV the
pipeline currently consumes is an export of that dataflow, so these tables are
shaped to accept either the export or a direct SDMX pull without change.

Grain, from most to least raw:

    OECDObservation   one SDMX observation (donor x recipient x sector x year)
    OECDProject       one funded project, consensus name + concatenated text
    OECDOrganization  one resolved organization
    OECDOrgProject    which projects belong to which organization

Modelling the observation grain matters because funding is only correct at that
level: ``OBS_VALUE`` is reported per observation, and the amount in USD depends
on that row's own ``UNIT_MULT``. Aggregating to org in pandas and writing only
the total (what the pipeline does today) makes it impossible to answer "which
years / sectors / donors does this total come from?" without re-running
everything from the 233MB intermediate.
"""

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy_utils import generic_repr
from sqlalchemy.dialects.postgresql import JSONB

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_md5


# The SDMX dimensions that together identify one observation. Sorted and
# hashed to form the primary key, so re-ingesting the same export is an upsert
# rather than a duplicate.
OBSERVATION_KEY_DIMENSIONS = (
    "OECD_ID",
    "DONOR_PROJECT_ID",
    "DONOR",
    "RECIPIENT",
    "SECTOR",
    "MEASURE",
    "FLOW_TYPE",
    "PRICE_BASE",
    "TIME_PERIOD",
)


def normalize_org_name(name: str) -> str:
    """Casefold and collapse whitespace — the basis of the organization key.

    Deliberately conservative: it does NOT strip legal suffixes or apply the
    naming dictionary. Those are *resolutions* and belong in
    OrgNameResolution, where they are auditable and reversible, rather than
    being baked irreversibly into a primary key.
    """
    return " ".join(str(name).split()).casefold()


@generic_repr
class OECDObservation(BaseMixin):
    """One SDMX observation, stored as reported.

    Only the code columns are kept as first-class fields. The export carries a
    duplicate human-label column for nearly every dimension (``Time period``,
    ``OECD ID``, ``Region``, ``Channel of delivery name``…) which is null on
    almost every row; of the 85 columns in the current intermediate, roughly
    half are these near-empty duplicates. Labels that are actually populated
    live in ``full_record`` and are resolved for display from the codes.
    """

    __tablename__ = 'oecd_observation'

    observation_id = Column(String, primary_key=True, index=True)

    # --- project linkage -------------------------------------------------
    # OECD_ID split into numeric core + suffix, matching the existing pipeline.
    oecd_id = Column(String, nullable=False, index=True)
    oecd_id_core = Column(String, nullable=False, index=True)
    oecd_id_suffix = Column(String, nullable=True)
    donor_project_id = Column(String, nullable=True, index=True)
    project_id = Column(
        String,
        ForeignKey('oecd_project.project_id', onupdate="CASCADE", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    # --- dimensions ------------------------------------------------------
    donor_code = Column(String, nullable=True, index=True)
    donor_name = Column(String, nullable=True)
    recipient_code = Column(String, nullable=True)
    recipient_name = Column(String, nullable=True)
    region = Column(String, nullable=True, index=True)
    income_group = Column(String, nullable=True)
    sector_code = Column(String, nullable=True, index=True)
    sector_name = Column(String, nullable=True)
    channel_delivery_code = Column(String, nullable=True)
    channel_delivery_name = Column(String, nullable=True, index=True)
    finance_type_code = Column(String, nullable=True)
    finance_type_name = Column(String, nullable=True)
    category_code = Column(String, nullable=True)
    category_name = Column(String, nullable=True)
    time_period = Column(Integer, nullable=True, index=True)

    # --- measure ---------------------------------------------------------
    # Kept as columns rather than assumed, because the aggregation is only
    # sound when they are constant. In the 09_2024 export flow_type is 'D'
    # (disbursements) on all 79,258 rows and price_base is 'V' (current
    # prices), but measure is mixed: 78,957 rows of 41 and 301 of 43. Summing
    # across measures blends two different instruments, so loaders should
    # filter or group on this rather than trusting uniformity.
    measure_code = Column(Integer, nullable=True, index=True)
    measure_name = Column(String, nullable=True)
    flow_type = Column(String, nullable=True, index=True)
    price_base = Column(String, nullable=True)
    obs_status = Column(String, nullable=True)

    obs_value = Column(Float, nullable=True)
    # As reported: 6 == millions. Stored rather than assumed — the pipeline
    # currently hardcodes `OBS_VALUE * 1000000`, which is right for this
    # export and silently wrong by orders of magnitude if OECD ever reports a
    # different multiplier.
    unit_mult = Column(Integer, nullable=True)
    unit_measure = Column(String, nullable=True)
    # Derived at load: obs_value * 10**unit_mult. Nominal (current) prices,
    # NOT inflation-adjusted — sums across years are nominal USD.
    amount_usd = Column(Float, nullable=True, index=True)

    # --- OECD Rio markers -------------------------------------------------
    # Policy markers assigned by OECD itself: 0 = not targeted, 1 = significant
    # objective, 2 = principal objective. The current pipeline drops these
    # entirely and infers relevance from text instead. They are worth keeping
    # as a model feature, as a validation signal against the classifier, and
    # as a filter in the explorer.
    marker_environment = Column(Integer, nullable=True, index=True)
    marker_biodiversity = Column(Integer, nullable=True, index=True)
    marker_climate_mitigation = Column(Integer, nullable=True, index=True)
    marker_climate_adaptation = Column(Integer, nullable=True, index=True)
    marker_desertification = Column(Integer, nullable=True)
    marker_gender_equality = Column(Integer, nullable=True)
    marker_governance = Column(Integer, nullable=True)

    # --- source text ------------------------------------------------------
    short_description = Column(String, nullable=True)
    project_title = Column(String, nullable=True)
    long_description = Column(String, nullable=True)

    # Everything not promoted above, verbatim, so re-deriving a dropped field
    # never requires going back to the CSV.
    full_record = Column(JSONB, nullable=True)

    def __init__(self, **kwargs):
        if 'observation_id' not in kwargs:
            kwargs['observation_id'] = self.create_observation_id(kwargs)
        super().__init__(**kwargs)

    @classmethod
    def create_observation_id(cls, record: dict) -> str:
        """Hash the SDMX dimension tuple.

        Accepts either raw export column names or this model's field names, so
        the same helper works for CSV ingestion and API ingestion.
        """
        alias = {
            "OECD_ID": ("OECD_ID", "oecd_id"),
            "DONOR_PROJECT_ID": ("DONOR_PROJECT_ID", "donor_project_id"),
            "DONOR": ("DONOR", "donor_code"),
            "RECIPIENT": ("RECIPIENT", "recipient_code"),
            "SECTOR": ("SECTOR", "sector_code"),
            "MEASURE": ("MEASURE", "measure_code"),
            "FLOW_TYPE": ("FLOW_TYPE", "flow_type"),
            "PRICE_BASE": ("PRICE_BASE", "price_base"),
            "TIME_PERIOD": ("TIME_PERIOD", "time_period"),
        }
        parts = []
        for dim in OBSERVATION_KEY_DIMENSIONS:
            value = None
            for key in alias[dim]:
                if record.get(key) is not None:
                    value = record[key]
                    break
            parts.append(f"{dim}={value}")
        return create_deterministic_md5("|".join(parts))


@generic_repr
class OECDProject(BaseMixin):
    """One funded project — the grain the relevance classifier scores."""

    __tablename__ = 'oecd_project'

    # `oecd_{oecd_id_core}_{donor_project_id}`, matching the existing
    # `id_unique_project` so the current intermediate can be backfilled
    # without re-deriving anything.
    project_id = Column(String, primary_key=True, index=True)

    oecd_id_core = Column(String, nullable=False, index=True)
    donor_project_id = Column(String, nullable=True, index=True)

    # Most frequent ORG_NAME across the project's observations. Frequently a
    # project title rather than an organization (the 09_2024 export yields
    # e.g. "Patagonia National Park" here) — which is exactly why resolution
    # is a separate, auditable step rather than an in-place overwrite.
    consensus_org_name = Column(String, nullable=True, index=True)

    # Title + long description concatenated across observations. This is the
    # text the classifier sees, so it is stored rather than recomputed: the
    # tokenizer truncates at 512 tokens, and without the exact input on record
    # a label cannot be reproduced or explained.
    description_concat = Column(String, nullable=True)
    text_id = Column(String, nullable=True, index=True)

    first_year = Column(Integer, nullable=True)
    last_year = Column(Integer, nullable=True)
    total_amount_usd = Column(Float, nullable=True, index=True)

    full_record = Column(JSONB, nullable=True)

    @classmethod
    def create_project_id(cls, oecd_id_core, donor_project_id) -> str:
        return f"oecd_{oecd_id_core}_{donor_project_id}"


@generic_repr
class OECDOrganization(BaseMixin):
    """A resolved organization.

    The key is a deterministic hash of the normalized name and nothing else.

    This is the fix for the identity bug in the current pipeline, where an
    organization's id is hashed from the *set of its project ids*: adding one
    new project changes the key, so every refresh looks like a fresh
    population and no row can be updated in place. Keying on stable content
    instead is what makes incremental refresh — and therefore "query updated
    data" — possible at all.
    """

    __tablename__ = 'oecd_organization'

    org_id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    name_normalized = Column(String, nullable=False, index=True)

    total_amount_usd = Column(Float, nullable=True, index=True)
    n_projects = Column(Integer, nullable=True)
    first_year = Column(Integer, nullable=True)
    last_year = Column(Integer, nullable=True)

    # Aggregations that vary with presentation (donor lists, sector tags,
    # region tags). JSONB rather than columns so adding a facet to the
    # explorer does not require a migration.
    aggregates = Column(JSONB, nullable=True)

    def __init__(self, **kwargs):
        if 'name_normalized' not in kwargs:
            kwargs['name_normalized'] = normalize_org_name(kwargs["name"])
        if 'org_id' not in kwargs:
            kwargs['org_id'] = self.create_org_id(kwargs["name_normalized"])
        else:
            assert kwargs['org_id'] == self.create_org_id(kwargs["name_normalized"])
        super().__init__(**kwargs)

    @classmethod
    def create_org_id(cls, name_normalized: str) -> str:
        return create_deterministic_md5(normalize_org_name(name_normalized))


@generic_repr
class OECDOrgProject(BaseMixin):
    """Which projects roll up into which organization.

    Preserves the project set that is currently hashed into the org id — the
    same information, but as rows that can be queried and corrected rather
    than as an opaque, unstable key.
    """

    __tablename__ = 'oecd_org_project'

    org_id = Column(
        String,
        ForeignKey('oecd_organization.org_id', onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    project_id = Column(
        String,
        ForeignKey('oecd_project.project_id', onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )


@generic_repr
class OrgNameResolution(BaseMixin):
    """How a raw source name became a resolved organization name.

    Replaces three things that are currently scattered and unqueryable: the
    GPT-4o name-extraction results, the hardcoded one-off corrections in
    manual_names_cleanup.py (ARA, ClientEarth, Climate Story Fund), and the
    Excel naming dictionary.

    Keyed by (raw_name_normalized, method) so a human correction and a model
    guess for the same name coexist, and the loader can prefer the human one.
    """

    __tablename__ = 'org_name_resolution'

    raw_name_normalized = Column(String, primary_key=True, index=True)
    # 'gpt-4o' | 'dictionary' | 'manual' | 'passthrough'
    method = Column(String, primary_key=True)

    raw_name = Column(String, nullable=False)
    # Null means "reviewed and rejected" — e.g. a project title that is not an
    # organization at all. Distinct from having no row, which means unreviewed.
    resolved_name = Column(String, nullable=True)
    confidence = Column(Float, nullable=True)
    notes = Column(String, nullable=True)
    response_full = Column(JSONB, nullable=True)
