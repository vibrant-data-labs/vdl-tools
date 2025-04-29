import vdl_tools.scrape_enrich.crunchbase.api as api
import vdl_tools.scrape_enrich.crunchbase._config as cb_config


url = "https://api.crunchbase.com/api/v4/searches/acquisitions"


# for organizations field names check crunchbase docs
# https://app.swaggerhub.com/apis-docs/Crunchbase/crunchbase-enterprise_api/1.0.3#/Acquisition
default_fields = [
    "acquiree_categories",
    "acquiree_funding_total",
    "acquiree_identifier",
    "acquiree_last_funding_type",
    "acquiree_locations",
    "acquiree_num_funding_rounds",
    "acquiree_revenue_range",
    "acquiree_short_description",
    "acquirer_categories",
    "acquirer_funding_stage",
    "acquirer_funding_total",
    "acquirer_identifier",
    "acquirer_locations",
    "acquirer_num_funding_rounds",
    "acquirer_revenue_range",
    "acquirer_short_description",
    "acquisition_type",
    "announced_on",
    "completed_on",
    "created_at",
    "disposition_of_acquired",
    "entity_def_id",
    "identifier",
    "permalink",
    "price",
    "rank_acquisition",
    "short_description",
    "status",
    "terms",
    "updated_at",
    "uuid"
]

all_fields = [
    *default_fields,
]


query = api.api_query_factory(cb_config.get_url(url), default_fields)