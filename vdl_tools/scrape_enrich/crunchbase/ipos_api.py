import vdl_tools.scrape_enrich.crunchbase.api as api
import vdl_tools.scrape_enrich.crunchbase._config as cb_config


url = "https://api.crunchbase.com/api/v4/searches/ipos"


# for organizations field names check crunchbase docs
# https://app.swaggerhub.com/apis-docs/Crunchbase/crunchbase-enterprise_api/1.0.3#/Ipo
default_fields = [
    "amount_raised",
    "created_at",
    "delisted_on",
    "entity_def_id",
    "identifier",
    "image_id",
    "organization_identifier",
    "permalink",
    "rank_ipo",
    "share_price",
    "shares_outstanding",
    "shares_sold",
    "short_description",
    "stock_exchange_symbol",
    "stock_full_symbol",
    "stock_symbol",
    "updated_at",
    "uuid",
    "valuation",
    "went_public_on",
]

all_fields = [
    *default_fields,
]


query = api.api_query_factory(cb_config.get_url(url), default_fields)
