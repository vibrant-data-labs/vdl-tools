import vdl_tools.scrape_enrich.crunchbase.api as api
import vdl_tools.scrape_enrich.crunchbase._config as cb_config


url = "https://api.crunchbase.com/api/v4/searches/funding_rounds"


# for funding rounds field names check crunchbase docs
# https://app.swaggerhub.com/apis-docs/Crunchbase/crunchbase-enterprise_api/1.0.3#/FundingRound
default_fields = [
    "name",
    "funded_organization_identifier",
    "money_raised",
    "announced_on",
    "lead_investor_identifiers",
    "investor_identifiers",
    "investment_stage",
    "funded_organization_funding_stage",
    "investment_type",
]

all_fields = [
    *default_fields,
    "closed_on",
    "funded_organization_categories",
    "funded_organization_description",
    "funded_organization_diversity_spotlights",
    "funded_organization_funding_total",
    "funded_organization_location",
    "funded_organization_revenue_range",
    "is_equity",
    "num_investors",
    "num_partners",
    "permalink",
    "post_money_valuation",
    "pre_money_valuation",
    "rank_funding_round",
    "short_description",
    "target_money_raised",
]


query = api.api_query_factory(cb_config.get_url(url), default_fields)


if __name__ == "__main__":
    res = query(
        filters=[
            api.contains(
                "funded_organization_description",
                [
                    "agriculture",
                    "abandoned farmland",
                    "bamboo production",
                ],
            ),
            api.includes(
                "funded_organization_location",
                [
                    "california-united-states"  # permalink of the location (to verify https://www.crunchbase.com/location/{permalink})
                ],
            ),
        ]
    )

    print(res.head())
