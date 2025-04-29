import vdl_tools.scrape_enrich.crunchbase.api as api
import vdl_tools.scrape_enrich.crunchbase._config as cb_config


url = "https://api.crunchbase.com/api/v4/searches/organizations"


# for organizations field names check crunchbase docs
# https://app.swaggerhub.com/apis-docs/Crunchbase/crunchbase-enterprise_api/1.0.3#/Organization
default_fields = [
    "identifier",
    "status",
    "description",
    "categories",
    "location_identifiers",
    "short_description",
    "founder_identifiers",
    "website_url",
    "linkedin",
    "founded_on",
    "last_funding_at",
    "funding_total",
    "location_group_identifiers",
    "revenue_range",
    "operating_status",
    "company_type",
    "category_groups",
    "num_employees_enum",
    "last_funding_total",
    "num_funding_rounds",
    "funding_stage",
    "ipo_status",
    "last_funding_type",
    "investor_identifiers",
    "num_investors",
    "acquirer_identifier",
    "equity_funding_total",
    "diversity_spotlights"
]

all_fields = [
    *default_fields,
    "aliases",
    "closed_on",
    "contact_email",
    "delisted_on",
    "demo_days",
    "entity_def_id",
    "exited_on",
    "facebook",
    "facet_ids",
    "funding_stage",
    "funds_total",
    "hub_tags",
    "image_id",
    "image_url",
    "investor_stage",
    "investor_type",
    "last_equity_funding_total",
    "last_equity_funding_type",
    "legal_name",
    "listed_stock_symbol",
    "name",
    "num_acquisitions",
    "num_alumni",
    "num_articles",
    "num_current_advisor_positions",
    "num_current_positions",
    "num_diversity_spotlight_investments",
    "num_enrollments",
    "num_event_appearances",
    "num_exits",
    "num_exits_ipo",
    "num_founder_alumni",
    "num_founders",
    "num_funds",
    "num_investments",
    "num_lead_investments",
    "num_lead_investors",
    "num_past_positions",
    "num_portfolio_organizations",
    "num_sub_organizations",
    "owner_identifier",
    "permalink",
    "permalink_aliases",
    "phone_number",
    "program_application_deadline",
    "program_duration",
    "program_type",
    "rank_delta_d30",
    "rank_delta_d7",
    "rank_delta_d90",
    "rank_org",
    "rank_principal",
    "school_method",
    "school_program",
    "school_type",
    "stock_exchange_symbol",
    "stock_symbol",
    "twitter",
    "valuation",
    "valuation_date",
    "went_public_on"
]


query = api.api_query_factory(cb_config.get_url(url), default_fields)

if __name__ == "__main__":
    import pandas as pd
    res: pd.DataFrame = query(
        filters=[
            api.contains(
                "description",
                [
                    "agriculture",
                    "abandoned farmland",
                    "bamboo production",
                ],
            ),
            api.eq("status", "operating"),
            api.includes(
                "location_identifiers",
                [
                    "california-united-states"  # permalink of the location to verify the location, open url https://www.crunchbase.com/location/{permalink}
                ],
            ),
        ]
    )

    res.to_excel('test.xlsx')
    print(res['founder_identifiers'].head())
