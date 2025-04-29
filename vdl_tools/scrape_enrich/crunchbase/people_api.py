import vdl_tools.scrape_enrich.crunchbase.api as api
import vdl_tools.scrape_enrich.crunchbase._config as cb_config


url = "https://api.crunchbase.com/api/v4/searches/people"


# for funding rounds field names check crunchbase docs
# https://app.swaggerhub.com/apis-docs/Crunchbase/crunchbase-enterprise_api/1.0.3#/Person
default_fields = [
    "aliases",
    "born_on",
    "created_at",
    "description",
    "died_on",
    "entity_def_id",
    "facebook",
    "facet_ids",
    "first_name",
    "gender",
    "identifier",
    "image_id",
    "image_url",
    "investor_stage",
    "investor_type",
    "last_name",
    "layout_id",
    "linkedin",
    "location_group_identifiers",
    "location_identifiers",
    "middle_name",
    "name",
    "num_articles",
    "num_current_advisor_jobs",
    "num_current_jobs",
    "num_diversity_spotlight_investments",
    "num_event_appearances",
    "num_exits",
    "num_exits_ipo",
    "num_investments",
    "num_jobs",
    "num_lead_investments",
    "num_past_advisor_jobs",
    "num_past_jobs",
    "num_portfolio_organizations",
    # "override_layout_id",
    "permalink",
    "permalink_aliases",
    "primary_job_title",
    "primary_organization",
    "rank_delta_d30",
    "rank_delta_d7",
    "rank_delta_d90",
    "rank_person",
    "rank_principal",
    "short_description",
    "twitter",
    "uuid",
    "website_url"
]

all_fields = [
    *default_fields
]


query = api.api_query_factory(cb_config.get_url(url), default_fields)


if __name__ == "__main__":
    res = query(
        filters=[
            api.eq('first_name', "Linus"),
            api.eq('last_name', 'Torvalds')
        ]
    )

    res.to_csv('test.csv')

    print(res.head())
