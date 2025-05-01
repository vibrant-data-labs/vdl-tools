import asyncio
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.scrape_enrich.netzero_insights.netzero_api import NetZeroAPI
from vdl_tools.scrape_enrich.netzero_insights.filters import MainFilter, StartupFilter, InvestorFilter


config = get_configuration()


def search_companies(
    include_keywords: list[str] = None,
    exclude_keywords: list[str] = None,
    include_investors: list[int] = None,
    exclude_investors: list[int] = None,
    limit: int = 100,
    use_sandbox: bool = False,
):
    netzero_api = NetZeroAPI(
        username=config.get("netzero_insights", "username"),
        password=config.get("netzero_insights", "password"),
        use_sandbox=use_sandbox,
    )
    include_keywords = include_keywords or []
    exclude_keywords = exclude_keywords or []
    include_keywords_phrases = [f'"{keyword}"' for keyword in include_keywords]
    exclude_keywords_phrases = [f'-"{keyword}"' for keyword in exclude_keywords]

    main_filter = MainFilter()
    if include_keywords:
        startup_include_filter = StartupFilter()
        startup_include_filter.wildcards = [" ".join(include_keywords_phrases)]
        startup_include_filter.wildcardsFields = ["pitchLine", "description"]
        main_filter.include = startup_include_filter

    if exclude_keywords:
        startup_exclude_filter = StartupFilter()
        startup_exclude_filter.wildcards = [" ".join(exclude_keywords_phrases)]
        startup_exclude_filter.wildcardsFields = ["pitchLine", "description"]
        main_filter.exclude = startup_exclude_filter

    if include_investors:
        investor_include_filter = InvestorFilter()
        investor_include_filter.investorIDs = include_investors
        main_filter.investorInclude = investor_include_filter

    if exclude_investors:
        investor_exclude_filter = InvestorFilter()
        investor_exclude_filter.investorIDs = exclude_investors
        main_filter.investorExclude = investor_exclude_filter

    companies = netzero_api.search_startups(
        main_filter=main_filter,
        limit=limit,
    )

    return companies


def get_companies_details(
    company_ids: list[int],
    use_sandbox: bool = False,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
):
    netzero_api = NetZeroAPI(
        username=config.get("netzero_insights", "username"),
        password=config.get("netzero_insights", "password"),
        use_sandbox=use_sandbox,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
    )

    companies = asyncio.run(netzero_api.get_startup_details(company_ids))

    return companies


if __name__ == "__main__":
    # companies = search_companies(
    #     include_keywords=["ocean"],
    #     use_sandbox=False,
    #     limit=500,
    # )

    USE_SANDBOX = False
    READ_FROM_CACHE = True
    WRITE_TO_CACHE = True

    grantham_investor_ids = [6121, 9939, 33402]
    grantham_companies = search_companies(
        include_investors=grantham_investor_ids,
        use_sandbox=USE_SANDBOX,
        limit=500,
    )

    grantham_company_ids = [company["clientID"] for company in grantham_companies['results']]

    companies = get_companies_details(
        company_ids=grantham_company_ids,
        use_sandbox=USE_SANDBOX,
        read_from_cache=READ_FROM_CACHE,
        write_to_cache=WRITE_TO_CACHE,
    )

