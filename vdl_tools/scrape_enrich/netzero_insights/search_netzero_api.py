import asyncio
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.scrape_enrich.netzero_insights.netzero_api import NetZeroAPI
from vdl_tools.scrape_enrich.netzero_insights.filters import MainFilter, StartupFilter, InvestorFilter
from vdl_tools.shared_tools.tools.logger import logger

config = get_configuration()


def get_netzero_api(
    use_sandbox: bool = False,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
):
    if use_sandbox:
        password = config.get("netzero_insights", "password_sandbox")
    else:
        password = config.get("netzero_insights", "password")
    return NetZeroAPI(
        username=config.get("netzero_insights", "username"),
        password=password,
        use_sandbox=use_sandbox,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
    )


def create_search_filter(
    include_keywords: list[str] = None,
    exclude_keywords: list[str] = None,
    include_investors: list[int] = None,
    exclude_investors: list[int] = None,
    include_taxonomy_items: list[int] = None,
    exclude_taxonomy_items: list[int] = None,
) -> MainFilter:

    include_keywords = include_keywords or []
    exclude_keywords = exclude_keywords or []
    include_keywords_phrases = [f'"{keyword}"' for keyword in include_keywords]
    exclude_keywords_phrases = [f'-"{keyword}"' for keyword in exclude_keywords]

    startup_include_filter = None
    startup_exclude_filter = None

    main_filter = MainFilter()
    if include_keywords:
        startup_include_filter = StartupFilter()
        startup_include_filter.wildcards = [" ".join(include_keywords_phrases)]
        startup_include_filter.wildcardsFields = ["pitchLine", "description"]

    if include_taxonomy_items:
        startup_include_filter = startup_include_filter or StartupFilter()
        startup_include_filter.taxonomyItems = include_taxonomy_items
        startup_include_filter.taxonomyItemsMode = "OR"

    if startup_include_filter:
        main_filter.include = startup_include_filter

    if exclude_keywords:
        startup_exclude_filter = StartupFilter()
        startup_exclude_filter.wildcards = [" ".join(exclude_keywords_phrases)]
        startup_exclude_filter.wildcardsFields = ["pitchLine", "description"]

    if exclude_taxonomy_items:
        startup_exclude_filter = startup_exclude_filter or StartupFilter()
        startup_exclude_filter.taxonomyItems = exclude_taxonomy_items
        startup_exclude_filter.taxonomyItemsMode = "OR"

    if startup_exclude_filter:
        main_filter.exclude = startup_exclude_filter

    if include_investors:
        investor_include_filter = InvestorFilter()
        investor_include_filter.investorIDs = include_investors
        main_filter.investorInclude = investor_include_filter

    if exclude_investors:
        investor_exclude_filter = InvestorFilter()
        investor_exclude_filter.investorIDs = exclude_investors
        main_filter.investorExclude = investor_exclude_filter

    return main_filter


def search_companies(
    limit: int = 100,
    use_sandbox: bool = False,
    netzero_api: NetZeroAPI = None,
    **filter_kwargs,
):
    netzero_api = netzero_api or get_netzero_api(use_sandbox=use_sandbox)
    main_filter = create_search_filter(**filter_kwargs)

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
    netzero_api: NetZeroAPI = None,
):
    netzero_api = netzero_api or get_netzero_api(
        use_sandbox=use_sandbox,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
    )

    companies = asyncio.run(netzero_api.get_startup_details(company_ids))

    return companies


def search_get_companies_details(
    use_sandbox: bool = False,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    limit: int = 100,
    **kwargs,
):
    # Get the netzero api client and share it with the other functions
    netzero_api = get_netzero_api(
        use_sandbox=use_sandbox,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
    )

    search_results = search_companies(
        use_sandbox=use_sandbox,
        netzero_api=netzero_api,
        limit=limit,
        **kwargs,
    )

    company_ids = [company["clientID"] for company in search_results['results']]
    logger.info(f"Found {len(company_ids)} companies")

    companies = get_companies_details(
        company_ids=company_ids,
        use_sandbox=use_sandbox,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
        netzero_api=netzero_api,
    )

    return companies


def get_startup_count(
    use_sandbox: bool = False,
    netzero_api: NetZeroAPI = None,
    **filter_kwargs,
):
    netzero_api = netzero_api or get_netzero_api(use_sandbox=use_sandbox)
    main_filter = create_search_filter(**filter_kwargs)
    return netzero_api.get_startup_count(main_filter)


if __name__ == "__main__":
    USE_SANDBOX = True
    READ_FROM_CACHE = True
    WRITE_TO_CACHE = False

    # ocean_search = search_get_companies_details(
    #     include_keywords=["ocean"],
    #     use_sandbox=USE_SANDBOX,
    #     read_from_cache=READ_FROM_CACHE,
    #     write_to_cache=WRITE_TO_CACHE,
    #     limit=10,
    # )

    print(get_startup_count(
        include_keywords=["ocean"],
        use_sandbox=USE_SANDBOX,
    ))
