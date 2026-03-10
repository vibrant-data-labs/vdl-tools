from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import get_netzero_api
netzero_api = get_netzero_api(use_sandbox=False)
netzero_api.logout()


from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import (
    search_companies,
    get_companies_details,
    get_company_commercial_deals,
)

companies = search_companies(minimum_commercial_deals=1, include_headquarters=[956473], limit=None, use_sandbox=False)