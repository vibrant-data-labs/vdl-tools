
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.scrape_enrich.netzero_insights.netzero_api import NetZeroAPI
from vdl_tools.scrape_enrich.netzero_insights.filters import StartupFilter


config = get_configuration()

def search_companies_by_name(
    names: str,
    use_sandbox: bool = False,
):
    netzero_api = NetZeroAPI(
        username=config.get("netzero_insights", "username"),
        password=config.get("netzero_insights", "password"),
        use_sandbox=use_sandbox,
    )
    company_ids = netzero_api.get_startups(
        include=StartupFilter(
            keywords=names,
        ),
    )
    return company_ids


if __name__ == "__main__":
    print(search_companies_by_name(["blocpower", "radiant nuclear"], use_sandbox=True))
