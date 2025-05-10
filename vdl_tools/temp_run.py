from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import WebPagesParsed, WebPagesScraped


with get_session() as session:
    web_pages_scraped_query = (
        session.query(
        WebPagesScraped.cleaned_key,
        WebPagesScraped.response_status_code,
        WebPagesScraped.home_url,
    )
        .filter(WebPagesScraped.page_type == 'PageType.INDEX')
        .subquery('scraped_pages')
    )

    web_pages_parsed_query = (
        session.query(WebPagesParsed, web_pages_scraped_query.c.response_status_code)
        .join(web_pages_scraped_query, WebPagesParsed.cleaned_home_key == web_pages_scraped_query.c.cleaned_key, isouter=True)
    )

    results = []
    for x in web_pages_parsed_query:
        result_dict = x[0].to_dict()
        result_dict['response_status_code'] = x[1]
        results.append(result_dict)
