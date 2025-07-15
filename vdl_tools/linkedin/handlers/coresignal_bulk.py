import requests
import json
import gzip
import time

from vdl_tools.shared_tools.tools.logger import logger


from vdl_tools.linkedin.handlers.coresignal_query import create_esl_dsl_query

MAX_N_TIMES_PROCESSING = 10
SECONDS_BETWEEN_REQUESTS = 60


def get_headers(api_key):
    return {
        'Content-Type': 'application/json',
        'apikey': api_key
    }


def make_bulk_request_company(linkedin_ids: list, api_key: str):
    query = create_esl_dsl_query(linkedin_ids=linkedin_ids)
    payload = {
        "data_format": "json",
        "es_dsl_query": query,
    }
    logger.info("Making request to Coresignal for %s companies", len(linkedin_ids))
    res = requests.post(
        "https://api.coresignal.com/cdapi/v2/data_requests/company_base/es_dsl",
        headers=get_headers(api_key),
        json=payload,
        timeout=360,
    )

    if not res.ok:
        logger.warning('CoreSignal response is not OK')
        logger.warning(res.content)
        logger.info(json.dumps(query, indent=2))
        return None

    request_id = res.json()['request_id']
    logger.info("Request made, request_id: %s", request_id)
    return request_id


def get_processed_filenames(
    request_id: str,
    api_key: str,
    wait_time:int = SECONDS_BETWEEN_REQUESTS,
    max_iterations:int = MAX_N_TIMES_PROCESSING,
):
    still_processing = True
    i = 0
    while still_processing:
        files_request = requests.get(
            f"https://api.coresignal.com/cdapi/v2/data_requests/{request_id}/files",
            headers=get_headers(api_key),
            timeout=360,
        )
        if files_request.json().get("message") == "Data request is still being processed":
            logger.info("still processing %s", i)
            time.sleep(wait_time)
            i += 1
        else:
            still_processing = False
        if i >= max_iterations:
            logger.exception(
                "Results still not finished, last iteration response: \n"
                f"{json.dumps(files_request.json(), indent=2)}"
            )
            return None
    if files_request.status_code != 200:
        if files_request.status_code != 422:
            # 422 is a valid response, it means that there are no files to retrieve because no companies
            # matched the search
            logger.exception(
                "Error retrieving files: %s - %s", files_request.status_code, files_request.content,
            )
        return None
    data_request_files = files_request.json()["data_request_files"]
    logger.info("Files names for %s files found", len(data_request_files))
    return data_request_files


def retrieve_bulk_results(request_id, filepart_names, api_key):
    logger.info("Retrieving bulks results for %s filenames", len(filepart_names))
    results = []
    for i, filename in enumerate(filepart_names):
        file_part = requests.get(
            f"https://api.coresignal.com/cdapi/v2/data_requests/{request_id}/files/{filename}",
            headers=get_headers(api_key),
            timeout=360,
        )
        gzipped_bytes = file_part.content
        decompressed_bytes = gzip.decompress(gzipped_bytes)
        decompressed_string = decompressed_bytes.decode('utf-8').strip()

        # It's set up as JSONL data (jsons split by new line)
        for json_string in decompressed_string.split('\n'):
            results.append(json.loads(json_string))
        if i % 100 == 0:
            logger.info("Processed %s / %s filenames", i, len(filepart_names))    
    return results


def get_bulk_organization_results(
    linkedin_ids,
    api_key=None,
    config=None,
    wait_time=SECONDS_BETWEEN_REQUESTS,
    max_iterations=MAX_N_TIMES_PROCESSING,
):
    if not (config and config.get('linkedin', {}).get('coresignal_api_key') or api_key):
        raise Exception("Must provide api_key or config")
    
    if not api_key:
        api_key = config['coresignal_api_key']

    request_id = make_bulk_request_company(linkedin_ids, api_key)
    if not request_id:
        return []
    filepart_names = get_processed_filenames(
        request_id,
        api_key,
        wait_time=wait_time,
        max_iterations=max_iterations
    )
    if not filepart_names:
        return []
    results = retrieve_bulk_results(request_id, filepart_names, api_key)
    return results


if __name__ == '__main__':
    from vdl_tools.shared_tools.tools.config_utils import get_configuration

    config = get_configuration()
    api_key = config['linkedin']['coresignal_api_key']

    test_ids = [
        'tesla-motors',
        'softbank-group-international',
        'rivian',
        'rubrik-inc',
        'creative-destruction-lab',
        'aurora-inc.',
        'flyarcher',
        'quantumscape',
        'nio%E8%94%9A%E6%9D%A5',
        'vanmoof',
        'faradayfuture',
        'bt',
        'fiskerinc',
        'efisheryid',
        'blocpower',
        'chargepoint',
        'affirm',
        '3035148',
        'beyond-meat',
        'vinfast-llc'
    ]
    results = get_bulk_organization_results(
        test_ids,
        api_key,
    )

