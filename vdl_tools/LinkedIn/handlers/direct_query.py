import requests
import vdl_tools.shared_tools.tools.log_utils as log

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}


def get_organization(id: str):
    url = f'https://www.linkedin.com/company/{id}'
    data = requests.get(url, headers=HEADERS, timeout=60)
    if ('authwall' in data.url) or ('/login' in data.url):
        log.warn(f"Failed to send GET request for organization {id}. Redirected to {data.url}")
        return None

    if not data.ok:
        log.warn(f"Failed to send GET request for organization {id}. Status code: {data.status_code}")
        if data.status_code == 404:
            raise FileNotFoundError(f"Organization {id} not found") 
        return None
    
    if '<body' not in data.text:
        log.warn(f"Failed to send GET request for organization {id}. No body in response")
        return None
    
    if 'core-section-container__content' not in data.text:
        log.warn(f"Failed to send GET request for organization {id}. No core-section-container in response")
        return None
    
    return data.text
