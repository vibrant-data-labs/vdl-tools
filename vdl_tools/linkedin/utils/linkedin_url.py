def extract_linkedin_id(url: str):
    '''
    Extracts the LinkedIn ID from a LinkedIn profile URL.
    '''
    li_url = url
    # omit the query string
    li_url = li_url.split('?')[0]

    # omit the trailing slash
    li_url = li_url[:-1] if li_url.endswith('/') else li_url

    # omit the about page
    if '/about' in li_url:
        li_url = li_url.split('/about')[0]

    return li_url.split('/')[-1]


if __name__ == '__main__':
    print(extract_linkedin_id('https://ge.linkedin.com/company/whatever/about/'))