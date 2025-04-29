import os
import re
from sys import platform
import requests
import pathlib as pl
import os
from operator import itemgetter

def extract_version_registry(output):
    try:
        google_version = ''
        for letter in output[output.rindex('DisplayVersion    REG_SZ') + 24:]:
            if letter != '\n':
                google_version += letter
            else:
                break
        return(google_version.strip())
    except TypeError:
        return

def extract_version_folder():
    # Check if the Chrome folder exists in the x32 or x64 Program Files folders.
    for i in range(2):
        path = 'C:\\Program Files' + (' (x86)' if i else '') +'\\Google\\Chrome\\Application'
        if os.path.isdir(path):
            paths = [f.path for f in os.scandir(path) if f.is_dir()]
            for path in paths:
                filename = os.path.basename(path)
                pattern = '\d+\.\d+\.\d+\.\d+'
                match = re.search(pattern, filename)
                if match and match.group():
                    # Found a Chrome version.
                    return match.group(0)

    return None

def get_chrome_version():
    version = None
    install_path = None

    try:
        if platform == "linux" or platform == "linux2":
            # linux
            install_path = "/usr/bin/google-chrome"
        elif platform == "darwin":
            # OS X
            install_path = "/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome"
        elif platform == "win32":
            # Windows...
            version = extract_version_folder()
    except Exception as ex:
        print(ex)

    version = os.popen(f"{install_path} --version").read().strip('Google Chrome ').strip() if install_path else version

    return version


def save_version(version):
    p = pl.Path.cwd() / '.cache' / 'chromedriver_version'
    p.parent.mkdir(exist_ok=True)

    with open(p, 'w+', encoding="utf-8") as f:
        f.write(version)


def get_saved_version():
    saved_data = pl.Path.cwd() / '.cache' / 'chromedriver_version'
    if not saved_data.exists():
        return None

    with open(saved_data, 'r', encoding="utf-8") as f:
        return f.read()


def get_chromedriver_version():
    local_chrome_version = get_chrome_version()
    if not local_chrome_version:
        raise ValueError("Chrome distribution is not found on the machine")

    major = local_chrome_version.split('.')[0]
    saved_version = get_saved_version()
    if saved_version:
        saved_major = saved_version.split('.')[0]
        if major == saved_major:
            return saved_version

    res_ver = requests.get('https://googlechromelabs.github.io/chrome-for-testing/known-good-versions.json')
    data = res_ver.json()

    versions = data['versions']
    versions.sort(key=itemgetter('revision'), reverse=True)
    most_recent = versions[0]['version']
    versions = [x for x in versions if x['version'].split('.')[0] == major]
    if len(versions) == 0:
        raise ValueError(f"Can not find the matching Chrome for Testing version, the most recent is {most_recent}, while local Chrome is {local_chrome_version}")
    
    chromedriver_version = versions[0]['version']
    save_version(chromedriver_version)
    return chromedriver_version

if __name__ == '__main__':
    print(get_chrome_version())