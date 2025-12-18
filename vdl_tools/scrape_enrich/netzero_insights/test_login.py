import requests

session = requests.Session()
response = session.post(
    f"https://20.108.20.67/security/formLogin",
    data={
        "username": "api@vibrantdatalabs.org",
        "password": "tWz_ntqMequ3q-9NkiPk7z1xJeQ!i4Y#bTV8"
    },
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    verify=False,
)