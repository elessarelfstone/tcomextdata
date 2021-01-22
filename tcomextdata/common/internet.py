import requests

from requests import Session


class ConfiguredSession(Session):

    def __init__(self):
        super().__init__()

    @classmethod
    def from_json(cls):
        pass


def get(url: str, headers=None, timeout=None) -> str:

    # we always set verify to False
    # cause we don't use certificate into
    # Kazakhtelecom network
    r = requests.get(url, verify=False, headers=headers, timeout=timeout)
    # try:
    if r.status_code != 200:
        r.raise_for_status()
    # except :
    #     raise ExternalSourceError()
    return r.text
