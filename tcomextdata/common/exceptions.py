from requests.exceptions import ConnectionError, HTTPError


class ExternalSourceError(ConnectionError, HTTPError):
    pass


class ParsedFileNotExists(Exception):
    pass


