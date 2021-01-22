import json
import os
import math
import time
from time import sleep
from urllib.parse import urlparse

from box import Box
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport, TransportProtocolError

from tcomextdata.common.csv import dict_to_csvrow, save_csvrows
from tcomextdata.common.internet import get
from tcomextdata.common.utils import (read_file, read_file_rows, get_file_lines_count,
                                      append_file)


def goszakup_graphql_parsing(enity_name: str, url: str, token: str, start_date: str,
                             end_date: str, limit: str, timeout: int,
                             key_column: str, retries: int, struct=None):

    """ Return rows with data parsed from GraphQl service of goszakup.gov.kz.
    Note that this function only returns rows, doesn't not saves in file.
    That behavior can be changed in future if there's need, by additional parameter fpath.
    Using this convenient more for some piece of data, like short date range(day, week, no more ),
    not all data, cause we can get refusion(exception like TransportProtocolError) from source service.
    Therefore here we don't use retry mechanism(.prs files).
    """

    headers = dict()
    # token have got from goszakup.gov.kz
    headers['Authorization'] = token

    query_fpath = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               f'{enity_name.lower()}.gql')
    query = gql(read_file(query_fpath))

    # standart Http request
    # could be asyncronic, but there are server side restrictions
    transport = RequestsHTTPTransport(
        url=url,
        verify=False,
        retries=retries,
        headers=headers
    )

    # create graphql client
    client = Client(transport=transport, fetch_schema_from_transport=True)
    # parameters to filter data for specified date and limit portion of retrieving data
    params = {'from': start_date, 'to': end_date, 'limit': limit}

    start_from = None

    result = []

    while True:
        p = params

        # first request
        if start_from:
            p['after'] = start_from

        data = client.execute(query, variable_values=p)

        if data.get(enity_name) is None or len(data.get(enity_name, [])) == 0:
            break

        data = data.get(enity_name)

        # start position for next request (pagination)
        start_from = data[-1][key_column]

        data = [dict_to_csvrow(d, struct) for d in data]
        result.extend(data)

        # sleep to avoid blocking us on source  service
        if timeout:
            time.sleep(timeout)

    return result


def goszakup_rest_parsing(csv_fpath: str, url: str, token: str, timeout: int,
                          limit: int, struct=None, prs_fpath=None, callb_luigi_status=None):
    """ Save rows with data parsed from REST service of goszakup.gov.kz to given csv file.
    Supports retry. Every response(json) contains a url to the next page(uri) for parsing("next_page" key)
    Limitation automatily will appear in uri once we specified it at start,
    like - /v3/rnu?page=next&limit=500&search_after=100827
    So, after fall, next time parsing will be started with uri presented by last one in .prs file.
    """

    def load_and_parse(_url):
        raw = get(_url, headers=headers, timeout=timeout)
        # Box has the method with same name
        raw = raw.replace('items', 'data', 1)
        b = Box(json.loads(raw))
        # parse limitation from uri for next_page
        lim = 0
        if b.next_page:
            lim = int(b.next_page.split('&')[1].split('=')[1])
        return b.next_page, b.total, lim, b.data

    headers = dict()
    headers['Authorization'] = token

    _url = None
    host = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(url))

    last_parsed_url = None
    # so if we had problems last time and parsing was terminated due any error
    if prs_fpath and os.path.exists(prs_fpath):
        uri = read_file_rows(prs_fpath).pop()
        last_parsed_url = f'{host}{uri}'

    parsed_rows_count = 0
    total = 0
    curr_url = f'{url}?limit={limit}'
    curr_limit = limit
    if last_parsed_url:
        curr_uri, _, curr_limit, _ = load_and_parse(last_parsed_url)
        parsed_rows_count = get_file_lines_count(csv_fpath)

    while curr_url:
        try:
            curr_uri, total, curr_limit, raw_data = load_and_parse(curr_url)
        except Exception as e:
            raise
            # sleep(timeout)
        else:
            if curr_uri:
                curr_url = f'{url}?{curr_uri}'
            else:
                curr_url = None

            if prs_fpath:
                append_file(prs_fpath, curr_uri)

            data = [dict_to_csvrow(d, struct) for d in raw_data]
            save_csvrows(csv_fpath, data, quoter="\"")
            parsed_rows_count += len(raw_data)
            sleep(timeout)

        if callb_luigi_status:
            status = f'Parsing {curr_url}. Total rows: {total}. Parsed rows: {parsed_rows_count}.'
            percent = math.ceil((parsed_rows_count * 100)/total)
            callb_luigi_status(status, percent)

    res = dict(total=total, parsed=parsed_rows_count)

    return res
