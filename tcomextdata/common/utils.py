import os
import gzip
import hashlib
import json
from pathlib import Path
from typing import Dict, Union
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import requests
import urllib3
import shutil
import subprocess as subp
from collections import namedtuple, Counter
from os.path import basename

from settings import TASKS_PARAMS_CONFIG_PATH

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# handy working with formats
Formats = namedtuple('Formats', ['extension', 'mime', 'offset', 'signature'])

REQUEST_TIMEOUT = 10


def read_file_rows(fpath):
    """ Return rows of text file as list """
    with open(fpath, "r", encoding="utf-8") as f:
        rows = [b.rstrip() for b in f.readlines()]

    return rows


def read_file(fpath: str) -> str:
    """ Return all rows of file as string"""
    with open(fpath, 'r', encoding="utf8") as f:
        data = f.read()

    return data


def append_file(fpath, data):
    """
    Add string at the end oftext file

    """
    with open(fpath, 'a+', encoding="utf8") as f:
        f.write(data + '\n')


def get_fname_noext(fpath):
    parts = list(os.path.splitext(basename(fpath)))
    parts.reverse()
    return parts.pop()


def get_fpath_noext(fpath):
    parts = list(os.path.splitext(fpath))
    parts.reverse()
    return parts.pop()


def replace_fext(fpath, ext):
    return Path(fpath).with_suffix(ext)


def gzip_file(fpath, ext):
    """ Gzip given file"""

    target_fpath = os.path.join(os.path.dirname(os.path.abspath(fpath)),
                                replace_fext(fpath, ext))

    with open(fpath, 'rb') as f_src:
        with gzip.open(target_fpath, 'wb') as f_target:
            shutil.copyfileobj(f_src, f_target)

    return target_fpath


def get(url: str, headers=None, timeout=None) -> str:

    # we always specify verify to False
    # cause we don't use certificate into
    # Kazakhtelecom network
    r = requests.get(url, verify=False, headers=headers, timeout=timeout)
    # try:
    if r.status_code != 200:
        r.raise_for_status()

    return r.text


def post(url: str, request_body: str,  headers=None, timeout=None) -> str:

    # we always specify verify to False
    # cause we don't use certificate into
    # Kazakhtelecom network
    r = requests.post(url, request_body, verify=False, headers=headers, timeout=timeout)
    # try:
    if r.status_code != 200:
        r.raise_for_status()

    return r.text


def file_formats():
    """ Load file formats we work with. For checking and proper saving."""
    # load formats from file
    data = read_file(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "file_formats.json")
    )

    # wrap in Formats struct
    _formats = []
    for f in json.loads(data):
        _formats.append(Formats(**f))

    return _formats


def identify_format(fpath):
    """ Read signature of file and return format if it's supported """

    _formats = file_formats()

    # read first N bytes
    with open(fpath, "rb") as file:
        # 300 bytes are enough
        header = file.read(300)

    # convert to hex
    stream = " ".join(['{:02X}'.format(byte) for byte in header])

    for frmt in _formats:
        # if there is offset
        offset = frmt.offset * 2 + frmt.offset
        if frmt.signature == stream[offset:len(frmt.signature) + offset]:
            return frmt.extension

    return None


def build_fname(fname, ext, suff=None):
    return f'{fname}_{suff}.{ext}' if suff else f'{fname}.{ext}'


def build_fpath(fdir, fname, ext):
    """ Return concatenated file's path """
    fpath = os.path.join(fdir, f'{fname}.{ext}')
    return fpath


def check_fileheader(fpath):
    """ Check file's signature. If match return True. """
    with open(fpath, "rb") as file:
        # 300 bytes are enough
        header = file.read(300)

    # convert to hex
    stream = " ".join(['{:02X}'.format(byte) for byte in header])

    # check
    for frmt in file_formats():
        offset = frmt.offset * 2 + frmt.offset
        if frmt.signature == stream[offset:len(frmt.signature) + offset]:
            return frmt.extension

    return None


def get_hash(f_path, mode='sha256'):
    """ Get hash of file"""

    h = hashlib.new(mode)

    with open(f_path, 'rb') as file:
        block = file.read(4096)
        while block:
            h.update(block)
            block = file.read(4096)

    return h.hexdigest()


def get_stata(c: Counter):
    return ' '.join(('{}:{}'.format(k, v) for k, v in c.items()))


def get_lastrow_ncolumn_value_in_csv(fpath, column_num: int, sep=';'):
    if os.path.exists(fpath) and (os.stat(fpath).st_size != 0):
        r = subp.check_output("awk -F'{}' 'END{{print $1}}' {}".format(sep, fpath))
        return r.split()[column_num].decode(encoding='utf-8')

    return None


def get_file_lines_count(fpath: str):
    if os.path.exists(fpath):
        r = subp.check_output("wc -l {}".format(fpath))
        return int(r.decode(encoding='utf-8').split()[0]) if r else 0

    return None


def get_yaml_task_config(section):
    with open(TASKS_PARAMS_CONFIG_PATH) as c:
        config = load(c, Loader=Loader)

    return config[section]
