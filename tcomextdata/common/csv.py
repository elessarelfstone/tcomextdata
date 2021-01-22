import os

import attr

CSV_SEP = ';'
CSV_SEP_REPLACEMENT = ' '


def clean_for_csv(value: str):
    # replace CSV_SEP symbol in value by ' '
    val = str(value).replace(CSV_SEP, CSV_SEP_REPLACEMENT)

    # remove trailing newline
    val = val.strip().replace('\n', '')

    # replace double quote by single qoute
    val = val.replace('"', "'")

    return val


def basic_corrector(value):
    return sep_clean(value).rstrip().replace('"', "'").replace('\n', '')


def sep_clean(value):
    return value.replace(CSV_SEP, '')


def date_corrector(value):
    return value.split('+')[0]


def num_corrector(value):
    return sep_clean(basic_corrector(value))


def common_corrector(value):
    return '' if value is None else value


def bool_corrector(value):
    return str(value) if isinstance(value, bool) else value


def float_corrector(value):
    return value.replace(',', '.')


def dict_to_csvrow(raw_dict, struct):
    """ Convert given dict into tuple using
    given structure(attr class)."""

    # cast each keys's name of dict to lower case
    src_dict = {k.lower(): v for k, v in raw_dict.items() if k.lower()}

    # get fields of structure
    keys = [a.name for a in attr.fields(struct)]

    d = {}

    # build new dict with fields specified in struct
    for k in keys:
        if k in src_dict.keys():
            d[k] = src_dict[k]

    # wrap in struct
    attr_obj = struct(**d)

    return attr.astuple(attr_obj)


def save_csvrows(fpath: str, recs, sep=None, quoter=None):
    """ Save list of tuples as csv rows to file """

    if sep:
        _sep = sep
    else:
        _sep = CSV_SEP

    if quoter is None:
        _q = ''
    else:
        _q = quoter

    with open(fpath, 'a+', encoding="utf-8") as f:
        for rec in recs:
            # clean
            _rec = [clean_for_csv(v) for v in rec]
            # quoting
            _rec = [f'{_q}{v}{_q}' for v in _rec]
            row = _sep.join(_rec)
            f.write(row + '\n')

    return os.path.getsize(fpath)