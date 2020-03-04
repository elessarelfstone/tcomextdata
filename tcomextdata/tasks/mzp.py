import json
import os
import re
from datetime import date
from dataclasses import dataclass, astuple

import luigi
import attr
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomextdata.tasks.base import ParseJavaScript
from tcomextdata.lib.js import js_raw
from tcomextdata.lib.exceptions import ExternalSourceError
from tcomextdata.lib.utils import save_to_csv
from settings import CONFIG_DIR
from tcomextdata.tasks.base import GzipToFtp, BaseConfig


@dataclass
class Row:
    id: str
    value: str
    date: str

    def __post_init__(self):
        self.id = str(self.id)
        self.value = str(self.value)



# @attr.s
# class Row:
#     id = attr.ib(converter=lambda x: str(x))
#     date = attr.ib(default='')
#     rate = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'mzp.conf')
add_config_path(config_path)


class kgd_mzp(BaseConfig):
    url = luigi.Parameter(default='')


def parse(url, pattern):
    r = re.search(pattern, js_raw(url))
    if r:
        return json.loads(r.group(2))
    else:
        raise ExternalSourceError('Javascript data not found')


class MzpParse(ParseJavaScript):

    def run(self):
        p = r'("ref"\s*):(\s*\[\S+\])'
        # concatenated all javascript of html
        r = re.search(p, js_raw(self.url))
        if r:
            d = parse(self.url, p)
            rows = [Row(**_d) for _d in d]
            save_to_csv(self.output().path, [astuple(_d) for _d in rows])
        else:
            raise ExternalSourceError('Javascript data not found')


@requires(MzpParse)
class GzipMzpToFtp(GzipToFtp):
    pass


class Mzp(luigi.WrapperTask):
    def requires(self):
        return GzipMzpToFtp(url=kgd_mzp().url, name=kgd_mzp().name())


if __name__ == '__main__':
    luigi.run()
