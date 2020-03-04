import os
from dataclasses import dataclass, astuple

import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomextdata.lib.excel import parse
from tcomextdata.lib.utils import save_to_csv
from settings import CONFIG_DIR
from tcomextdata.tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@dataclass
class Row:
    code: str
    namekz: str
    nameru: str


config_path = os.path.join(CONFIG_DIR, 'mkeis.conf')
add_config_path(config_path)


class sgov_mkeis(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class MkeisParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(MkeisParse)
class GzipMkeisToFtp(GzipToFtp):
    pass


class Mkeis(luigi.WrapperTask):
    def requires(self):
        return GzipMkeisToFtp(url=sgov_mkeis().url, name=sgov_mkeis().name(),
                              skiptop=sgov_mkeis().skiptop)


if __name__ == '__main__':
    luigi.run()
