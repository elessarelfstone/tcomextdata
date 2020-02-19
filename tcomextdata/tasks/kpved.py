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


config_path = os.path.join(CONFIG_DIR, 'kpved.conf')
add_config_path(config_path)


class sgov_kpved(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class KpvedParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(KpvedParse)
class GzipKpvedToFtp(GzipToFtp):
    pass


class Kpved(luigi.WrapperTask):
    def requires(self):
        return GzipKpvedToFtp(url=sgov_kpved().url, name=sgov_kpved().name(),
                              skiptop=sgov_kpved().skiptop)


if __name__ == '__main__':
    luigi.run()
