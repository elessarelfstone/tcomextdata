""" Streets codifier """
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


config_path = os.path.join(CONFIG_DIR, 'kurk.conf')
add_config_path(config_path)


class sgov_kurk(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class KurkParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(KurkParse)
class GzipKurkToFtp(GzipToFtp):
    pass


class Kurk(luigi.WrapperTask):
    def requires(self):
        return GzipKurkToFtp(url=sgov_kurk().url, name=sgov_kurk().name(),
                              skiptop=sgov_kurk().skiptop)


if __name__ == '__main__':
    luigi.run()
