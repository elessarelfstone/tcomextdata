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
    num: str
    bin: str
    rnn: str
    taxpayer_organization: str
    taxpayer_name: str
    owner_name: str
    owner_iin: str
    owner_no: str
    order_date: str


config_path = os.path.join(CONFIG_DIR, 'inactive.conf')
add_config_path(config_path)


class kgd_inactive(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class InactiveParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(InactiveParse)
class GzipInactiveToFtp(GzipToFtp):
    pass


class Inactive(luigi.WrapperTask):
    def requires(self):
        return GzipInactiveToFtp(url=kgd_inactive().url, name=kgd_inactive().name(),
                                 skiptop=kgd_inactive().skiptop)


if __name__ == '__main__':
    luigi.run()
