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
    owner_rnn: str
    court_decision: str
    court_decision_date: str


config_path = os.path.join(CONFIG_DIR, 'bankrupt.conf')
add_config_path(config_path)


class kgd_bankrupt(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class BankruptParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(BankruptParse)
class GzipBankruptToFtp(GzipToFtp):
    pass


class Bankrupt(luigi.WrapperTask):
    def requires(self):
        return GzipBankruptToFtp(url=kgd_bankrupt().url, name=kgd_bankrupt().name(),
                                 skiptop=kgd_bankrupt().skiptop)


if __name__ == '__main__':
    luigi.run()
