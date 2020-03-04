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
    region: str
    office_of_tax_enforcement: str
    ote_id: str
    bin: str
    rnn: str
    taxpayer_organization_ru: str
    taxpayer_organization_kz: str
    last_name_kz: str
    first_name_kz: str
    middle_name_kz: str
    last_name_ru: str
    first_name_ru: str
    middle_name_ru: str
    owner_iin: str
    owner_rnn: str
    owner_name_kz: str
    owner_name_ru: str
    economic_sector: str
    total_due: str
    sub_total_main: str
    sub_total_late_fee: str
    sub_total_fine: str


config_path = os.path.join(CONFIG_DIR, 'debtors150.conf')
add_config_path(config_path)


class kgd_debtors150(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class Debtors150Parse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(Debtors150Parse)
class GzipDebtorsToFtp(GzipToFtp):
    pass


class Debtors150(luigi.WrapperTask):
    def requires(self):
        return GzipDebtorsToFtp(url=kgd_debtors150().url, name=kgd_debtors150().name(),
                                skiptop=kgd_debtors150().skiptop)


if __name__ == '__main__':
    luigi.run()
