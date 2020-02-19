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
    lv0: str = ''
    lv1: str = ''
    lv2: str = ''
    lv3: str = ''


config_path = os.path.join(CONFIG_DIR, 'oked.conf')
add_config_path(config_path)


class sgov_oked(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


def update_rows(rows):
    """ Complete each row with levels """
    curr_root = rows[0].code

    for r in rows:
        # build new code
        # A, B, C, etc are like roots for a certain code
        if ('.' in r.code) or (r.code.replace('.', '').isdigit()):
            code = f'{curr_root}.{r.code}'
        else:
            code = r.code
            curr_root = r.code

        b = code.split('.')
        size = len(b)
        if size == 2:
            r.lv0 = b[0]

        elif size == 3:
            if len(b[2]) == 1:
                r.lv0, r.lv1 = b[0], b[1]
            else:
                r.lv0, r.lv1, r.lv2 = b[0], b[1], f'{b[1]}{b[2][0]}'

        elif size == 4:
            r.lv0, r.lv1, r.lv2, r.lv3 = b[0], b[1], f'{b[1]}{b[2][0]}', f'{b[1]}{b[2]}'


class OkedParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=sgov_oked().skiptop)
        update_rows(rows)
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(OkedParse)
class GzipOkedToFtp(GzipToFtp):
    pass


class Oked(luigi.WrapperTask):
    def requires(self):
        return GzipOkedToFtp(url=sgov_oked().url, name=sgov_oked().name(),
                             skiptop=sgov_oked().skiptop)


if __name__ == '__main__':
    luigi.run()
