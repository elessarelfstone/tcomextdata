import re
import os
import urllib3
from dataclasses import dataclass, astuple
from shutil import move

import luigi
import requests
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomextdata.lib.excel import parse
from tcomextdata.lib.utils import save_to_csv, save_webfile, build_fpath
from tcomextdata.lib.unpacking import unpack, zipo_flist
from settings import CONFIG_DIR, TMP_DIR
from tcomextdata.tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile

config_path = os.path.join(CONFIG_DIR, 'companies.conf')
add_config_path(config_path)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class Row:
    bin: str
    full_name_kz: str
    full_name_ru: str
    registration_date: str
    oked_1: str
    activity_kz: str
    activity_ru: str
    oked_2: str
    krp: str
    krp_name_kz: str
    krp_name_ru: str
    kato: str
    settlement_kz: str
    settlement_ru: str
    legal_address: str
    head_fio: str


class sgov_companies(BaseConfig):
    url = luigi.Parameter(default=None)
    template_url = luigi.Parameter(default=None)
    regex = luigi.Parameter(default=None)
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')
    sheets = luigi.TupleParameter(default=None)


class RetrieveCompaniesWebDataFiles(luigi.Task):
    url = luigi.Parameter(default=None)
    name = luigi.Parameter(default=None)
    template_url = luigi.Parameter(default=None)
    regex = luigi.Parameter(default=None)

    def _links(self):
        """ Parse links for all regions"""

        # we have page which contains all ids
        # for all region links, like ESTAT086119, etc
        _json = requests.get(self.url, verify=False).text
        ids = re.findall(self.regex, _json)

        # build links
        links = [f'{self.template_url}{_id}' for _id in ids]
        return links

    def output(self):
        fpaths = []
        links = self._links()

        # according number of links we build paths for
        # xls files, cause each zip file contains one xls file
        for i, link in enumerate(links):
            fpath = os.path.join(TMP_DIR, f'{self.name}_{i}.xls')
            fpaths.append(fpath)

        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):
        links = self._links()
        for i, f in enumerate(self.output()):

            # get just filename without extension
            bsname = os.path.basename(f.path).split('.')[0]

            # build path for each zip archive
            fpath = os.path.join(TMP_DIR, f'{bsname}.zip')
            save_webfile(links[i], fpath)
            zipo, flist = zipo_flist(fpath)

            folder = os.path.abspath(os.path.dirname(f.path))

            # extract single file
            zipo.extract(flist[0], folder)
            src = os.path.join(folder, flist[0])
            # rename xls file to file in output()
            move(src, f.path)


@requires(RetrieveCompaniesWebDataFiles)
class ParseCompanies(luigi.Task):

    sheets = luigi.TupleParameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        for target in self.input():
            sheets = self.sheets
            rows = parse(target.path, Row, skiprows=sgov_companies().skiptop,
                         sheets=sheets)
            save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(ParseCompanies)
class GzipCompaniesToFtp(GzipToFtp):
    pass


class Companies(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesToFtp(url=sgov_companies().url,
                                 name=sgov_companies().name(),
                                 template_url=sgov_companies().template_url,
                                 regex=sgov_companies().regex,
                                 sheets=sgov_companies().sheets)


if __name__ == '__main__':
    luigi.run()








