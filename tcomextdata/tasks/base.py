import os
import luigi

from luigi.contrib.ftp import RemoteTarget
from luigi.util import requires

from tcomextdata.lib.utils import (build_fpath, build_webfpath,
                                   save_webfile, gziped_fname, gzip_file,
                                   identify_webfileformat, download)
from tcomextdata.lib.unpacking import unpack

from settings import TMP_DIR, FTP_PATH, FTP_HOST, FTP_USER, FTP_PASS


class BaseConfig(luigi.Config):
    @classmethod
    def name(cls):
        return cls.__name__.lower()


class RetrieveWebDataFile(luigi.Task):
    url = luigi.Parameter()
    name = luigi.Parameter()

    def output(self):
        fpath = os.path.join(TMP_DIR, self.name)
        return luigi.LocalTarget(fpath)

    def run(self):
        # download file and get format(rar, zip, xls, etc) of file
        frmt = save_webfile(self.url, self.output().path)


class RetrieveWebDataFileFromArchive(luigi.Task):
    url = luigi.Parameter()
    name = luigi.Parameter()
    fnames = luigi.ListParameter(default=None)

    @staticmethod
    def get_filepaths(folder, fnames):
        return [os.path.join(folder, fname) for fname in fnames]

    def output(self):
        fnames = self.fnames
        fpaths = self.get_filepaths(TMP_DIR, fnames)
        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):

        # build path for downloading file
        fpath = os.path.join(TMP_DIR, self.name)

        # download file and get format(rar, zip, xls, etc) of file
        frmt = save_webfile(self.url, fpath)
        unpack(fpath, frmt, [t.path for t in self.output()])


class ParseJavaScript(luigi.Task):

    url = luigi.Parameter(default='')
    name = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))


@requires(RetrieveWebDataFile)
class ParseWebExcelFile(luigi.Task):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))



@requires(RetrieveWebDataFileFromArchive)
class ParseWebExcelFileFromArchive(luigi.Task):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))


class GzipToFtp(luigi.Task):

    date = luigi.DateParameter(default=None)

    def output(self):
        _ftp_path = os.path.join(FTP_PATH, gziped_fname(self.input().path))
        return RemoteTarget(_ftp_path, FTP_HOST,
                            username=FTP_USER, password=FTP_PASS)

    def run(self):
        _fpath = gzip_file(self.input().path)
        self.output().put(_fpath, atomic=False)
