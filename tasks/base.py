from enum import Enum
import os
from datetime import datetime
from pathlib import Path

import attr
import luigi
from luigi.contrib.ftp import RemoteTarget

from tcomextdata.common.dates import PreviousPeriod, FILENAME_DATE_FORMAT, today
from tcomextdata.common.utils import (replace_fext, gzip_file,
                                      get_yaml_task_config)

from settings import (FTP_PATH, FTP_HOST, FTP_USER,
                      FTP_PASS, DATA_PATH)


@attr.s
class DataList(object):
    data = attr.ib()


class Period(Enum):
    day = 1
    week = 2
    month = 3


class BaseTask(luigi.Task):
    """ Base root class for all tasks"""

    def set_status(self, status: str, percent: int):
        self.set_status_message(status)
        self.set_progress_percentage(percent)

    name = luigi.Parameter(default='')
    descr = luigi.Parameter(default='')
    struct = luigi.Parameter(default=None)


class LoadingDataIntoFile(BaseTask):
    """ Base class for tasks that loads data into file"""
    directory = luigi.Parameter(default=DATA_PATH)

    def fpath(self, ext, suff=None):
        d = Path(self.directory)
        stem = '_'.join([self.name, suff]) if suff else self.name
        return d.joinpath(stem).with_suffix(ext)


class LoadingDataIntoCsvFile(LoadingDataIntoFile):
    """ Base class for tasks which intended to save data CSV files """
    ext = luigi.Parameter(default='.csv')
    sep = luigi.Parameter(default=';')
    suff = luigi.Parameter(default=today())

    @property
    def csv_fpath(self):
        return self.fpath(self.ext, suff=self.suff)

    def output(self):
        directory = str(self.directory)
        fname = self.fpath(self.ext, suff=self.suff)
        return luigi.LocalTarget(os.path.join(directory, fname))

    def run(self):
        open(self.output().path, 'w', encoding="utf-8").close()


class GzipAndUploadToFtp(luigi.Task):

    ftp_dir = luigi.Parameter(default='')
    period = luigi.Parameter(default=PreviousPeriod.previous_day)
    ftp_host = luigi.Parameter(default=FTP_HOST)
    ftp_user = luigi.Parameter(default=FTP_USER, significant=False)
    ftp_pass = luigi.Parameter(default=FTP_PASS, significant=False)
    ftp_path = luigi.Parameter(default=FTP_PATH)
    ftp_fs_sep = luigi.Parameter(default='/', significant=False)
    gzext = luigi.Parameter(default='.gzip', significant=False)

    def ftp_directory(self):

        # custom directory on ftp specified
        if self.ftp_dir:
            path = self.ftp_fs_sep.join([self.ftp_path, self.ftp_dir])
        else:
            path = self.ftp_path

        return path

    def output(self):
        fname = replace_fext(self.input().path, self.gzext).name
        ftp_path = self.ftp_fs_sep.join([self.ftp_directory(), fname])
        return RemoteTarget(ftp_path, self.ftp_host,
                            username=self.ftp_user, password=self.ftp_pass)

    def run(self):
        gz_fpath = gzip_file(self.input().path, self.gzext)
        self.output().put(gz_fpath, atomic=False)
        os.remove(self.input().path)


class JavaScriptParsingToCsv(LoadingDataIntoCsvFile):

    url = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='')


class BigDataToCsv(LoadingDataIntoCsvFile):
    """
    Base class for tasks which are supposed to process big data piece by
    piece and be rerun if it hasn't done work last time.
    """
    # how long to wait between requests(tries) if we deal
    # with come restrictions from server side
    timeout = luigi.IntParameter(default=0)

    # extension of file with data that helps resume process
    parsed_fext = luigi.Parameter(default='.prs')
    # extension of file with statistic information
    success_fext = luigi.Parameter(default='.scs')

    @property
    def success_fpath(self):
        """
        If file exists that means task succesfully completed
        otherwise due some reason it was interuppated.
        """
        return replace_fext(self.csv_fpath, self.success_fext)

    @property
    def parsed_fpath(self):
        return replace_fext(self.csv_fpath, self.parsed_fext)

    def progress(self, status, percent):
        """
        Use it as callback when you need to see progress of process
        with additional information.
        """
        self.set_status_message(status)
        self.set_progress_percentage(percent)

    def complete(self):
        if not os.path.exists(self.success_fpath):
            return False
        else:
            return True


class Runner(luigi.WrapperTask):
    """
    Base class for tasks which are supposed only to prepare
    all input parameters and run tasks with main functionality.
    """
    date = luigi.DateParameter(default=datetime.today())
    period = luigi.EnumParameter(enum=Period, default=Period.day)
    delta_days = luigi.IntParameter(default=1)
    name = luigi.Parameter()

    @property
    def params(self):
        """
            Load configuration as dict and return section according given name.
        """
        params = get_yaml_task_config(self.name)
        params['name'] = self.name
        return params

    @property
    def suff(self):
        return self.date.strftime(FILENAME_DATE_FORMAT)
