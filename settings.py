import os
from tempfile import gettempdir

TMP_DIR = gettempdir()

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(WORK_DIR, 'config')


FTP_PATH = '/external_sources/http_test2/'
FTP_HOST = '10.8.36.51'
FTP_USER = 'ftpuser'
FTP_PASS = 'ftpuser'
