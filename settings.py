import os

from dotenv import load_dotenv

load_dotenv()


FTP_PATH = os.getenv('FTP_PATH')
FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')

KGD_API_TOKEN = os.getenv('KGD_API_TOKEN')
DGOV_API_KEY = os.getenv('DGOV_API_KEY')
GOSZAKUP_GQL_TOKEN = os.getenv('GOSZAKUP_GQL_TOKEN')
GOSZAKUP_REST_TOKEN = os.getenv('GOSZAKUP_REST_TOKEN')

DATA_PATH = os.getenv('DATA_PATH')
TEMP_PATH = os.getenv('TEMP_PATH')

TASKS_PARAMS_CONFIG_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                        'config', 'tasks.yml')

