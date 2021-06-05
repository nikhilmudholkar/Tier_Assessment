import psycopg2
from config_parser import ConfigParser
from utils.logger_util import get_logger

logger = get_logger(__name__)

configs = ConfigParser().config


def GetPostgresConnection():
    con = psycopg2.connect(database=configs.get('database'),
                           user=configs.get('username'),
                           password=configs.get('password'),
                           host=configs.get('host'),
                           port=configs.get('port'))
    return con
