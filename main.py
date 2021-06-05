import glob
import time
import ast
from src.LoadJsons import LoadJsons
from config_parser import ConfigParser
from utils.logger_util import get_logger

logger = get_logger(__name__)

configs = ConfigParser().config
if __name__ == '__main__':
    files_list = glob.glob(f"{configs.get('data_dir')}*.json")
    logger.info(files_list)
    for file in files_list:
        start = time.time()
        logger.info(f"Data loading process started for {file}")

        json_obj = LoadJsons(file)
        json_obj.GetDf()

        if file.split('/')[-1] == 'weather.json':
            json_obj.ResolveDatetime(['date_time'])
            json_obj.RemoveNegativeValues(ast.literal_eval(configs.get('non_negative_columns')))

        if file.split('/')[-1] == 'track_events.json':
            json_obj.ResolveDatetime(ast.literal_eval(configs.get('date_columns')))
            json_obj.RemoveCorruptValues(ast.literal_eval(configs.get('date_columns')))

        json_obj.WriteDfJDBC()
        json_obj.WriteDfRepartition()

        json_obj.CreateCSVRepartition()
        json_obj.WriteDfCOPY()

        logger.info(f"Data loading process finished for {file}")
        end = time.time()
        logger.info(f"Total time taken to load = {end-start} seconds")
        # break


