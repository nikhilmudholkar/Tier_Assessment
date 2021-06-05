import os
from ruamel.yaml import YAML
from typing import Dict
from utils.logger_util import get_logger

logger = get_logger(__name__)
class ConfigParser:
    config: Dict = None

    def __init__(self) -> None:
        try:
            yaml = YAML(typ='safe')
            config_filename = 'configurations.yml'
            with open(config_filename, 'r') as config_file:
                self.config = yaml.load(config_file)
                logger.info('configurations loaded successfully')
        except Exception as e:
            logger.info('configurations loading failed')
