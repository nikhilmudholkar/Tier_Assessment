import logging


def get_logger(name):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name=name)

    try:
        handler = logging.FileHandler('/home/parallax/PycharmProjects/Tier_Assessment/app.log')
    except:
        handler = logging.FileHandler('app.log')

    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = get_logger(__name__)
