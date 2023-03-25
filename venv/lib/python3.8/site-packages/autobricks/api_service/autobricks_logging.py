import logging
import os


def get_logger(name: str = "root"):

    autobricks_logging = os.getenv("AUTOBRICKS_LOGGING")

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=autobricks_logging,
    )
    logger = logging.getLogger(name)
    return logger
