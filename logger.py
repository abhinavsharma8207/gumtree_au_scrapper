import logging


def log(message, criteria=None):
    log_format = "%(levelname)s %(asctime)s - %(message)s"

    logging.basicConfig(filename="logfile.log",
                        filemode="w",
                        format=log_format,
                        level=logging.DEBUG)

    logger = logging.getLogger()
    # Testing our Logger
    if criteria is None:
        logger.error(message)
    else:
        logger.debug(message)
