import logging

def log(message: str, level: str, logger: logging.Logger = None):
    """
    Log a message at the specified level.

    level:
      - "debug"
      - "info"
      - "warning"
      - "error"
      - "critical"
    """

    if logger is None:
        print(f"{level.upper()}: {message}")
    else:
        if level == "debug":
            logger.debug(message)
        elif level == "info":
            logger.info(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
        elif level == "critical":
            logger.critical(message)
        else:
            raise ValueError(f"Unknown log level: {level}")