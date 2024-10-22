import logging
import os


def get_logger(log_name,log_file=None,level=logging.INFO):
    """
    Creates and returns a logger instance.

    Args:
        log_name (str): The name of the logger.
        log_file (str): The file path where logs should be write.
        level (int): The logging level.

    Returns:
        logging.Logger: Configured logger instance.
    """

    # Create a logger
    logger = logging.getLogger(log_name)
    logger.setLevel(level)

    # Create directory for log file if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # create file handler to write logs to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)

    # Create console handler to print logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Define log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    if not logger.handlers:  
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

