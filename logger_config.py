import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logger(config):
    """Setup application logger with rotation"""
    logger = logging.getLogger('congress_downloader')
    logger.setLevel(config['level'])

    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(config['file'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Setup rotating file handler
    file_handler = RotatingFileHandler(
        config['file'],
        maxBytes=config['max_size'],
        backupCount=config['backup_count']
    )

    # Setup console handler
    console_handler = logging.StreamHandler()

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Apply formatter to handlers
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
