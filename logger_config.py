import logging
from logging.handlers import RotatingFileHandler
import os

# ANSI color codes for different log levels
COLORS = {
    'DEBUG': '\033[0;36m',    # Cyan
    'INFO': '\033[0;32m',     # Green
    'WARNING': '\033[0;33m',  # Yellow
    'ERROR': '\033[0;31m',    # Red
    'CRITICAL': '\033[0;35m', # Magenta
    'RESET': '\033[0m'        # Reset color
}

class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors to log levels"""
    def format(self, record):
        # Save original levelname
        orig_levelname = record.levelname
        # Add color to levelname
        record.levelname = f"{COLORS.get(record.levelname, '')}{record.levelname}{COLORS['RESET']}"
        # Format the message
        result = super().format(record)
        # Restore original levelname
        record.levelname = orig_levelname
        return result

def setup_logger(config):
    """Setup application logger with rotation and color coding"""
    logger = logging.getLogger('congress_downloader')
    logger.setLevel(config['level'])

    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(config['file'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Setup rotating file handler with standard formatter (no colors in files)
    file_handler = RotatingFileHandler(
        config['file'],
        maxBytes=config.get('max_size', 10485760),  # Default 10MB
        backupCount=config.get('backup_count', 5)
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    # Setup console handler with colored formatter
    console_handler = logging.StreamHandler()
    console_formatter = ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Log startup message to verify formatting
    logger.info("Logger initialized with color coding and timestamps")

    return logger