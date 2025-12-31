"""
Logging configuration for IntelliChat
Automatically creates log files on first use
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
LOGS_DIR = PROJECT_ROOT / "data" / "logs"

# Create logs directory if it doesn't exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Log file paths
APP_LOG = LOGS_DIR / "app.log"
ERROR_LOG = LOGS_DIR / "error.log"
API_LOG = LOGS_DIR / "api.log"

# Log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logger(
    name,
    log_file,
    level=logging.INFO,
    max_bytes=10*1024*1024,  # 10MB
    backup_count=5
):
    """
    Setup a logger with file and console handlers
    
    Args:
        name: Logger name
        log_file: Path to log file
        level: Logging level
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatters
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def setup_error_logger():
    """Setup logger specifically for errors"""
    error_logger = logging.getLogger("intellichat.error")
    error_logger.setLevel(logging.ERROR)
    
    # Clear existing handlers
    error_logger.handlers.clear()
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt=DATE_FORMAT
    )
    
    # Error file handler
    error_handler = RotatingFileHandler(
        ERROR_LOG,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    error_logger.addHandler(error_handler)
    
    return error_logger

def setup_api_logger():
    """Setup logger for API requests"""
    api_logger = logging.getLogger("intellichat.api")
    api_logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    api_logger.handlers.clear()
    
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt=DATE_FORMAT
    )
    
    # API file handler
    api_handler = RotatingFileHandler(
        API_LOG,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    api_handler.setLevel(logging.INFO)
    api_handler.setFormatter(formatter)
    
    api_logger.addHandler(api_handler)
    
    return api_logger

# Initialize loggers
app_logger = setup_logger("intellichat", APP_LOG)
error_logger = setup_error_logger()
api_logger = setup_api_logger()

def get_logger(name=None):
    """
    Get a logger instance
    
    Args:
        name: Logger name (default: "intellichat")
    
    Returns:
        logging.Logger: Logger instance
    """
    if name:
        return logging.getLogger(f"intellichat.{name}")
    return app_logger

def log_exception(exc):
    """
    Log an exception with full traceback
    
    Args:
        exc: Exception object
    """
    error_logger.exception(f"Exception occurred: {exc}")

def log_api_request(method, endpoint, status_code, response_time):
    """
    Log an API request
    
    Args:
        method: HTTP method
        endpoint: API endpoint
        status_code: Response status code
        response_time: Response time in milliseconds
    """
    api_logger.info(
        f"{method} {endpoint} - {status_code} - {response_time:.2f}ms"
    )

# Log startup message
def log_startup():
    """Log application startup"""
    app_logger.info("="*60)
    app_logger.info("IntelliChat Starting")
    app_logger.info(f"Timestamp: {datetime.now().isoformat()}")
    app_logger.info(f"Logs Directory: {LOGS_DIR}")
    app_logger.info("="*60)

# Example usage and testing
if __name__ == "__main__":
    # Test logging
    log_startup()
    
    logger = get_logger()
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    
    error_logger.error("This is an error message")
    
    log_api_request("GET", "/api/chat", 200, 145.5)
    
    try:
        # Simulate an error
        raise ValueError("Test exception")
    except Exception as e:
        log_exception(e)
    
    print(f"\n✓ Logs created in: {LOGS_DIR}")
    print(f"  - {APP_LOG.name}")
    print(f"  - {ERROR_LOG.name}")
    print(f"  - {API_LOG.name}")