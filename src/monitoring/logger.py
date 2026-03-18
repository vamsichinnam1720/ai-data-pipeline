"""Logging system"""
import logging
import sys
from colorama import Fore, Style, init
from config.config import Config

init(autoreset=True)

class ColoredFormatter(logging.Formatter):
    COLORS = {'DEBUG': Fore.CYAN, 'INFO': Fore.GREEN, 'WARNING': Fore.YELLOW, 'ERROR': Fore.RED, 'CRITICAL': Fore.RED + Style.BRIGHT}
    
    def format(self, record):
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)

class Logger:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.logger = logging.getLogger('PipelineLogger')
        self.logger.setLevel(getattr(logging, Config.LOG_LEVEL))
        self.logger.handlers.clear()
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        
        Config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(Config.LOG_FILE)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def info(self, msg): self.logger.info(msg)
    def warning(self, msg): self.logger.warning(msg)
    def error(self, msg): self.logger.error(msg)
    def debug(self, msg): self.logger.debug(msg)
    def success(self, msg): self.logger.info(f"{Fore.GREEN}✓ {msg}{Style.RESET_ALL}")
    def section(self, title):
        sep = "=" * 60
        self.logger.info(f"\n{Fore.BLUE}{sep}\n{title}\n{sep}{Style.RESET_ALL}\n")

logger = Logger()
