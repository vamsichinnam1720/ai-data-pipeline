"""Central configuration management"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    UPLOAD_DIR = DATA_DIR / "uploads"
    BACKUP_DIR = DATA_DIR / "backups"
    DATABASE_DIR = DATA_DIR / "database"
    LOGS_DIR = BASE_DIR / "logs"
    CONFIG_DIR = BASE_DIR / "config"
    
    DATABASE_PATH = os.getenv("DATABASE_PATH", str(DATABASE_DIR / "pipeline.db"))
    
    WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "")
    STOCK_API_KEY = os.getenv("STOCK_API_KEY", "")
    CRYPTO_API_KEY = os.getenv("CRYPTO_API_KEY", "")
    
    API_CONFIG_PATH = CONFIG_DIR / "api_config.json"
    
    ANOMALY_Z_SCORE_THRESHOLD = float(os.getenv("ANOMALY_Z_SCORE_THRESHOLD", "3.0"))
    ANOMALY_IQR_MULTIPLIER = float(os.getenv("ANOMALY_IQR_MULTIPLIER", "1.5"))
    MISSING_VALUE_THRESHOLD = float(os.getenv("MISSING_VALUE_THRESHOLD", "0.2"))
    
    API_RETRY_ATTEMPTS = int(os.getenv("API_RETRY_ATTEMPTS", "3"))
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "10"))
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = LOGS_DIR / "pipeline.log"
    ANOMALY_LOG_FILE = LOGS_DIR / "anomaly.log"
    
    ENABLE_ALERTS = os.getenv("ENABLE_ALERTS", "true").lower() == "true"
    ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")
    
    MAX_MISSING_PERCENTAGE = 50
    OUTLIER_PERCENTILE_LOW = 1
    OUTLIER_PERCENTILE_HIGH = 99
    
    @classmethod
    def create_directories(cls):
        directories = [cls.DATA_DIR, cls.UPLOAD_DIR, cls.BACKUP_DIR, 
                      cls.DATABASE_DIR, cls.LOGS_DIR]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        for directory in [cls.UPLOAD_DIR, cls.BACKUP_DIR, cls.DATABASE_DIR, cls.LOGS_DIR]:
            gitkeep = directory / ".gitkeep"
            if not gitkeep.exists():
                gitkeep.touch()
    
    @classmethod
    def load_api_config(cls):
        if cls.API_CONFIG_PATH.exists():
            with open(cls.API_CONFIG_PATH, 'r') as f:
                return json.load(f)
        return {}
    
    @classmethod
    def validate_config(cls):
        cls.create_directories()
        return True
    
    @classmethod
    def get_summary(cls):
        return {
            "Database": str(cls.DATABASE_PATH),
            "Upload Directory": str(cls.UPLOAD_DIR),
            "Backup Directory": str(cls.BACKUP_DIR),
            "Log File": str(cls.LOG_FILE),
            "API Retry Attempts": cls.API_RETRY_ATTEMPTS,
            "Anomaly Z-Score Threshold": cls.ANOMALY_Z_SCORE_THRESHOLD,
            "Enable Alerts": cls.ENABLE_ALERTS
        }
