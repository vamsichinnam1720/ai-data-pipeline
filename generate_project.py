"""
AI Data Pipeline - Complete Project Generator
Run this script to generate all missing project files
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

def create_file(filepath, content):
    """Create a file with given content"""
    full_path = BASE_DIR / filepath
    full_path.parent.mkdir(parents=True, exist_ok=True)
    with open(full_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✓ Created: {filepath}")

def generate_all_files():
    """Generate all project files"""
    
    print("\n🚀 Generating AI Data Pipeline Project Files...\n")
    
    # ============================================
    # ROOT CONFIGURATION FILES
    # ============================================
    
    create_file("requirements.txt", """pandas==2.1.4
numpy==1.26.2
requests==2.31.0
matplotlib==3.8.2
seaborn==0.13.0
plotly==5.18.0
scikit-learn==1.3.2
scipy==1.11.4
pyspellchecker==0.8.1
fuzzywuzzy==0.18.0
python-Levenshtein==0.23.0
python-dotenv==1.0.0
colorama==0.4.6
rich==13.7.0
tqdm==4.66.1
tabulate==0.9.0
""")

    create_file(".env.example", """# API Keys
WEATHER_API_KEY=your_openweathermap_api_key_here
STOCK_API_KEY=your_alphavantage_api_key_here
CRYPTO_API_KEY=your_coingecko_api_key_here

# Database Configuration
DATABASE_PATH=data/database/pipeline.db

# Paths
UPLOAD_DIR=data/uploads
BACKUP_DIR=data/backups

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/pipeline.log

# Anomaly Detection
ANOMALY_Z_SCORE_THRESHOLD=3.0
ANOMALY_IQR_MULTIPLIER=1.5
MISSING_VALUE_THRESHOLD=0.2

# API Configuration
API_RETRY_ATTEMPTS=3
API_TIMEOUT=10

# Monitoring
ENABLE_ALERTS=true
""")

    create_file(".gitignore", """__pycache__/
*.py[cod]
venv/
env/
.env
.vscode/
data/uploads/*
!data/uploads/.gitkeep
data/backups/*
data/database/*.db
logs/*.log
.DS_Store
""")

    # ============================================
    # SRC INIT
    # ============================================
    
    create_file("src/__init__.py", '''"""AI Data Engineering Pipeline"""
__version__ = '1.0.0'
''')

    # ============================================
    # CONFIG MODULE
    # ============================================
    
    create_file("config/__init__.py", '''"""Configuration module"""
from .config import Config
__all__ = ['Config']
''')

    create_file("config/config.py", '''"""Central configuration management"""
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
''')

    create_file("config/api_config.json", """{
  "weather": {
    "openweathermap": {
      "base_url": "https://api.openweathermap.org/data/2.5/weather",
      "requires_key": true,
      "params": {"q": "{city}", "appid": "{api_key}", "units": "metric"}
    }
  },
  "stock": {
    "alphavantage": {
      "base_url": "https://www.alphavantage.co/query",
      "requires_key": true,
      "params": {"function": "TIME_SERIES_DAILY", "symbol": "{symbol}", "apikey": "{api_key}"}
    }
  },
  "crypto": {
    "coingecko": {
      "base_url": "https://api.coingecko.com/api/v3/simple/price",
      "requires_key": false,
      "params": {"ids": "{coin_id}", "vs_currencies": "usd"}
    }
  }
}
""")

    # ============================================
    # DATABASE MODULE
    # ============================================
    
    create_file("src/database/__init__.py", '''"""Database module"""
from .db_manager import DatabaseManager
from .schema import Schema
__all__ = ['DatabaseManager', 'Schema']
''')

    create_file("src/database/schema.py", '''"""Database schema definitions"""

class Schema:
    CREATE_DATA_TABLE = """
    CREATE TABLE IF NOT EXISTS data_{table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {columns},
        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source TEXT DEFAULT 'csv',
        is_anomaly INTEGER DEFAULT 0
    )
    """
    
    CREATE_METADATA_TABLE = """
    CREATE TABLE IF NOT EXISTS metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT NOT NULL,
        total_rows INTEGER,
        total_columns INTEGER,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_source TEXT DEFAULT 'csv',
        column_info TEXT
    )
    """
    
    CREATE_QUERY_HISTORY_TABLE = """
    CREATE TABLE IF NOT EXISTS query_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query_text TEXT NOT NULL,
        corrected_query TEXT,
        execution_time REAL,
        result_count INTEGER,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    @staticmethod
    def get_column_definition(dtype):
        dtype_str = str(dtype).lower()
        if 'int' in dtype_str:
            return 'INTEGER'
        elif 'float' in dtype_str:
            return 'REAL'
        elif 'datetime' in dtype_str or 'date' in dtype_str:
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    @staticmethod
    def create_table_from_dataframe(df, table_name):
        columns = []
        for col in df.columns:
            col_type = Schema.get_column_definition(df[col].dtype)
            safe_col = col.replace(' ', '_').replace('-', '_').lower()
            columns.append(f"{safe_col} {col_type}")
        columns_str = ',\\n        '.join(columns)
        return Schema.CREATE_DATA_TABLE.format(table_name=table_name, columns=columns_str)
    
    @staticmethod
    def get_all_table_schemas():
        return [Schema.CREATE_METADATA_TABLE, Schema.CREATE_QUERY_HISTORY_TABLE]
''')

    create_file("src/database/db_manager.py", '''"""Database manager"""
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict
import json

from config.config import Config
from .schema import Schema

class DatabaseManager:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self._initialize_database()
    
    def _initialize_database(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self.get_connection() as conn:
            for schema in Schema.get_all_table_schemas():
                conn.execute(schema)
            conn.commit()
    
    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            columns = [description[0] for description in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return results
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        try:
            create_stmt = Schema.create_table_from_dataframe(df, table_name)
            with self.get_connection() as conn:
                conn.execute(create_stmt)
                conn.commit()
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, source: str = 'csv') -> bool:
        try:
            df_copy = df.copy()
            df_copy['source'] = source
            df_copy['is_anomaly'] = 0
            df_copy.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df_copy.columns]
            
            with self.get_connection() as conn:
                df_copy.to_sql(f'data_{table_name}', conn, if_exists='append', index=False)
                conn.commit()
            
            self._log_metadata(table_name, df, source)
            return True
        except Exception as e:
            print(f"Error inserting data: {e}")
            return False
    
    def get_dataframe(self, table_name: str, filters: Optional[Dict] = None) -> pd.DataFrame:
        try:
            query = f"SELECT * FROM data_{table_name}"
            if filters:
                conditions = [f"{k} = ?" for k in filters.keys()]
                query += " WHERE " + " AND ".join(conditions)
                params = tuple(filters.values())
            else:
                params = ()
            
            with self.get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params)
            return df
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return pd.DataFrame()
    
    def _log_metadata(self, table_name: str, df: pd.DataFrame, source: str):
        column_info = {col: {'dtype': str(df[col].dtype)} for col in df.columns}
        query = "INSERT INTO metadata (table_name, total_rows, total_columns, data_source, column_info) VALUES (?, ?, ?, ?, ?)"
        self.execute_update(query, (table_name, len(df), len(df.columns), source, json.dumps(column_info)))
    
    def log_query(self, query_text: str, corrected_query: Optional[str] = None, 
                  execution_time: float = 0.0, result_count: int = 0):
        query = "INSERT INTO query_history (query_text, corrected_query, execution_time, result_count) VALUES (?, ?, ?, ?)"
        self.execute_update(query, (query_text, corrected_query, execution_time, result_count))
    
    def get_table_list(self) -> List[str]:
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'data_%'"
        results = self.execute_query(query)
        return [row['name'].replace('data_', '') for row in results]
''')

    # ============================================
    # MONITORING MODULE  
    # ============================================
    
    create_file("src/monitoring/__init__.py", '''"""Monitoring module"""
from .logger import Logger, logger
from .alerter import Alerter, alerter
__all__ = ['Logger', 'logger', 'Alerter', 'alerter']
''')

    create_file("src/monitoring/logger.py", '''"""Logging system"""
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
        self.logger.info(f"\\n{Fore.BLUE}{sep}\\n{title}\\n{sep}{Style.RESET_ALL}\\n")

logger = Logger()
''')

    create_file("src/monitoring/alerter.py", '''"""Alert system"""
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from .logger import logger

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

class AlertType(Enum):
    ANOMALY_DETECTED = "anomaly_detected"
    API_FAILURE = "api_failure"

class Alerter:
    def __init__(self):
        self.enabled = True
        self.alert_history = []
    
    def anomaly_alert(self, anomaly_count: int, anomaly_types: Dict[str, int], auto_fixed: int):
        logger.warning(f"Detected {anomaly_count} anomalies ({auto_fixed} auto-fixed)")

alerter = Alerter()
''')

    # ============================================
    # PROCESSING MODULE
    # ============================================
    
    create_file("src/processing/__init__.py", '''"""Processing module"""
from .cleaner import DataCleaner
from .validator import DataValidator
__all__ = ['DataCleaner', 'DataValidator']
''')

    create_file("src/processing/cleaner.py", '''"""Data cleaning"""
import pandas as pd
import re
from config.config import Config
from src.monitoring.logger import logger

class DataCleaner:
    def __init__(self):
        self.cleaning_report = {'duplicates_removed': 0, 'missing_values_handled': 0, 'columns_cleaned': 0}
    
    def clean_data(self, df: pd.DataFrame, auto_fix: bool = True):
        logger.info("Starting data cleaning...")
        df_cleaned = df.copy()
        df_cleaned = self._clean_column_names(df_cleaned)
        df_cleaned = self._remove_duplicates(df_cleaned)
        if auto_fix:
            df_cleaned = self._handle_missing_values(df_cleaned)
        logger.success(f"Cleaning complete: {self.cleaning_report}")
        return df_cleaned, self.cleaning_report
    
    def _clean_column_names(self, df):
        cleaned = [re.sub(r'[^a-z0-9_]', '_', col.lower()).strip('_') for col in df.columns]
        df.columns = cleaned
        return df
    
    def _remove_duplicates(self, df):
        original_len = len(df)
        df_clean = df.drop_duplicates()
        removed = original_len - len(df_clean)
        if removed > 0:
            self.cleaning_report['duplicates_removed'] = removed
            logger.warning(f"Removed {removed} duplicates")
        return df_clean
    
    def _handle_missing_values(self, df):
        total_missing = df.isnull().sum().sum()
        if total_missing == 0:
            return df
        logger.info(f"Handling {total_missing} missing values...")
        for col in df.columns:
            if df[col].isnull().sum() == 0:
                continue
            if df[col].dtype in ['int64', 'float64']:
                df[col].fillna(df[col].median(), inplace=True)
            else:
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col].fillna(mode_val[0], inplace=True)
        self.cleaning_report['missing_values_handled'] = total_missing
        return df
''')

    create_file("src/processing/validator.py", '''"""Data validation"""
import pandas as pd
from src.monitoring.logger import logger

class DataValidator:
    def __init__(self):
        self.validation_results = {'is_valid': True, 'errors': [], 'warnings': [], 'quality_score': 100.0}
    
    def validate(self, df: pd.DataFrame):
        logger.info("Validating data...")
        self.validation_results = {'is_valid': True, 'errors': [], 'warnings': [], 'quality_score': 100.0}
        
        if df.empty:
            self.validation_results['errors'].append("DataFrame is empty")
            self.validation_results['is_valid'] = False
        
        missing = df.isnull().sum().sum()
        if missing > 0:
            pct = (missing / (len(df) * len(df.columns))) * 100
            self.validation_results['warnings'].append(f"Missing values: {missing} ({pct:.1f}%)")
        
        dups = df.duplicated().sum()
        if dups > 0:
            self.validation_results['warnings'].append(f"Duplicates: {dups}")
        
        score = 100.0
        score -= min((missing / (len(df) * len(df.columns))) * 100, 30)
        score -= min((dups / len(df)) * 100, 20)
        score -= len(self.validation_results['errors']) * 10
        score -= len(self.validation_results['warnings']) * 2
        self.validation_results['quality_score'] = max(score, 0)
        
        logger.success(f"Validation complete. Score: {self.validation_results['quality_score']:.1f}/100")
        return self.validation_results
''')

    # ============================================
    # INGESTION MODULE
    # ============================================
    
    create_file("src/ingestion/__init__.py", '''"""Ingestion module"""
from .csv_loader import CSVLoader
from .api_connector import APIConnector
from .fallback_manager import FallbackManager
__all__ = ['CSVLoader', 'APIConnector', 'FallbackManager']
''')

    create_file("src/ingestion/csv_loader.py", '''"""CSV loader"""
import pandas as pd
from pathlib import Path
import shutil
from config.config import Config
from src.monitoring.logger import logger

class CSVLoader:
    def __init__(self):
        self.current_file = None
        self.backup_file = None
    
    def load_csv(self, filepath: str, create_backup: bool = True):
        try:
            logger.info(f"Loading CSV: {filepath}")
            df = pd.read_csv(filepath)
            self.current_file = filepath
            
            if create_backup:
                filename = Path(filepath).name
                backup_path = Config.BACKUP_DIR / filename
                shutil.copy2(filepath, backup_path)
                self.backup_file = str(backup_path)
                logger.info(f"Backup created: {backup_path}")
            
            logger.success(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return None
    
    def get_backup(self):
        if self.backup_file and Path(self.backup_file).exists():
            return self.load_csv(self.backup_file, create_backup=False)
        return None
''')

    create_file("src/ingestion/api_connector.py", '''"""API connector"""
import requests
import pandas as pd
import time
from config.config import Config
from src.monitoring.logger import logger

class APIConnector:
    def __init__(self):
        self.api_config = Config.load_api_config()
    
    def fetch_weather_data(self, city: str, api_key=None):
        api_key = api_key or Config.WEATHER_API_KEY
        if not api_key:
            logger.error("Weather API key not configured")
            return None
        
        config = self.api_config.get('weather', {}).get('openweathermap', {})
        url = config.get('base_url')
        params = {'q': city, 'appid': api_key, 'units': 'metric'}
        
        return self._make_request('OpenWeatherMap', url, params)
    
    def _make_request(self, api_name: str, url: str, params: dict):
        for attempt in range(Config.API_RETRY_ATTEMPTS):
            try:
                logger.info(f"API call to {api_name} (attempt {attempt + 1})")
                start = time.time()
                response = requests.get(url, params=params, timeout=Config.API_TIMEOUT)
                elapsed = time.time() - start
                
                if response.status_code == 200:
                    logger.success(f"API call successful ({elapsed:.2f}s)")
                    return pd.DataFrame([response.json()])
                else:
                    logger.warning(f"API returned {response.status_code}")
            except Exception as e:
                logger.error(f"API call failed: {e}")
        return None
''')

    create_file("src/ingestion/fallback_manager.py", '''"""Fallback manager"""
from src.monitoring.logger import logger
from src.monitoring.alerter import alerter
from .csv_loader import CSVLoader
from .api_connector import APIConnector

class FallbackManager:
    def __init__(self):
        self.csv_loader = CSVLoader()
        self.api_connector = APIConnector()
        self.current_source = 'csv'
        self.fallback_active = False
    
    def get_data(self, mode: str = 'csv', **kwargs):
        if mode == 'api':
            return self._get_with_api_fallback(**kwargs)
        else:
            return self._get_from_csv(**kwargs)
    
    def _get_with_api_fallback(self, **kwargs):
        api_type = kwargs.get('api_type', 'weather')
        
        if api_type == 'weather':
            df = self.api_connector.fetch_weather_data(kwargs.get('city', 'London'))
        else:
            df = None
        
        if df is None:
            logger.warning("API failed, switching to CSV backup")
            self.fallback_active = True
            self.current_source = 'csv'
            df = self.csv_loader.get_backup()
            if df is not None:
                logger.success("Fallback successful")
            else:
                logger.error("No backup available")
        else:
            self.current_source = 'api'
            self.fallback_active = False
        
        return df
    
    def _get_from_csv(self, **kwargs):
        filepath = kwargs.get('filepath')
        if filepath:
            return self.csv_loader.load_csv(filepath)
        return None
''')

    # ============================================
    # INTELLIGENCE MODULE
    # ============================================
    
    create_file("src/intelligence/__init__.py", '''"""Intelligence module"""
from .anomaly_detector import AnomalyDetector
from .anomaly_fixer import AnomalyFixer
__all__ = ['AnomalyDetector', 'AnomalyFixer']
''')

    create_file("src/intelligence/anomaly_detector.py", '''"""Anomaly detection"""
import pandas as pd
import numpy as np
from scipy import stats
from config.config import Config
from src.monitoring.logger import logger

class AnomalyDetector:
    def __init__(self):
        self.detection_summary = {'outliers': 0, 'missing_patterns': 0}
    
    def detect_all(self, df: pd.DataFrame):
        logger.info("Detecting anomalies...")
        df_flagged = df.copy()
        df_flagged['_anomaly_flag'] = 0
        
        outlier_mask = self._detect_outliers(df)
        df_flagged.loc[outlier_mask, '_anomaly_flag'] = 1
        
        total = df_flagged['_anomaly_flag'].sum()
        logger.warning(f"Detected {total} anomalies")
        
        return df_flagged, self.detection_summary
    
    def _detect_outliers(self, df):
        outlier_mask = np.zeros(len(df), dtype=bool)
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            z_scores = np.abs(stats.zscore(df[col].dropna()))
            z_outliers = z_scores > Config.ANOMALY_Z_SCORE_THRESHOLD
            
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            iqr_outliers = (df[col] < Q1 - Config.ANOMALY_IQR_MULTIPLIER * IQR) | (df[col] > Q3 + Config.ANOMALY_IQR_MULTIPLIER * IQR)
            
            valid_indices = df[col].notna()
            col_outliers = z_outliers | iqr_outliers[df[col].notna()]
            outlier_mask[valid_indices] |= col_outliers.values
            
            count = col_outliers.sum()
            if count > 0:
                self.detection_summary['outliers'] += count
                logger.debug(f"Found {count} outliers in '{col}'")
        
        return outlier_mask
''')

    create_file("src/intelligence/anomaly_fixer.py", '''"""Anomaly fixer"""
import pandas as pd
import numpy as np
from src.monitoring.logger import logger
from src.monitoring.alerter import alerter

class AnomalyFixer:
    def __init__(self):
        self.fix_log = []
    
    def fix_anomalies(self, df: pd.DataFrame, anomaly_flags):
        logger.info("Auto-fixing anomalies...")
        df_fixed = df.copy()
        fixed_count = 0
        
        df_fixed, outlier_fixes = self._fix_outliers(df_fixed, anomaly_flags)
        fixed_count += outlier_fixes
        
        logger.success(f"Auto-fixed {fixed_count} anomalies")
        
        if anomaly_flags.sum() > 0:
            alerter.anomaly_alert(anomaly_flags.sum(), {'outliers': outlier_fixes}, fixed_count)
        
        return df_fixed
    
    def _fix_outliers(self, df, anomaly_flags):
        df_fixed = df.copy()
        fixed = 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            outlier_idx = anomaly_flags & df[col].notna()
            if outlier_idx.sum() == 0:
                continue
            
            lower = df[col].quantile(0.01)
            upper = df[col].quantile(0.99)
            
            original = df.loc[outlier_idx, col].copy()
            df_fixed.loc[outlier_idx, col] = df_fixed.loc[outlier_idx, col].clip(lower, upper)
            
            fixes = (original != df_fixed.loc[outlier_idx, col]).sum()
            fixed += fixes
            
            if fixes > 0:
                logger.debug(f"Fixed {fixes} outliers in '{col}'")
        
        return df_fixed, fixed
''')

    # ============================================
    # NLP MODULE
    # ============================================
    
    create_file("src/nlp/__init__.py", '''"""NLP module"""
from .query_parser import QueryParser
from .grammar_corrector import GrammarCorrector
from .query_executor import QueryExecutor
__all__ = ['QueryParser', 'GrammarCorrector', 'QueryExecutor']
''')

    create_file("src/nlp/query_parser.py", '''"""Query parser"""
import re
from fuzzywuzzy import process
from src.monitoring.logger import logger

class QueryParser:
    AGGREGATIONS = ['sum', 'total', 'count', 'average', 'mean', 'max', 'min']
    
    def __init__(self, df_columns):
        self.columns = df_columns
    
    def parse(self, query: str):
        query_lower = query.lower()
        parsed = {'original': query, 'operation': 'select', 'columns': [], 'group_by': None}
        
        for agg in self.AGGREGATIONS:
            if agg in query_lower:
                parsed['operation'] = 'aggregate'
                parsed['aggregation_type'] = agg
                break
        
        words = query_lower.split()
        for word in words:
            clean = re.sub(r'[^a-z0-9_]', '', word)
            if len(clean) < 3:
                continue
            match = process.extractOne(clean, self.columns, score_cutoff=70)
            if match and match[0] not in parsed['columns']:
                parsed['columns'].append(match[0])
        
        for keyword in ['by', 'per']:
            if keyword in query_lower:
                idx = query_lower.find(keyword)
                rest = query_lower[idx + len(keyword):].strip().split()
                if rest:
                    match = process.extractOne(rest[0], self.columns, score_cutoff=70)
                    if match:
                        parsed['group_by'] = match[0]
                        break
        
        logger.info(f"Parsed query: {parsed}")
        return parsed
''')

    create_file("src/nlp/grammar_corrector.py", '''"""Grammar corrector"""
from pyspellchecker import SpellChecker
import re
from src.monitoring.logger import logger

class GrammarCorrector:
    def __init__(self):
        self.spell = SpellChecker()
        self.spell.word_frequency.load_words(['sum', 'average', 'count', 'group', 'total'])
    
    def correct(self, query: str):
        words = query.split()
        corrected_words = []
        corrections = []
        
        for word in words:
            if re.search(r'[0-9]', word) or len(word) < 2:
                corrected_words.append(word)
                continue
            
            clean = re.sub(r'[^a-zA-Z]', '', word).lower()
            if clean and clean not in self.spell:
                corrected = self.spell.correction(clean)
                if corrected and corrected != clean:
                    corrected_words.append(corrected)
                    corrections.append({'original': clean, 'corrected': corrected})
                else:
                    corrected_words.append(word)
            else:
                corrected_words.append(word)
        
        corrected_query = ' '.join(corrected_words)
        if corrections:
            logger.info(f"Applied {len(corrections)} corrections")
        return corrected_query, corrections
''')

    create_file("src/nlp/query_executor.py", '''"""Query executor"""
import pandas as pd
from src.monitoring.logger import logger

class QueryExecutor:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def execute(self, parsed_query: dict):
        result = self.df.copy()
        
        if parsed_query.get('group_by') and parsed_query.get('operation') == 'aggregate':
            result = self._apply_aggregation(
                result, 
                parsed_query['group_by'],
                parsed_query.get('aggregation_type', 'sum'),
                parsed_query.get('columns', [])
            )
        elif parsed_query.get('columns'):
            valid_cols = [c for c in parsed_query['columns'] if c in result.columns]
            if valid_cols:
                result = result[valid_cols]
        
        logger.info(f"Query returned {len(result)} rows")
        return result
    
    def _apply_aggregation(self, df, group_col: str, agg_type: str, agg_cols: list):
        if group_col not in df.columns:
            return df
        
        agg_func_map = {'sum': 'sum', 'total': 'sum', 'average': 'mean', 'mean': 'mean', 
                       'count': 'count', 'max': 'max', 'min': 'min'}
        agg_func = agg_func_map.get(agg_type, 'sum')
        
        if not agg_cols:
            agg_cols = df.select_dtypes(include=['number']).columns.tolist()
            agg_cols = [c for c in agg_cols if c != group_col]
        
        if not agg_cols:
            return df.groupby(group_col).size().reset_index(name='count')
        
        result = df.groupby(group_col)[agg_cols].agg(agg_func).reset_index()
        return result
''')

    # ============================================
    # ANALYTICS MODULE
    # ============================================
    
    create_file("src/analytics/__init__.py", '''"""Analytics module"""
from .statistics import StatisticsAnalyzer
from .visualizer import Visualizer
__all__ = ['StatisticsAnalyzer', 'Visualizer']
''')

    create_file("src/analytics/statistics.py", '''"""Statistics analyzer"""
import pandas as pd
import numpy as np

class StatisticsAnalyzer:
    def get_summary(self, df: pd.DataFrame):
        summary = {
            'shape': df.shape,
            'columns': df.columns.tolist(),
            'missing_values': df.isnull().sum().to_dict(),
            'numeric_summary': {},
            'categorical_summary': {}
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            summary['numeric_summary'] = df[numeric_cols].describe().to_dict()
        
        cat_cols = df.select_dtypes(include=['object']).columns
        for col in cat_cols:
            summary['categorical_summary'][col] = {
                'unique_count': df[col].nunique(),
                'top_values': df[col].value_counts().head(5).to_dict()
            }
        
        return summary
    
    def get_correlations(self, df: pd.DataFrame):
        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            return pd.DataFrame()
        return numeric_df.corr()
''')

    create_file("src/analytics/visualizer.py", '''"""Visualizer"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from config.config import Config
from src.monitoring.logger import logger

sns.set_style('whitegrid')

class Visualizer:
    def __init__(self):
        self.output_dir = Config.DATA_DIR / 'visualizations'
        self.output_dir.mkdir(exist_ok=True)
    
    def plot_distribution(self, df: pd.DataFrame, column: str, save: bool = True):
        if column not in df.columns:
            logger.error(f"Column '{column}' not found")
            return None
        
        plt.figure(figsize=(10, 6))
        
        if df[column].dtype in ['int64', 'float64']:
            sns.histplot(df[column].dropna(), kde=True)
            plt.title(f'Distribution of {column}')
        else:
            df[column].value_counts().plot(kind='bar')
            plt.title(f'Count of {column}')
        
        plt.xlabel(column)
        plt.ylabel('Frequency')
        plt.tight_layout()
        
        if save:
            filepath = self.output_dir / f'{column}_distribution.png'
            plt.savefig(filepath)
            plt.close()
            logger.info(f"Saved plot: {filepath}")
            return str(filepath)
        else:
            plt.show()
            return None
    
    def plot_correlation_heatmap(self, df: pd.DataFrame, save: bool = True):
        numeric_df = df.select_dtypes(include=['number'])
        if numeric_df.empty:
            logger.warning("No numeric columns")
            return None
        
        plt.figure(figsize=(12, 8))
        corr = numeric_df.corr()
        sns.heatmap(corr, annot=True, fmt='.2f', cmap='coolwarm', center=0)
        plt.title('Correlation Heatmap')
        plt.tight_layout()
        
        if save:
            filepath = self.output_dir / 'correlation_heatmap.png'
            plt.savefig(filepath)
            plt.close()
            logger.info(f"Saved plot: {filepath}")
            return str(filepath)
        else:
            plt.show()
            return None
''')

    # ============================================
    # MAIN APPLICATION
    # ============================================
    
    create_file("main.py", '''"""
AI Data Engineering Pipeline - Main Application
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from config.config import Config
from src.database.db_manager import DatabaseManager
from src.ingestion.fallback_manager import FallbackManager
from src.processing.cleaner import DataCleaner
from src.processing.validator import DataValidator
from src.intelligence.anomaly_detector import AnomalyDetector
from src.intelligence.anomaly_fixer import AnomalyFixer
from src.nlp.query_parser import QueryParser
from src.nlp.grammar_corrector import GrammarCorrector
from src.nlp.query_executor import QueryExecutor
from src.analytics.statistics import StatisticsAnalyzer
from src.analytics.visualizer import Visualizer
from src.monitoring.logger import logger


class DataPipeline:
    """Main pipeline application"""
    
    def __init__(self):
        self.console = Console()
        self.db = DatabaseManager()
        self.fallback_manager = FallbackManager()
        self.cleaner = DataCleaner()
        self.validator = DataValidator()
        self.anomaly_detector = AnomalyDetector()
        self.anomaly_fixer = AnomalyFixer()
        self.stats_analyzer = StatisticsAnalyzer()
        self.visualizer = Visualizer()
        
        self.current_df = None
        self.current_table = None
        self.data_mode = 'csv'
        
        Config.create_directories()
        Config.validate_config()
    
    def run(self):
        """Run the main application loop"""
        self.display_welcome()
        
        while True:
            try:
                self.display_menu()
                choice = Prompt.ask("Select an option", default="1")
                
                if choice == '1':
                    self.upload_csv()
                elif choice == '2':
                    self.query_data()
                elif choice == '3':
                    self.view_statistics()
                elif choice == '4':
                    self.create_visualizations()
                elif choice == '5':
                    self.view_data_quality()
                elif choice == '0':
                    self.console.print("\\n[bold green]Goodbye![/bold green]\\n")
                    break
                else:
                    self.console.print("[red]Invalid option[/red]")
                    
            except KeyboardInterrupt:
                if Confirm.ask("\\nExit?"):
                    break
            except Exception as e:
                logger.error(f"Error: {e}")
                self.console.print(f"[red]Error: {e}[/red]")
    
    def display_welcome(self):
        welcome = """
        [bold cyan]╔═══════════════════════════════════════════════════╗
        ║   AI Data Engineering Pipeline                    ║
        ║   Self-Healing & Natural Language Interface       ║
        ╚═══════════════════════════════════════════════════╝[/bold cyan]
        
        [green]✓ Automatic data cleaning
        ✓ Anomaly detection and auto-fixing
        ✓ Natural language queries
        ✓ Advanced analytics[/green]
        """
        self.console.print(Panel(welcome, border_style="cyan"))
    
    def display_menu(self):
        menu = Table(title="\\n[bold cyan]Main Menu[/bold cyan]", show_header=False, border_style="blue")
        menu.add_column("Option", style="cyan", width=5)
        menu.add_column("Description", style="white")
        
        menu.add_row("1", "📁 Upload CSV File")
        menu.add_row("2", "💬 Query Data (Natural Language)")
        menu.add_row("3", "📊 View Statistics")
        menu.add_row("4", "📈 Create Visualizations")
        menu.add_row("5", "✅ View Data Quality")
        menu.add_row("0", "🚪 Exit")
        
        self.console.print(menu)
        
        if self.current_table:
            self.console.print(f"\\n[green]Current Dataset:[/green] {self.current_table}")
    
    def upload_csv(self):
        self.console.print("\\n[bold]📁 CSV Upload[/bold]\\n")
        filepath = Prompt.ask("Enter CSV file path")
        
        if not Path(filepath).exists():
            self.console.print("[red]File not found![/red]")
            return
        
        logger.section("Loading CSV Data")
        df = self.fallback_manager.get_data(mode='csv', filepath=filepath)
        
        if df is None:
            self.console.print("[red]Failed to load CSV![/red]")
            return
        
        self._process_data(df, Path(filepath).stem)
    
    def _process_data(self, df: pd.DataFrame, table_name: str):
        logger.section("Validating Data")
        validation = self.validator.validate(df)
        self.console.print(f"\\n[cyan]Quality Score:[/cyan] {validation['quality_score']:.1f}/100")
        
        logger.section("Cleaning Data")
        df_cleaned, _ = self.cleaner.clean_data(df, auto_fix=True)
        
        logger.section("Detecting Anomalies")
        df_flagged, _ = self.anomaly_detector.detect_all(df_cleaned)
        
        if df_flagged['_anomaly_flag'].sum() > 0:
            if Confirm.ask("\\nAnomalies detected. Auto-fix?", default=True):
                logger.section("Auto-Fixing Anomalies")
                df_final = self.anomaly_fixer.fix_anomalies(df_flagged, df_flagged['_anomaly_flag'].astype(bool))
            else:
                df_final = df_flagged
        else:
            df_final = df_cleaned
            self.console.print("[green]✓ No anomalies![/green]")
        
        df_final = df_final[[c for c in df_final.columns if not c.startswith('_')]]
        
        logger.section("Storing in Database")
        self.db.create_table_from_dataframe(df_final, table_name)
        self.db.insert_dataframe(df_final, table_name, source=self.data_mode)
        
        self.current_df = df_final
        self.current_table = table_name
        
        self.console.print(f"\\n[bold green]✓ Complete![/bold green] Loaded {len(df_final)} rows, {len(df_final.columns)} columns")
    
    def query_data(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded. Upload CSV first.[/yellow]")
            return
        
        self.console.print("\\n[bold]💬 Natural Language Query[/bold]")
        self.console.print("[dim]Example: 'show me total sales by region'[/dim]\\n")
        
        query = Prompt.ask("Enter query")
        
        corrector = GrammarCorrector()
        corrected_query, corrections = corrector.correct(query)
        
        if corrections:
            self.console.print(f"[yellow]Corrected:[/yellow] {corrected_query}")
        
        parser = QueryParser(self.current_df.columns.tolist())
        parsed = parser.parse(corrected_query)
        
        executor = QueryExecutor(self.current_df)
        result = executor.execute(parsed)
        
        self._display_dataframe(result, limit=20)
        self.db.log_query(query, corrected_query if corrections else None, result_count=len(result))
    
    def view_statistics(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded.[/yellow]")
            return
        
        self.console.print("\\n[bold]📊 Statistics[/bold]\\n")
        summary = self.stats_analyzer.get_summary(self.current_df)
        
        self.console.print(f"[cyan]Shape:[/cyan] {summary['shape'][0]} rows × {summary['shape'][1]} columns")
        self.console.print(f"[cyan]Missing:[/cyan] {sum(summary['missing_values'].values())} total\\n")
        
        if summary['numeric_summary']:
            self.console.print("[bold]Numeric Summary:[/bold]")
            numeric_df = pd.DataFrame(summary['numeric_summary'])
            self._display_dataframe(numeric_df.round(2))
    
    def create_visualizations(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded.[/yellow]")
            return
        
        self.console.print("\\n[bold]📈 Visualizations[/bold]\\n")
        self.console.print("1. Distribution plot\\n2. Correlation heatmap")
        
        choice = Prompt.ask("Select", choices=["1", "2"])
        
        if choice == "1":
            column = Prompt.ask("Column name")
            if column in self.current_df.columns:
                filepath = self.visualizer.plot_distribution(self.current_df, column)
                self.console.print(f"[green]✓ Saved: {filepath}[/green]")
        
        elif choice == "2":
            filepath = self.visualizer.plot_correlation_heatmap(self.current_df)
            if filepath:
                self.console.print(f"[green]✓ Saved: {filepath}[/green]")
    
    def view_data_quality(self):
        if self.current_df is None:
            self.console.print("[yellow]No data loaded.[/yellow]")
            return
        
        self.console.print("\\n[bold]✅ Data Quality[/bold]\\n")
        validation = self.validator.validate(self.current_df)
        
        self.console.print(f"[cyan]Quality Score:[/cyan] {validation['quality_score']:.1f}/100")
        self.console.print(f"[cyan]Status:[/cyan] {'PASSED' if validation['is_valid'] else 'FAILED'}")
        
        if validation['warnings']:
            self.console.print(f"\\n[yellow]Warnings ({len(validation['warnings'])})[/yellow]:")
            for warning in validation['warnings'][:5]:
                self.console.print(f"  ⚠️  {warning}")
    
    def _display_dataframe(self, df: pd.DataFrame, limit: int = 10):
        if df.empty:
            self.console.print("[yellow]No data[/yellow]")
            return
        
        table = Table(show_header=True, header_style="bold cyan")
        for col in df.columns:
            table.add_column(str(col))
        
        for _, row in df.head(limit).iterrows():
            table.add_row(*[str(val) for val in row])
        
        self.console.print(table)
        
        if len(df) > limit:
            self.console.print(f"\\n[dim]... and {len(df) - limit} more rows[/dim]")


def main():
    pipeline = DataPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
''')

    # ============================================
    # SAMPLE DATA
    # ============================================
    
    print("\\n📊 Creating sample data files...\\n")
    
    # Create sample CSV with pandas
    try:
        import pandas as pd
        import numpy as np
        
        # Sample sales data
        np.random.seed(42)
        sales_data = {
            'date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
            'product': np.random.choice(['Product_A', 'Product_B', 'Product_C'], 100),
            'sales': np.random.randint(100, 1000, 100).astype(float),
            'quantity': np.random.randint(1, 50, 100).astype(float),
            'price': np.random.uniform(10, 100, 100).round(2)
        }
        
        # Add anomalies
        sales_data['sales'][10] = 10000.0
        sales_data['quantity'][20] = -5.0
        sales_data['price'][30] = np.nan
        
        df_sales = pd.DataFrame(sales_data)
        
        # Create examples directory
        examples_dir = BASE_DIR / "examples"
        examples_dir.mkdir(exist_ok=True)
        
        df_sales.to_csv(examples_dir / 'sample_sales.csv', index=False)
        print("✓ Created: examples/sample_sales.csv")
        
    except Exception as e:
        print(f"⚠️  Could not create sample data: {e}")
        print("   You can create your own CSV files manually")
    
    # ============================================
    # DOCUMENTATION
    # ============================================
    
    create_file("README.md", """# 🚀 AI Data Engineering Pipeline

## Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the application
python main.py

# 3. Upload CSV
Select option 1
Enter: examples/sample_sales.csv

# 4. Query naturally
Select option 2
Query: "show me total sales by region"
```

## Features

✅ Upload CSV files
✅ Automatic data cleaning
✅ Anomaly detection & auto-fixing
✅ Natural language queries
✅ Data quality scoring
✅ Statistical analysis
✅ Visualizations

## Documentation

- **Quick Start**: Run `python main.py` and follow prompts
- **Sample Data**: Check `examples/sample_sales.csv`
- **Configuration**: Edit `.env` file for API keys

## Natural Language Queries

Examples:
- "show me total sales by region"
- "average price per product"
- "count orders by category"

The system auto-corrects spelling mistakes!

## Project Structure
```
AI_DATA_PIPELINE2/
├── main.py                 # Main application
├── requirements.txt        # Dependencies
├── config/                 # Configuration
├── src/                    # Source code
│   ├── database/          # Database operations
│   ├── ingestion/         # Data loading
│   ├── processing/        # Data cleaning
│   ├── intelligence/      # Anomaly detection
│   ├── nlp/               # Natural language
│   ├── analytics/         # Statistics & viz
│   └── monitoring/        # Logging & alerts
├── data/                   # Data storage
└── examples/              # Sample data
```

## Requirements

- Python 3.8+
- See `requirements.txt`

## License

MIT License
""")

    print("\\n" + "="*60)
    print("✅ ALL FILES GENERATED SUCCESSFULLY!")
    print("="*60)
    print("\\n📋 Next Steps:")
    print("\\n1. Install dependencies:")
    print("   pip install -r requirements.txt")
    print("\\n2. Run the application:")
    print("   python main.py")
    print("\\n3. Try the sample data:")
    print("   examples/sample_sales.csv")
    print("\\n🚀 Happy analyzing!\\n")


if __name__ == "__main__":
    generate_all_files()