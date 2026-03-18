"""CSV loader"""
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
