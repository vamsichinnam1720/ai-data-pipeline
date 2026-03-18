"""Data cleaning"""
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
