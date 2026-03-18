"""Data validation"""
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
