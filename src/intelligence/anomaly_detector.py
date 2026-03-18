"""Anomaly detection"""
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
