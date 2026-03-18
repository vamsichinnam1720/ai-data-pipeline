"""Statistics analyzer"""
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
