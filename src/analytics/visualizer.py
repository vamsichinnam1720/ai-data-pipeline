"""Visualizer"""
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
