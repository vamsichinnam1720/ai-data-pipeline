"""Database manager"""
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
