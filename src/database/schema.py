"""Database schema definitions"""

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
        columns_str = ',\n        '.join(columns)
        return Schema.CREATE_DATA_TABLE.format(table_name=table_name, columns=columns_str)
    
    @staticmethod
    def get_all_table_schemas():
        return [Schema.CREATE_METADATA_TABLE, Schema.CREATE_QUERY_HISTORY_TABLE]
