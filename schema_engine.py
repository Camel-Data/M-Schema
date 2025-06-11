import json, os
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select, text
from sqlalchemy.engine import Engine
from utils import read_json, write_json, save_raw_text, examples_to_str
from m_schema import MSchema
from CamelLuigi.query_loaders import AthenaDWLoader
from CamelLuigi.query_loaders import get_aws_connection


class SchemaEngine:
    def __init__(self, game: str, schema: Optional[str] = None,
                 ignore_tables: Optional[List[str]] = None, include_tables: Optional[List[str]] = None,
                 sample_rows_in_table_info: int = 3, max_string_length: int = 300,
                 mschema: Optional[MSchema] = None, db_name: Optional[str] = ''):
        
        self._game = game
        self._connection = get_aws_connection(game)
        self._loader = AthenaDWLoader
        self._schema = schema or game  # 如果没有指定schema，使用game作为schema
        self._db_name = db_name
        self._ignore_tables = ignore_tables or []
        self._include_tables = include_tables
        self._sample_rows_in_table_info = sample_rows_in_table_info
        self._max_string_length = max_string_length
        
        # Dictionary to store table names and their corresponding schema
        self._tables_schemas: Dict[str, str] = {}
        
        # Get available tables
        self._usable_tables = self._get_usable_tables()
        
        if mschema is not None:
            self._mschema = mschema
        else:
            self._mschema = MSchema(schema=self._schema)
            self.init_mschema()

    def _get_usable_tables(self) -> List[str]:
        """获取可用的表列表"""
        try:
            # 使用information_schema查询表信息
            if self._schema:
                sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self._schema}'"
            else:
                sql = "SELECT table_name FROM information_schema.tables"
            
            df = self._loader(self._game,sql).load()
            tables = df['table_name'].tolist() if not df.empty else []
            
            # Apply include/exclude filters
            if self._include_tables:
                tables = [t for t in tables if t in self._include_tables]
            
            tables = [t for t in tables if t not in self._ignore_tables]
            
            # Store schema for each table
            for table in tables:
                self._tables_schemas[table] = self._schema or 'default'
            
            return tables
        except Exception as e:
            print(f"Error getting tables: {e}")
            return []

    @property
    def mschema(self) -> MSchema:
        """Return M-Schema"""
        return self._mschema

    def get_table_comment(self, table_name: str):
        """获取表注释 - Athena可能不支持，返回空字符串"""
        return ''

    def get_schema_names(self) -> List[str]:
        """获取schema名称列表"""
        try:
            sql = "SELECT DISTINCT table_schema FROM information_schema.tables"
            df = self._loader(self._game,sql).load()
            return df['table_schema'].tolist() if not df.empty else ['default']
        except:
            return ['default']

    def fetch_distinct_values(self, table_name: str, column_name: str, max_num: int = 5):
        """获取列的不同值"""
        try:
            table_ref = f"{self._tables_schemas[table_name]}.{table_name}" if self._tables_schemas.get(table_name) else table_name
            sql = f"SELECT DISTINCT {column_name} FROM {table_ref} WHERE {column_name} IS NOT NULL LIMIT {max_num}"
            df = self._loader(self._game,sql).load()

            values = []
            if not df.empty:
                for value in df.iloc[:, 0]:
                    if value is not None and str(value).strip() != '':
                        values.append(value)
            return values
        except Exception as e:
            print(f"Error fetching distinct values for {table_name}.{column_name}: {e}")
            return []

    def get_table_columns(self, table_name: str):
        """获取表的列信息"""
        try:
            schema_name = self._tables_schemas.get(table_name, 'default')
            
            # 查询列信息
            columns_sql = f"""
            SELECT column_name, data_type, comment, extra_info
            FROM information_schema.columns 
            WHERE table_schema = '{schema_name}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            
            df = self._loader(self._game, columns_sql).load()
            
            columns = []
            if not df.empty:
                for _, row in df.iterrows():
                    # 检查是否为主键
                    is_primary_key = 'PRIMARY KEY' in str(row.get('extra_info', '')) if row.get('extra_info') else False
                    
                    column_info = {
                        'name': row['column_name'],
                        'type': row['data_type'],
                        'comment': row.get('comment', '').strip() if row.get('comment') else '',
                        'primary_key': is_primary_key
                    }
                    columns.append(column_info)
            return columns
        except Exception as e:
            print(f"Error getting columns for {table_name}: {e}")
            return []

    def init_mschema(self):
        """初始化MSchema"""
        for table_name in self._usable_tables:
            table_comment = self.get_table_comment(table_name)
            table_with_schema = f"{self._tables_schemas[table_name]}.{table_name}"
            self._mschema.add_table(table_with_schema, fields={}, comment=table_comment)

            # 获取列信息
            columns = self.get_table_columns(table_name)
            for column in columns:
                field_name = column['name']
                field_type = str(column['type'])
                field_comment = column.get('comment', '').strip()
                primary_key = column.get('primary_key', False)
                
                # 获取示例值
                try:
                    examples = self.fetch_distinct_values(table_name, field_name, 5)
                except:
                    examples = []
                examples = examples_to_str(examples)

                self._mschema.add_field(
                    table_with_schema, field_name, field_type=field_type, primary_key=primary_key,
                    comment=field_comment, examples=examples
                )

    def execute_sql(self, sql: str):
        """执行SQL查询"""
        return self._loader(self._game,sql).load()
