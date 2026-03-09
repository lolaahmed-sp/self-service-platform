from __future__ import annotations

import duckdb
import pandas as pd


class DuckDBWarehouse:
    def __init__(self, db_path: str = "warehouse.duckdb") -> None:
        self.db_path = db_path

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def ensure_table_from_dataframe(self, table_name: str, dataframe: pd.DataFrame) -> None:
        with self._connect() as conn:
            conn.register("incoming_df", dataframe)
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM incoming_df LIMIT 0")

    def load_dataframe(self, table_name: str, dataframe: pd.DataFrame, load_mode: str) -> int:
        self.ensure_table_from_dataframe(table_name, dataframe)
        with self._connect() as conn:
            conn.register("incoming_df", dataframe)
            if load_mode == "overwrite":
                conn.execute(f"DELETE FROM {table_name}")
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM incoming_df")
        return len(dataframe)

    def execute(self, sql: str) -> None:
        with self._connect() as conn:
            conn.execute(sql)

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        with self._connect() as conn:
            return conn.execute(f"SELECT * FROM {table_name}").fetchdf()
