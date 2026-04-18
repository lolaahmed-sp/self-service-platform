from __future__ import annotations

import duckdb
import pandas as pd
from pathlib import Path


class DuckDBWarehouse:
    def __init__(self, db_path: str = "warehouse.duckdb") -> None:
        self.db_path = db_path
        self._init_metadata_tables()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    # =========================
    # METADATA TABLES
    # =========================

    def _init_metadata_tables(self) -> None:
        with self._connect() as conn:
            # Watermarks (Task 5)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_watermarks (
                    pipeline_name   TEXT,
                    incremental_key TEXT,
                    max_value       TEXT,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (pipeline_name, incremental_key)
                )
            """)

            # Schema tracking (Task 6)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_schemas (
                    pipeline_name TEXT,
                    column_name   TEXT,
                    column_type   TEXT,
                    stored_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (pipeline_name, column_name)
                )
            """)

    # =========================
    # CORE DATA METHODS
    # =========================

    def ensure_table_from_dataframe(
        self, table_name: str, dataframe: pd.DataFrame
    ) -> None:
        with self._connect() as conn:
            conn.register("incoming_df", dataframe)
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS "
                "SELECT * FROM incoming_df LIMIT 0"
            )

    def load_dataframe(
        self, table_name: str, dataframe: pd.DataFrame, load_mode: str
    ) -> int:
        self.ensure_table_from_dataframe(table_name, dataframe)

        with self._connect() as conn:
            conn.register("incoming_df", dataframe)

            if load_mode == "overwrite":
                conn.execute(f"DELETE FROM {table_name}")

            conn.execute(f"INSERT INTO {table_name} SELECT * FROM incoming_df")

        return len(dataframe)

    # =========================
    # SQL TRANSFORM (Task 2)
    # =========================

    def execute_sql_file(self, path: str) -> None:
        """Read a .sql file and execute it against the warehouse."""
        sql_path = Path(path)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL transform file not found: {path}")
        sql = sql_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.execute(sql)
        print(f"[Transform] Executed {path}")

    # =========================
    # WATERMARK METHODS (Task 5)
    # =========================

    def get_watermark(self, pipeline_name: str, key: str):
        rows = self.query(
            """
            SELECT max_value
            FROM metadata_watermarks
            WHERE pipeline_name = ? AND incremental_key = ?
            """,
            [pipeline_name, key],
        )
        return rows[0]["max_value"] if rows else None

    def update_watermark(self, pipeline_name: str, key: str, value: str) -> None:
        # FIX: DuckDB upsert syntax — INSERT OR REPLACE is MySQL, not valid here
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO metadata_watermarks
                    (pipeline_name, incremental_key, max_value, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (pipeline_name, incremental_key)
                DO UPDATE SET
                    max_value  = excluded.max_value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                [pipeline_name, key, value],
            )

    # =========================
    # SCHEMA METHODS (Task 6)
    # =========================

    def store_schema(self, pipeline_name: str, df: pd.DataFrame) -> None:
        """Store or update schema for a pipeline."""
        schema_rows = [
            (pipeline_name, col, str(dtype)) for col, dtype in df.dtypes.items()
        ]
        # FIX: DuckDB upsert syntax — INSERT OR REPLACE is MySQL, not valid here
        with self._connect() as conn:
            for pipeline_name_val, col, dtype in schema_rows:
                conn.execute(
                    """
                    INSERT INTO metadata_schemas
                        (pipeline_name, column_name, column_type, stored_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (pipeline_name, column_name)
                    DO UPDATE SET
                        column_type = excluded.column_type,
                        stored_at   = CURRENT_TIMESTAMP
                    """,
                    [pipeline_name_val, col, dtype],
                )

    def get_stored_schema(self, pipeline_name: str) -> dict:
        rows = self.query(
            """
            SELECT column_name, column_type
            FROM metadata_schemas
            WHERE pipeline_name = ?
            """,
            [pipeline_name],
        )
        return {r["column_name"]: r["column_type"] for r in rows}

    def compare_schema(self, pipeline_name: str, df: pd.DataFrame) -> list[str]:
        """
        Compare incoming schema vs stored schema.
        Returns list of drift messages. Empty list means no drift.
        """
        stored = self.get_stored_schema(pipeline_name)

        if not stored:
            return []  # First run — no stored schema yet

        incoming = {col: str(dtype) for col, dtype in df.dtypes.items()}
        errors: list[str] = []

        for col in stored:
            if col not in incoming:
                errors.append(f"Column '{col}' was removed from source")

        for col in incoming:
            if col not in stored:
                errors.append(f"New column '{col}' not in stored schema")
            elif incoming[col] != stored[col]:
                errors.append(
                    f"Column '{col}' type changed: {stored[col]} → {incoming[col]}"
                )

        return errors

    # =========================
    # QUERY METHODS
    # =========================

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        with self._connect() as conn:
            if params:
                df = conn.execute(sql, params).fetchdf()
            else:
                df = conn.execute(sql).fetchdf()
        return df.to_dict(orient="records")

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        with self._connect() as conn:
            return conn.execute(f"SELECT * FROM {table_name}").fetchdf()
