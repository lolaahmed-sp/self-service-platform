from __future__ import annotations

import duckdb
import pandas as pd
from pathlib import Path


class DuckDBWarehouse:
    def __init__(self, db_path: str = "warehouse.duckdb") -> None:
        self.db_path = db_path

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    # Core data methods

    def ensure_table_from_dataframe(
        self, table_name: str, dataframe: pd.DataFrame
    ) -> None:
        with self._connect() as conn:
            conn.register("incoming_df", dataframe)
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                "AS SELECT * FROM incoming_df LIMIT 0"
            )

    def load_dataframe(
        self, table_name: str, dataframe: pd.DataFrame, load_mode: str
    ) -> int:
        self.ensure_table_from_dataframe(table_name, dataframe)
        with self._connect() as conn:
            conn.register("incoming_df", dataframe)
            if load_mode == "overwrite":
                conn.execute(f"DELETE FROM {table_name}")
            cols = ", ".join(dataframe.columns)
            conn.execute(
                f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM incoming_df"
            )
        return len(dataframe)

    # Write method (used by registry + run_logger)

    def execute(self, sql: str, params: list | None = None) -> None:
        """Execute a write statement (INSERT, UPDATE, CREATE, DELETE)."""
        with self._connect() as conn:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    # Read method (used by tests + status command)

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute a SELECT and return results as a list of dicts."""
        with self._connect() as conn:
            if params:
                df = conn.execute(sql, params).fetchdf()
            else:
                df = conn.execute(sql).fetchdf()
        return df.to_dict(orient="records")

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        with self._connect() as conn:
            return conn.execute(f"SELECT * FROM {table_name}").fetchdf()

    # SQL transform method (Task 2)

    def execute_sql_file(self, path: str) -> None:
        """Read a .sql file and execute it against the warehouse."""
        sql_path = Path(path)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL transform file not found: {path}")
        sql = sql_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.execute(sql)
        print(f"[Transform] Executed {path}")

    # Schema tracking methods (Task 6)

    def _init_schema_table(self) -> None:
        self.execute("""
            CREATE TABLE IF NOT EXISTS metadata_schemas (
                pipeline_name TEXT,
                column_name   TEXT,
                column_type   TEXT,
                stored_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (pipeline_name, column_name)
            )
        """)

    def store_schema(self, pipeline_name: str, df: pd.DataFrame) -> None:
        """Store column names and types for a pipeline after first load."""
        self._init_schema_table()
        with self._connect() as conn:
            for col, dtype in df.dtypes.items():
                conn.execute(
                    """
                    INSERT INTO metadata_schemas
                        (pipeline_name, column_name, column_type)
                    VALUES (?, ?, ?)
                    ON CONFLICT (pipeline_name, column_name)
                    DO UPDATE SET
                        column_type = excluded.column_type
                    """,
                    [pipeline_name, col, str(dtype)],
                )

    def get_stored_schema(self, pipeline_name: str) -> dict:
        """Return stored column → type mapping, or empty dict on first run."""
        self._init_schema_table()
        rows = self.query(
            "SELECT column_name, column_type FROM metadata_schemas "
            "WHERE pipeline_name = ?",
            [pipeline_name],
        )
        return {r["column_name"]: r["column_type"] for r in rows}

    def compare_schema(self, pipeline_name: str, df: pd.DataFrame) -> list[str]:
        """
        Compare incoming DataFrame schema against stored schema.
        Returns a list of drift messages. Empty list means no drift.
        """
        stored = self.get_stored_schema(pipeline_name)
        if not stored:
            return []

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

    # Watermark methods (Task 5)

    def get_watermark(self, pipeline_name: str, key: str):
        """Return stored watermark value, or None if no watermark exists yet."""
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_watermarks (
                    pipeline_name   TEXT,
                    incremental_key TEXT,
                    max_value       TEXT,
                    PRIMARY KEY (pipeline_name, incremental_key)
                )
            """)
            result = conn.execute(
                """
                SELECT max_value
                FROM metadata_watermarks
                WHERE pipeline_name = ? AND incremental_key = ?
                """,
                [pipeline_name, key],
            ).fetchone()
        return result[0] if result else None

    def update_watermark(self, pipeline_name: str, key: str, value: str) -> None:
        """Store or update the watermark for a pipeline + incremental key."""
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_watermarks (
                    pipeline_name   TEXT,
                    incremental_key TEXT,
                    max_value       TEXT,
                    PRIMARY KEY (pipeline_name, incremental_key)
                )
            """)
            conn.execute(
                """
                INSERT INTO metadata_watermarks
                    (pipeline_name, incremental_key, max_value)
                VALUES (?, ?, ?)
                ON CONFLICT (pipeline_name, incremental_key)
                DO UPDATE SET max_value = excluded.max_value
                """,
                [pipeline_name, key, value],
            )

    # Config versioning (Task 12)

    def track_config_version(
        self, pipeline_name: str, config_hash: str, config_snapshot: str
    ) -> bool:
        """
        Store a config version if it has changed since the last recorded version.
        Returns True if a new version was recorded, False if config is unchanged.
        """
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_config_history (
                    id               INTEGER,
                    pipeline_name    TEXT,
                    config_hash      TEXT,
                    config_snapshot  TEXT,
                    recorded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (pipeline_name, config_hash)
                )
            """)

            result = conn.execute(
                """
                SELECT config_hash FROM metadata_config_history
                WHERE pipeline_name = ?
                ORDER BY recorded_at DESC
                LIMIT 1
                """,
                [pipeline_name],
            ).fetchone()

            if result and result[0] == config_hash:
                return False  # config unchanged

            conn.execute(
                """
                INSERT INTO metadata_config_history
                    (pipeline_name, config_hash, config_snapshot)
                VALUES (?, ?, ?)
                ON CONFLICT (pipeline_name, config_hash) DO NOTHING
                """,
                [pipeline_name, config_hash, config_snapshot],
            )
            return True
