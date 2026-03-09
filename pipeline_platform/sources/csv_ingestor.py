from __future__ import annotations

from pathlib import Path

import pandas as pd


class CSVIngestor:
    def read(self, path: str) -> pd.DataFrame:
        csv_path = Path(path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV source not found: {path}")
        return pd.read_csv(csv_path)
