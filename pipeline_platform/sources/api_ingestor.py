from __future__ import annotations

import os
import requests
import pandas as pd


class APIIngestor:
    def __init__(self, endpoint: str, auth_env_var: str | None = None):
        self.endpoint = endpoint
        self.auth_env_var = auth_env_var

    def extract(
        self, incremental_key: str | None = None, watermark: str | None = None
    ) -> pd.DataFrame:
        headers = {}

        # Auth support
        if self.auth_env_var:
            token = os.getenv(self.auth_env_var)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        params = {}

        # TRUE incremental pushdown
        if incremental_key and watermark:
            params[incremental_key] = watermark

        response = requests.get(self.endpoint, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        return pd.DataFrame(data)
