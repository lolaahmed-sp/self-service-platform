from __future__ import annotations


class SchemaValidationError(Exception):
    """Raised when schema drift is detected between pipeline runs."""


class DataQualityError(Exception):
    """Raised when a data quality check fails."""
