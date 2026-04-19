from __future__ import annotations

import pandas as pd

from pipeline_platform.errors import DataQualityError


def run_quality_checks(df: pd.DataFrame, checks_config) -> None:
    """
    Run all configured data quality checks against a DataFrame.
    Raises DataQualityError listing all failures if any check fails.
    Does nothing if quality_checks.enabled is False or no checks defined.
    """
    if not checks_config or not checks_config.enabled:
        return

    if not checks_config.checks:
        return

    failures: list[str] = []

    for check in checks_config.checks:
        result = _run_single_check(df, check)
        if result:
            failures.append(result)

    if failures:
        raise DataQualityError(
            "Data quality checks failed:\n" + "\n".join(f"  ✗ {f}" for f in failures)
        )

    print(f"[DQ] All {len(checks_config.checks)} checks passed ({len(df)} rows)")


def _run_single_check(df: pd.DataFrame, check) -> str | None:
    """
    Run one check. Returns an error message string if it fails,
    or None if it passes.
    """
    name = check.name
    column = check.column
    rule = check.rule

    # Column existence check — applies to all rules except min_rows
    if rule != "min_rows" and column not in df.columns:
        return f"{name}: column '{column}' not found in data"

    if rule == "not_null":
        nulls = int(df[column].isnull().sum())
        if nulls > 0:
            return f"{name}: '{column}' has {nulls} null value(s)"

    elif rule == "unique":
        dupes = int(df[column].duplicated().sum())
        if dupes > 0:
            return f"{name}: '{column}' has {dupes} duplicate value(s)"

    elif rule == "greater_than":
        threshold = check.value if check.value is not None else 0
        numeric = pd.to_numeric(df[column], errors="coerce")
        failed = int((numeric <= threshold).sum())
        if failed > 0:
            return (
                f"{name}: '{column}' has {failed} value(s) not greater than {threshold}"
            )

    elif rule == "min_rows":
        min_required = int(check.value) if check.value is not None else 1
        if len(df) < min_required:
            return f"{name}: expected at least {min_required} rows, got {len(df)}"

    elif rule == "timestamp_format":

        def is_valid(v):
            try:
                pd.to_datetime(v)
                return True
            except Exception:
                return False

        invalid = int((~df[column].apply(is_valid)).sum())
        if invalid > 0:
            return f"{name}: '{column}' has {invalid} invalid timestamp(s)"

<<<<<<< HEAD
    return None  # check passed
=======
    return None
>>>>>>> task/10-failure-notifications
