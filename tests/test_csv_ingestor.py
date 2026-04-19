from __future__ import annotations

import pytest
import pandas as pd

from pipeline_platform.sources.csv_ingestor import CSVIngestor


def test_read_returns_dataframe(tmp_path):
    """A valid CSV is read into a DataFrame with correct shape."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,amount,status\n1,100,open\n2,200,closed\n3,300,open\n"
    )

    df = CSVIngestor().read(str(csv_file))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["order_id", "amount", "status"]


def test_read_correct_values(tmp_path):
    """Values are read correctly — not truncated or shifted."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n42,Alice\n99,Bob\n")

    df = CSVIngestor().read(str(csv_file))

    assert df.iloc[0]["id"] == 42
    assert df.iloc[0]["name"] == "Alice"
    assert df.iloc[1]["id"] == 99
    assert df.iloc[1]["name"] == "Bob"


def test_read_single_row(tmp_path):
    """A CSV with exactly one data row is handled correctly."""
    csv_file = tmp_path / "single.csv"
    csv_file.write_text("id,value\n1,hello\n")

    df = CSVIngestor().read(str(csv_file))

    assert len(df) == 1
    assert df.iloc[0]["value"] == "hello"


def test_read_empty_csv_returns_empty_dataframe(tmp_path):
    """A CSV with only a header row returns an empty DataFrame, not an error."""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("id,name\n")

    df = CSVIngestor().read(str(csv_file))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    assert list(df.columns) == ["id", "name"]


def test_read_preserves_column_names_with_spaces(tmp_path):
    """Column names with spaces are preserved exactly."""
    csv_file = tmp_path / "cols.csv"
    csv_file.write_text("First Name,Last Name,Age\nAlice,Smith,30\n")

    df = CSVIngestor().read(str(csv_file))

    assert "First Name" in df.columns
    assert "Last Name" in df.columns


def test_read_missing_file_raises_file_not_found(tmp_path):
    """A path that does not exist raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        CSVIngestor().read(str(tmp_path / "does_not_exist.csv"))


def test_read_non_csv_extension_with_valid_content(tmp_path):
    """File extension does not matter — valid CSV content still parses."""
    txt_file = tmp_path / "data.txt"
    txt_file.write_text("id,val\n1,x\n")

    df = CSVIngestor().read(str(txt_file))
    assert len(df) == 1
