from pathlib import Path
from typing import Optional

import pandas as pd

from app.interfaces import EtlLoader


class BaseETL(EtlLoader):
    """Base class for ETL operations, handling file loading and data standardization.

    This class provides common functionality for loading data from CSV or Excel files
    and standardizing columns to specific types. Subclasses should implement the
    abstract `transform` method to perform specific data transformations.

    Attributes:
        fname: The path to the input file.
        sheet_name: Optional sheet name for Excel files.
    """

    def __init__(self, fname: str | Path, sheet_name: Optional[str] = None) -> None:
        """Initialize the BaseETL instance with file path and optional sheet name.

        Args:
            fname: Path to the input file (CSV or Excel).
            sheet_name: Optional sheet name for Excel files; if None, loads the first sheet.
        """
        self.fname = Path(fname)
        self.sheet_name = sheet_name

    def load(self, **kwargs) -> pd.DataFrame:
        """Load data from the file specified in fname, supporting CSV and Excel formats.

        Args:
            **kwargs: Additional keyword arguments passed to pd.read_csv or pd.read_excel.

        Returns:
            A pandas DataFrame containing the loaded data.

        Raises:
            ValueError: If the file extension is not supported (only CSV and XLS/XLSX).
            Any exceptions raised by pandas read functions (e.g., FileNotFoundError).
        """

        match self.fname.suffix:
            case ".csv":
                df = pd.read_csv(self.fname, **kwargs)
            case ".xlsx" | ".xls":
                df = (
                    pd.read_excel(self.fname, sheet_name=self.sheet_name, **kwargs)
                    if self.sheet_name
                    else pd.read_excel(self.fname, **kwargs)
                )
            case _:
                raise ValueError(
                    f"File extension - {self.fname.suffix} - not supported."
                )

        return df

    def transform(self) -> pd.DataFrame:
        """Abstract method to perform data transformation on the loaded data.

        Subclasses must implement this method to define specific transformation logic,
        such as cleaning, aggregating, or enhancing the DataFrame.

        Returns:
            A transformed pandas DataFrame.

        Raises:
            NotImplementedError: If not overridden in a subclass.
        """
        ...
