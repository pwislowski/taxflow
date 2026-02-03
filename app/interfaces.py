from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional

import pandas as pd

from app.enums import (
    Category,
    HighLevelSegmentedPnlColumns,
    OtpSegmentedPnlColumns,
)


class Pipeline(ABC):
    @abstractmethod
    def process(self) -> pd.DataFrame: ...


class EtlLoader(ABC):
    @abstractmethod
    def transform(self) -> pd.DataFrame: ...


class GsEtlLoader(ABC):
    @abstractmethod
    def generate_pipeline_context(
        self,
    ) -> Dict[str, Dict[HighLevelSegmentedPnlColumns, float]]: ...


class SegmentationHandler(ABC):
    @abstractmethod
    def __init__(
        self,
        df: pd.DataFrame,
        column: Optional[OtpSegmentedPnlColumns] = None,
        isin: Optional[List[Category]] = None,
        filter_func: Optional[Callable[[pd.DataFrame], pd.Series]] = None,
    ) -> None: ...

    @abstractmethod
    def process(self) -> pd.DataFrame: ...
