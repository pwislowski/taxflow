from dataclasses import dataclass
from typing import Optional, Sequence

import pandas as pd

from app.interfaces import GsEtlLoader


@dataclass
class PipelineContext:
    company_code: Optional[str] = None
    external_cost_business_units: Optional[Sequence[str]] = None
    grand_totals: Optional[pd.DataFrame] = None
    gsdivbu_charges: Optional[GsEtlLoader] = None
    gsdivbu_charges_df: Optional[pd.DataFrame] = None
    gsdivbu_model: Optional[GsEtlLoader] = None
    otp_pnl: Optional[pd.DataFrame] = None
    relevant_gsdiv_bus: Optional[Sequence[str]] = None
    rnd_services: Optional[pd.DataFrame] = None
    royalties: Optional[pd.DataFrame] = None
    target_ebit_erosion: Optional[float] = None
