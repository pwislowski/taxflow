from enum import StrEnum
from typing import List


class Activity(StrEnum):
    Routine = "routine"
    Entrepreneur = "entrepreneur"


class Segment(StrEnum):
    Distribution = "DIS"
    Transportation = "THUB"
    OwnManufacturing = "OM"
    Other = "OTH"
    Principal = "PPAL"
    ContractManufacturing = "CM"
    Unassigned = "OTH BU"


class Category(StrEnum):
    Distribution = "distribution"
    OwnManufacturingThirdParty = "own_manufacturing_third_party"
    OwnManufacturingIC = "own_manufacturing_ic"
    ContractManufacturing = "contract_manufacturing"


class SapBwColumns(StrEnum):
    PnlItem = "pnl_item"
    CompanyCode = "company_code"
    CompanyCodeText = "company_code_text"
    ProfitCenter = "profit_center"
    ProfitCenterText = "profit_center_text"
    GlAccount = "gl_account"
    GlAccountText = "gl_account_text"
    MprColumn = "mpr_column"
    Amount = "value_chf"
    FiscalYearPeriod = "fiscal_year_period"
    FiscalYearPeriodText = "fiscal_year_period_text"
    LocalCurrency = "local_currency"
    LocalCurrencyText = "local_currency_text"


class GsDivBuExpenseColumns(StrEnum):
    Recipient = "recipient"
    NetAllocated = "net_allocated"
    NetAllocatedInternalCost = "net_allocated_internal_cost"
    NetAllocatedExternalCost = "net_allocated_external_cost"
    NetAllocatedMarkup = "net_allocated_markup"

    @classmethod
    def list_column_order(cls) -> List["GsDivBuExpenseColumns"]:
        return [
            cls.Recipient,
            cls.NetAllocated,
            cls.NetAllocatedInternalCost,
            cls.NetAllocatedExternalCost,
            cls.NetAllocatedMarkup,
        ]


class GsDivBuIncomeColumns(StrEnum):
    CompanyCode = "company_code"
    ActualCostBaseline = "actual_cost_baseline"
    ActualCostInternalCost = "actual_cost_internal_cost"
    ActualCostExternalCost = "actual_cost_external_cost"
    ActualCostMarkup = "actual_cost_markup"
    LocalCost = "local_cost"
    NetExpense = "net_expense"
    NetExpenseInternalCost = "net_expense_internal_cost"
    NetExpenseExternalCost = "net_expense_external_cost"
    NetExpenseMarkup = "net_expense_markup"
    NetIncome = "net_income"

    @classmethod
    def list_column_order(cls) -> List["GsDivBuIncomeColumns"]:
        return [
            cls.CompanyCode,
            cls.ActualCostBaseline,
            cls.ActualCostInternalCost,
            cls.ActualCostExternalCost,
            cls.ActualCostMarkup,
            cls.LocalCost,
            cls.NetExpense,
            cls.NetExpenseInternalCost,
            cls.NetExpenseExternalCost,
            cls.NetExpenseMarkup,
            cls.NetIncome,
        ]


class HighLevelSegmentedPnlColumns(StrEnum):
    TotalNetSales = "total_net_sales"
    Cogs = "cogs"
    OpertionalIncome = "operational_income"
    GrossProfitBeforeVariances = "gross_profit_before_variances"
    GrossProfitAfterVariances = "gross_profit_after_variances"
    TotalSAR = "total_sar"
    TotalEBIT = "total_ebit"

    @classmethod
    def list_column_order(cls) -> List["HighLevelSegmentedPnlColumns"]:
        return [
            cls.TotalNetSales,
            cls.GrossProfitAfterVariances,
            cls.TotalSAR,
            cls.TotalEBIT,
        ]


class OtpSegmentedPnlColumns(StrEnum):
    CompanyCode = "CompanyCode"
    OrgBU = "OrgBU"
    PlantProdCoCode = "PlantProdCoCode"
    ProcuredFrom = "ProcuredFrom"
    TPSegmentFinal = "TPSegmentFinal"
    InterCompanyFlag = "InterCompanyFlag"
    TradingPartnerCoCode = "TradingPartnerCoCode"
    ProfitCenter = "ProfitCenter"
    Key = "Key"
    Net_Sales_3P = "Net Sales 3P"
    Net_Sales_GC = "Net Sales GC"
    Net_Sales_Total = "Net Sales Total"
    Cogs = "COGS"
    Gross_Profit_Before_Variances = "Gross Profit Before Variances"
    Variances = "Variances"
    Gross_Profit_After_Variances = "Gross Profit After Variances"
    Sales_and_Marketing_OH = "Sales & Marketing OH"
    OH_Administration = "OH Administration"
    Research_and_Technology_OH = "Research & Technology OH"
    Total_SAR = "Total SAR"
    Net_Operating_Profit = "Net Operating Profit"
    VA_Inventory_Receivables_non_c = "VA Inventory & Receiv. & oth. non c"
    Operational_Income = "Operational Income"
    Other_Income = "Other Income"
    Other_Expenses = "Other Expenses"
    Unusual_Expenses_Income = "Unusual Expenses (Income)"
    Operating_Income = "Operating Income"
    Ebit = "EBIT"
    LowerQuartile = "LowerQuartile"
    Median = "Median"
    UpperQuartile = "UpperQuartile"
    LowTarget_90 = "LowTarget(90%)"
    Target = "Target"
    HighTarget_110 = "HighTarget(110%)"
    Nom = "NOM"
    Ncp = "NCP"
    Adjustments_In_IQ_Range = "Adjustments In IQ Range"
    Adjustments_In_Target = "Adjustments In Target"
    OutOfRange = "OutOfRange"
    Activity = "activity"
    Category = "category"

    @classmethod
    def list_values(cls) -> List[str]:
        """Return a list of all possible column names defined in this enum."""
        return [member.value for member in cls]

    @classmethod
    def list_text_cols(cls) -> List[str]:
        """Return a list of text-based column names that should be treated as strings."""
        return [
            "CompanyCode",
            "OrgBU",
            "PlantProdCoCode",
            "ProcuredFrom",
            "TPSegmentFinal",
            "InterCompanyFlag",
            "TradingPartnerCoCode",
            "ProfitCenter",
            "Key",
        ]

    @classmethod
    def list_percentage_cols(cls) -> List[str]:
        """Return a list of percentage column names that require special handling (e.g., conversion to float)."""
        return [
            "LowerQuartile",
            "Median",
            "UpperQuartile",
            "LowTarget(90%)",
            "Target",
            "HighTarget(110%)",
            "NOM",
            "NCP",
        ]

    @classmethod
    def list_added_cols(cls) -> List[str]:
        """Return a list of columns added by the pipeline for activity and category segmentation."""
        return [
            cls.Activity,
            cls.Category,
        ]

    @classmethod
    def list_numeric_cols(cls) -> List[str]:
        """Return a list of numeric column names by excluding text, percentage, and added columns from all values."""
        return [
            col
            for col in cls.list_values()
            if col
            not in [
                *cls.list_text_cols(),
                *cls.list_percentage_cols(),
                *cls.list_added_cols(),
            ]
        ]
