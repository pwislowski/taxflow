from app.enums import Activity, Category, Segment


def test_activity_enum():
    """Test Activity enum values and parsing."""
    assert Activity.Routine == "routine"
    assert Activity.Entrepreneur == "entrepreneur"

    # Test parsing strings
    assert Activity("routine") == Activity.Routine
    assert Activity("entrepreneur") == Activity.Entrepreneur


def test_segment_enum():
    """Test Segment enum includes all expected values."""
    expected_segments = ["DIS", "THUB", "OM", "OTH", "PPAL", "CM", "OTH BU"]
    actual_values = [seg.value for seg in Segment]
    assert set(actual_values) == set(expected_segments)  # Order doesn't matter


def test_category_enum():
    """Test Category enum values and parsing."""
    assert Category.Distribution == "distribution"
    assert (
        Category.OwnManufacturingThirdParty == "own_manufacturing_third_party"
    )
    assert Category.OwnManufacturingIC == "own_manufacturing_ic"
    assert Category.ContractManufacturing == "contract_manufacturing"

    # Test parsing
    assert Category("distribution") == Category.Distribution
    assert Category("contract_manufacturing") == Category.ContractManufacturing
