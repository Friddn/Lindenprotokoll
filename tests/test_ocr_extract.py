import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lindenprotokoll' / 'app'))
from ocr_extract import parse_ocr_fields
from graph_digitizer import resample_15min


def test_german_heatpump_values():
    got = parse_ocr_fields('6. November 2026\nStromverbrauch 23,3 kWh\nThermische Energie 93,5 kWh')
    assert got == {'date': '2026-11-06', 'electricity_kwh': 23.3, 'thermal_kwh': 93.5}


def test_resampling_is_estimated_and_does_not_extrapolate():
    pts = [
        {'ts': '2026-11-06T00:15', 'value_kw': 1.0, 'confidence': 0.8},
        {'ts': '2026-11-06T00:45', 'value_kw': 2.0, 'confidence': 0.6},
    ]
    out = resample_15min(pts, '2026-11-06')
    assert [p['ts'] for p in out] == ['2026-11-06T00:15', '2026-11-06T00:30', '2026-11-06T00:45']
    assert out[1]['value_kw'] == 1.5
    assert all(p['point_kind'] == 'estimated-interpolated-15min' for p in out)


def test_mobile_value_before_label_layout():
    text = '''6. November 2026\n23,3 kWh\n| Stromverbrauch\n93,5 kWh\n| Thermische Energie'''
    assert parse_ocr_fields(text) == {
        'date': '2026-11-06', 'electricity_kwh': 23.3, 'thermal_kwh': 93.5
    }


def test_wh_values_are_normalized_to_kwh():
    got = parse_ocr_fields('03.09.2026\nStromverbrauch 19.250 Wh\nThermische Energie 97 350 Wh')
    assert got['electricity_kwh'] == 19.25
    assert got['thermal_kwh'] == 97.35


def test_wh_value_before_label_and_decimal_comma():
    got = parse_ocr_fields('03.09.2026\n19,250 kWh | Stromverbrauch\n97,35 kWh | Thermische Energie')
    assert got['electricity_kwh'] == 19.25
    assert got['thermal_kwh'] == 97.35


def test_two_decimal_kwh_values_are_preserved():
    got = parse_ocr_fields('03.09.2026\nStromverbrauch 19,25 kWh\nThermische Energie 97.35 kWh')
    assert got['electricity_kwh'] == 19.25
    assert got['thermal_kwh'] == 97.35


def test_two_metrics_on_same_line_keep_their_own_values():
    got = parse_ocr_fields('03.09.2026 Stromverbrauch 19,25 kWh Thermische Energie 97,35 kWh')
    assert got['electricity_kwh'] == 19.25
    assert got['thermal_kwh'] == 97.35


def test_thermal_value_is_never_used_as_electricity_without_electricity_label():
    got = parse_ocr_fields('03.09.2026\nThermische Energie 97,35 kWh')
    assert got['electricity_kwh'] is None
    assert got['thermal_kwh'] == 97.35


def test_missing_electricity_label_does_not_borrow_thermal_value():
    got = parse_ocr_fields('03.09.2026\n97,35 kWh\nThermische Energie')
    assert got['electricity_kwh'] is None
    assert got['thermal_kwh'] == 97.35
