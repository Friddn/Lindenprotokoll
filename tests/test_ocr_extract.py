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
