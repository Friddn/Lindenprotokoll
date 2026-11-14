"""Conservative parsing of Tesseract text from heat-pump screenshots.

Values are suggestions only; the user must confirm them before persistence.
"""
import re
from datetime import datetime

MONTHS = {
    "januar": 1, "january": 1, "februar": 2, "february": 2,
    "marz": 3, "märz": 3, "march": 3, "april": 4, "mai": 5, "may": 5,
    "juni": 6, "june": 6, "juli": 7, "july": 7, "august": 8,
    "september": 9, "oktober": 10, "october": 10, "november": 11,
    "dezember": 12, "december": 12,
}

def _number(text):
    text = text.strip().replace(" ", "")
    # German decimal comma; thousands separators are not expected for daily kWh.
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    try:
        value = float(text)
        return value if 0 <= value < 10000 else None
    except ValueError:
        return None

def parse_ocr_fields(text):
    result = {"date": None, "electricity_kwh": None, "thermal_kwh": None}
    if not text:
        return result
    date_patterns = [
        r"\b(\d{1,2})[.\-/ ]+(\d{1,2})[.\-/ ]+(20\d{2})\b",
        r"\b(\d{1,2})\.?\s+([A-Za-zÄÖÜäöü]+)\s+(20\d{2})\b",
    ]
    for pattern in date_patterns:
        m = re.search(pattern, text, re.I)
        if not m: continue
        try:
            if m.lastindex == 3 and m.group(2).isdigit():
                d, mo, y = map(int, m.groups())
            else:
                d, month, y = m.groups(); mo = MONTHS.get(month.lower())
                d, y = int(d), int(y)
            if mo:
                result["date"] = datetime(int(y), int(mo), int(d)).date().isoformat()
                break
        except (ValueError, TypeError):
            pass
    # In the mobile screenshot layout Tesseract often reads the large value
    # immediately before its label (value-first), not after it. Keep the search
    # window short so one metric cannot accidentally consume the next metric.
    patterns = {
        "electricity_kwh": [
            r"Stromverbrauch[^\n]{0,45}?([0-9][0-9.,]*)\s*kWh",
            r"([0-9][0-9.,]*)\s*kWh(?:(?!Thermische\s+Energie)[\s\S]){0,80}?Stromverbrauch",
        ],
        "thermal_kwh": [
            r"Thermische\s+Energie[^\n]{0,45}?([0-9][0-9.,]*)\s*kWh",
            r"([0-9][0-9.,]*)\s*kWh(?:(?!Stromverbrauch)[\s\S]){0,80}?Thermische\s+Energie",
        ],
    }
    for key, alternatives in patterns.items():
        for pattern in alternatives:
            m = re.search(pattern, text, re.I)
            if m:
                result[key] = _number(m.group(1))
                break
    return result
