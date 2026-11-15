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

def _number(text, unit="kwh"):
    """Parse German/English formatted energy and normalize Wh to kWh."""
    raw = text.strip().replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    if not raw:
        return None
    # German convention: dot groups thousands, comma is decimal separator.
    # For Wh, a lone dot followed by exactly three digits is commonly a
    # thousands separator (e.g. 19.250 Wh = 19.25 kWh).
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    elif unit.lower() == "wh" and re.fullmatch(r"\d{1,3}\.\d{3}", raw):
        raw = raw.replace(".", "")
    try:
        value = float(raw)
        if not 0 <= value < 10_000_000:
            return None
        if unit.lower() == "wh":
            value /= 1000.0
        return round(value, 6)
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
    # Match both value-before-label and label-before-value layouts, and both
    # kWh and Wh. Keep each match close to its metric label to avoid crossing.
    number = re.compile(r"([0-9][0-9.,\t \u00a0\u202f]*)[\t \u00a0\u202f]*(kWh|Wh)\b", re.I)
    labels = {
        "electricity_kwh": re.compile(r"(?:Stromverbrauch|W[aä]rmepumpen[- ]?Strom)", re.I),
        "thermal_kwh": re.compile(r"Thermische\s+Energie", re.I),
    }
    lines = text.splitlines()
    for key, label_rx in labels.items():
        found = None
        for index, line in enumerate(lines):
            label_match = label_rx.search(line)
            if not label_match:
                continue
            # OCR can put the value before the label on the same line or
            # on the immediately preceding line.
            # Prefer a value following this label on the same line. This avoids
            # assigning the previous metric's value when OCR puts both metrics
            # on one line (e.g. "Stromverbrauch 19,2 kWh Thermische Energie 97,3 kWh").
            same_line_after = list(number.finditer(line[label_match.end():]))
            if same_line_after:
                found = same_line_after[0]
                # Match objects are relative to the substring; retain the unit/value
                # groups only, which are the fields consumed below.
            if found is None:
                same_line_before = list(number.finditer(line[:label_match.start()]))
                if same_line_before:
                    found = same_line_before[-1]
            if found is None and index > 0 and not any(rx.search(lines[index - 1]) for other, rx in labels.items() if other != key):
                preceding = list(number.finditer(lines[index - 1]))
                if preceding:
                    found = preceding[-1]
            if found is None:
                for following in lines[index + 1:index + 3]:
                    if any(rx.search(following) for rx in labels.values()):
                        break
                    candidates = list(number.finditer(following))
                    if candidates:
                        found = candidates[0]
                        break
            if found:
                result[key] = _number(found.group(1), found.group(2))
            break
    return result
