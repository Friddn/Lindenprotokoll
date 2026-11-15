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
    # Keep each metric in its own label-delimited region. In particular, never
    # borrow the next/previous metric's value just because it is nearby in OCR.
    number = re.compile(r"([0-9][0-9.,\t \u00a0\u202f]*)[\t \u00a0\u202f]*(kWh|Wh)\b", re.I)
    labels = {
        "electricity_kwh": re.compile(r"(?:Stromverbrauch|W[aä]rmepumpen[- ]?Strom|elektrische\s+Energie)", re.I),
        "thermal_kwh": re.compile(r"(?:Thermische\s+Energie|W[aä]rmemenge|thermische\s+Leistung)", re.I),
    }
    lines = text.splitlines()
    all_labels = []
    for key, rx in labels.items():
        for match in rx.finditer(text):
            all_labels.append((match.start(), match.end(), key))
    all_labels.sort()

    for pos, (key_rx_start, key_rx_end, key) in enumerate(all_labels):
        previous_end = all_labels[pos - 1][1] if pos else 0
        next_start = all_labels[pos + 1][0] if pos + 1 < len(all_labels) else len(text)
        after = text[key_rx_end:next_start]
        before = text[previous_end:key_rx_start]
        # In mobile OCR the numeric value is often on the line immediately
        # before its label. Prefer that close value over a following metric's
        # value, which may appear before the next label.
        preceding = list(number.finditer(before))
        chosen = None
        if preceding:
            candidate = preceding[-1]
            absolute_value_pos = previous_end + candidate.start()
            previous_label_on_same_line = bool(pos and
                text.rfind("\n", 0, absolute_value_pos) == text.rfind("\n", 0, all_labels[pos-1][1]))
            if not previous_label_on_same_line and before[candidate.end():].count("\n") <= 1:
                chosen = candidate
        if chosen is None:
            following = list(number.finditer(after))
            if following:
                chosen = following[0]
        if chosen is not None:
            result[key] = _number(chosen.group(1), chosen.group(2))

    # OCR engines sometimes put the value on the immediately adjacent line.
    # Only use that fallback if the adjacent line contains a number and no
    # competing metric label; this is deliberately conservative.
    if not all_labels:
        for key, rx in labels.items():
            for i, line in enumerate(lines):
                if not rx.search(line):
                    continue
                if i > 0 and not any(other.search(lines[i-1]) for other in labels.values()):
                    m = list(number.finditer(lines[i-1]))
                    if m: result[key] = _number(m[-1].group(1), m[-1].group(2))
                if result[key] is None and i + 1 < len(lines) and not any(other.search(lines[i+1]) for other in labels.values()):
                    m = list(number.finditer(lines[i+1]))
                    if m: result[key] = _number(m[0].group(1), m[0].group(2))
                break
    return result
