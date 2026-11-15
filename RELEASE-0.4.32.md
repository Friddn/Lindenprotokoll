# Lindenprotokoll 0.4.32 – heat-pump review and chart options

## Changes
- Added an additive `approved` flag to heat-pump screenshots; confirming daily values marks the screenshot approved and advances directly to the next OCR-completed, unapproved screenshot.
- Added an OCR review filter and a bulk action to remove byte-identical heat-pump screenshots while preserving one original and leaving confirmed daily totals intact.
- Added daily heat-pump chart display modes for heat pump, total, without heat pump, separate lines, and optional stacked components.
- Added logarithmic Y-axis controls to the electricity and heat-pump statistics charts.
- Kept the consumption relationship explicit: total = heat pump + non-heat-pump; without heat pump = total − heat pump. Inconsistent days where heat-pump use exceeds total are flagged and are not assigned a negative remainder.

## Data notes
- `daily_non_hp_electricity` remains an explicitly non-heat-pump daily input, not a total meter reading.
- The statistics chart's household series is derived from electricity meter deltas and represents total household consumption. Its non-heat-pump component is calculated only when both total and heat-pump values exist.
- Daily heat-pump totals are keyed by date in SQLite, so duplicate daily records for the same date cannot coexist; imports/upserts update that date's record.
- Bulk screenshot cleanup removes byte-identical files and screenshots with the same parsed date plus identical OCR electricity/thermal readings. It deliberately does not infer duplicates from date alone.

## Validation
- Python compilation passed.
- Automated tests: 8 passed, 1 skipped.
- Live browser/Chart.js rendering and Home Assistant add-on runtime were not available in this environment and should be checked on a backup/test instance before production use.
