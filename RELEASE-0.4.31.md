# Lindenprotokoll 0.4.31 – Meal grouping for regression

## Changes
- Regression dataset now combines meal entries for the same person and parsed date/time into one meal observation.
- Food variables are combined across all entries in that meal; food occurrence thresholds count meal observations rather than split entries.
- Regression preview reports how many source entries were combined.
- Pain events are matched to meals belonging to the same person, including when the person filter is set to all.
- No database records are modified; grouping happens only while building the regression dataset.

## Validation
- Added a regression dataset test covering same-time entries, equivalent time formats, and separation between people.
- Python compilation and automated test suite passed.
- Live application validation remains recommended.
