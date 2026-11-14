# Lindenprotokoll 0.4.25 – CSV import compatibility

This release carries forward the 0.4.24 functionality and the CSV import compatibility changes.

## CSV import compatibility

- Supports the combined `lindenprotokoll_consumption_*.csv` export by dispatching electricity, water and fuel records using the exported subtype.
- Restores illness abdominal-pain regions as well as notes.
- Explains that `lindenprotokoll_all_*.csv` is a metadata overview and does not contain the detailed fields needed for a complete restore. Use the category-specific meal, illness and consumption exports.
- Keeps the existing simplified CSV import templates supported.

## Versioning

- Home Assistant add-on version set to `0.4.25`.
- Existing 0.4.24 screenshot digitisation and electricity-statistics functionality is carried forward unchanged.

## Validation caveat

The source package's import changes were syntax-checked, but a complete CSV import execution test and Home Assistant add-on build/runtime test have not been verified in this environment. Back up `/data` before updating and test imports against a copy of your data first.
