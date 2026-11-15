# Lindenprotokoll 0.4.30 – Electricity statistics visibility

## Fixes
- Moved the heat-pump daily statistics loader into the rendered statistics page content block. It now runs after its JavaScript function is declared.
- Changed the default Strom date range to **Alle**, so older cumulative electricity-meter readings are not silently hidden by a one-year default.
- Versioned the statistics local-storage key to prevent a previously saved one-year selection from overriding the new default.
- Existing meter readings remain in the original consumption tables and are not modified.

## Validation
- Python source compilation and ZIP integrity checked.
- Template checks confirm the loader call is inside the rendered content block and only invoked once.
- Live browser/Home Assistant validation remains required.
