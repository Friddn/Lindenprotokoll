# Lindenprotokoll 0.4.33 – OCR precision and unit normalization

## Changes
- Added two-decimal precision to the editable heat-pump daily OCR fields (`step=0.01`).
- Renamed the per-screenshot OCR action to **OCR erneut ausführen**; it reruns local Tesseract against the original image and refreshes the OCR suggestion text. It does not write or overwrite confirmed daily totals.
- Extended OCR field extraction to recognize both `kWh` and `Wh` for heat-pump electricity and thermal energy, normalizing Wh values to kWh before presenting them for confirmation.
- Supports decimal comma and decimal point, plus common German thousands grouping such as `19.250 Wh` (= 19.25 kWh).
- Renamed the confirmation action to **Save and next**.

## Validation
- OCR parser tests: 11 passed, 1 skipped across the project test suite.
- Added regression tests for Wh conversion, German number formatting, and preservation of two decimal places.
- Live Tesseract recognition against the user's complete screenshot collection and Home Assistant browser rendering still require validation in the running installation.

## QA corrections within 0.4.33 (version unchanged)
- Fixed a JavaScript initialization-order error in the Wärmepumpe statistics renderer: `inconsistentDays` is now calculated before the KPI references it. This error could stop the daily chart from rendering.
- Fixed OCR extraction when both metric labels and values appear on the same text line; the thermal-energy parser now selects the value following its own label rather than reusing the electricity value.
- Added a screenshot image endpoint restricted to files inside the heat-pump screenshot storage directory, and displayed the original screenshot in each review item.
- Added optional from/to date filtering to screenshot management; it uses the graph date where available and otherwise the upload date.
- Regression suite after corrections: 12 passed, 1 skipped. Python compilation and static integration checks passed. Flask is not installed in this environment, so HTTP route/browser runtime tests remain outstanding.
