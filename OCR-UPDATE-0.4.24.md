# Lindenprotokoll 0.4.24 – automatic screenshot digitisation and electricity statistics

## Included

- Automatic chart grid detection and y-axis tick OCR for the supported heat-pump screenshot layout; no manual pixel/axis calibration fields are required for the automatic digitisation action.
- Automatic red thermal and green electrical curve tracing.
- Curve tracing now prioritizes the saturated line stroke over the translucent red area fill. This fixes the previous median-of-line-and-fill error that pulled the red curve downward.
- Fixed a variable-shadowing defect in plateau detection that could alter the y-axis denominator and understate later curve values.
- Automatic OCR extraction of the date and printed daily kWh totals. The user must still review and confirm the printed daily totals before they become authoritative daily records.
- Interpolated 15-minute curve estimates, explicitly marked as estimates and not original sensor readings. No extrapolation beyond the traced curve.
- Statistik (Strom) retains its existing charts, filters, KPIs and table, and adds heat-pump-only, total-only and combined daily views over the selected date interval.
- Heat-pump daily totals and average-per-data-day KPIs are calculated from available heat-pump days. Combined household totals are shown only when both heat-pump and non-heat-pump daily values are available; missing values are not treated as zero.

## Data semantics

- Green curve: estimated electrical power in kW.
- Red curve: estimated thermal power in kW.
- 15-minute samples: interpolated estimates from the screenshot curve geometry, not original source samples.
- Printed daily electricity/thermal values: OCR suggestions only until reviewed and confirmed.
- Daily heat-pump energy is taken from confirmed printed daily totals where present; otherwise imported hourly energy may be summed.
- Combined daily consumption is heat-pump energy plus explicitly imported daily non-heat-pump energy. Do not add heat-pump energy to an existing household meter total a second time.

## Validation performed

- `python -m pytest -q`: 8 tests passed.
- `python -m compileall -q lindenprotokoll/app`: passed.
- Real screenshot integration test (`IMG_9140.png`): automatic y-axis recognition found six ticks from 10 to 0 kW; both curves traced; green peak approximately 3.0 kW and red peak approximately 8.3 kW.
- Tesseract OCR on the real screenshot recognized 6 November 2026, 23.3 kWh electricity and 93.5 kWh thermal energy; parser returned the corresponding structured fields.

## Not verified in this environment

- Home Assistant add-on image build and runtime on the target Supervisor/architecture.
- Full browser-driven interaction with a production copy of the user's `/data` database.
- Automatic calibration is designed around the current heat-pump screenshot layout. Other chart layouts may be rejected rather than guessed; review all extracted estimates before using them for analysis.

Back up `/data` before installing. Install the add-on update without replacing the existing `/data` directory.

## CSV import compatibility update

- Added a dedicated `Verbrauch-Export` import path for the combined `lindenprotokoll_consumption_*.csv` export. It dispatches electricity, water and fuel rows by their exported `subtype`.
- The illness export now restores abdominal-pain `regions` as well as notes.
- The combined `lindenprotokoll_all_*.csv` metadata export is explicitly rejected with an explanation: it does not contain the detailed fields required to reconstruct entries. Use the meal, illness and consumption exports for restoration.
- Existing simplified CSV templates remain supported.
