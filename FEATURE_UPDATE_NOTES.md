# Lindenprotokoll feature update

## Included in this update

- Retains the existing application pages, existing tables and existing records.
- Shared six-digit PIN gate from the prior update: initial PIN `000000`, mandatory replacement, hashed storage, Settings PIN change, session throttling, persistent random Flask secret in `/data`.
- New local SQLite tables for hourly heat-pump energy, hourly outdoor temperature, daily non-heat-pump electricity, archived graph screenshots and candidate graph points.
- New `/statistik/waermepumpe` page with an hourly heat-pump/temperature dual-axis chart and a daily chart for heat-pump / non-heat-pump / combined electricity.
- CSV imports are idempotent (same timestamp/date updates the existing row) and do not modify legacy meter readings.
- Original screenshots are archived under `/data/graph_screenshots`.

## CSV formats

Hourly heat pump: `timestamp,electricity_kwh,thermal_kwh`

Hourly weather: `timestamp,temperature_c`

Daily electricity excluding heat pump: `date,kwh,notes`

Use local ISO timestamps such as `2026-11-12T13:00`. Hourly electricity and thermal values are kWh within that hour. They are not instantaneous kW values.

## Important limitations / required validation

- Screenshot upload currently archives originals; automated curve digitization, axis calibration, sampling-cadence inference and candidate-point review are **not implemented yet**. No graph values are fabricated from the screenshots.
- Tornesch hourly historical weather values are not bundled. Import an hourly CSV for 2025 onward from a trusted source. The app does not silently call an external service.
- Heat-pump hourly values must be imported from the source/export or a separately validated screenshot-digitization workflow. The original screenshots alone do not currently populate the chart.
- Existing legacy electricity meter entries are meter readings and are not converted into hourly heat-pump consumption.
- The hourly chart plots kWh per hour (energy), while its right axis plots degrees Celsius. The daily chart sums the hourly heat-pump values and adds the daily non-heat-pump value. A daily total is shown only where the non-heat-pump daily value exists.

## Deployment

1. Back up the complete Home Assistant add-on `/data` directory and current add-on files.
2. Install this project version, keeping `/data` intact.
3. Start the add-on and verify existing pages, PIN login and settings.
4. Set a new PIN when prompted (do not keep `000000`).
5. Import CSV data and verify date/time zone and units before relying on charts.
6. If anything fails, restore the previous add-on version; do not delete `/data`.

The source was syntax-checked with `python -m py_compile`. Full runtime/integration tests against a live Home Assistant add-on and its production database have not been performed in this environment.
