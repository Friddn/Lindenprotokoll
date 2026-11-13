# Lindenprotokoll 0.4.20 – corrected feature update

## Included corrections

- Bumped the Home Assistant add-on version to `0.4.20` so the repository can advertise an update.
- Replaced browser-session-only PIN failure counting with persistent SQLite lockout state keyed by a one-way hash of the client address (the raw address is not stored). The shared PIN remains hashed in the database.
- Restricted the one-time PIN setup endpoint: after initial setup it redirects to Settings, where changing the PIN requires the current PIN.
- Added CSRF tokens to all server-rendered POST forms and validates them before processing. JSON endpoints validate the request Origin when the browser supplies it.
- Configured HttpOnly and SameSite=Lax session cookies and a 30-day session lifetime.
- Added a 15 MB request limit and image verification/dimension checks for uploaded screenshots.
- Re-running screenshot digitization now atomically replaces the candidate points belonging to that screenshot instead of accumulating duplicates.
- Reworked curve simplification to suppress pixel stair-step noise, retain corners and plateau boundaries, and only infer equal-valued plateau positions when a repeated pixel spacing is observed. All such positions remain estimates.
- Corrected the hourly data endpoint: daily non-heat-pump kWh values are no longer repeated at every hourly timestamp.
- The hourly dual-axis graph now compares actual hourly heat-pump electricity energy with Tornesch temperature and can overlay screenshot-derived electricity/thermal curve candidates as clearly labelled estimates.
- The daily chart keeps confirmed printed heat-pump totals separate from daily non-heat-pump values. Combined daily consumption is shown only when both are available.
- CSV imports validate timestamps, numeric finiteness and reasonable temperature/energy ranges.
- Removed Python bytecode/cache files from the package.

## Data semantics

- `daily_hp_totals.electricity_kwh`: manually confirmed printed heat-pump electricity total for the day. This is authoritative for daily heat-pump statistics.
- `daily_hp_totals.thermal_kwh`: manually confirmed printed thermal-energy total for the day.
- `daily_non_hp_electricity.kwh`: daily electricity consumption explicitly excluding the heat pump. It is not distributed over hours.
- `hp_hourly.electricity_kwh` and `thermal_kwh`: imported energy for each hour (kWh/hour interval). Numerically this corresponds to average kW over that hour, but the field remains energy.
- `hp_graph_points`: estimated curve geometry in kW, never treated as original measurements. Confidence and point kind distinguish corners, plateau boundaries and inferred plateau positions.
- Printed daily totals are not changed to force agreement with the reconstructed curve. Differences remain review items.

## Validation performed for this package

- `python -m compileall` passed for application Python files.
- Synthetic-image unit tests for the graph digitizer pass.
- Static template scan confirmed every server-rendered POST form includes a CSRF field.
- SQLite schema/migration syntax was inspected; existing tables are extended additively and no existing measurement rows are deleted by initialization.
- Full Flask test-client and Home Assistant add-on build/runtime tests could not be run in this environment because Flask is not installed and package installation is unavailable. Please run the add-on against a copy of `/data` before production deployment.

## GitHub / Home Assistant update

1. Create a branch, for example `fix-0.4.20-review`.
2. Upload the extracted contents of this ZIP to the repository root, preserving its existing folder structure.
3. Review the Git diff; do not upload the ZIP file itself.
4. Test the add-on build and app with a copy of the Home Assistant `/data` directory.
5. Merge into the default branch only after testing. Home Assistant normally tracks the repository's default branch.
6. Back up the live `/data` directory before installing the update. Never replace `/data` with ZIP contents.

## Known limitations

- Printed daily values are entered and confirmed manually. OCR is optional and does not automatically map recognized numbers to fields.
- Curve digitization is still a geometric estimate. A screenshot cannot reveal collinear intermediate source samples with certainty; plateau positions are inferred only when repeated spacing appears in the detected geometry. Do not treat these as original sensor samples.
- The chart currently uses Chart.js from a CDN, so the charts require network access to load Chart.js. The application data itself remains local.
- This is a shared-PIN gate for a trusted local network, not a replacement for HTTPS, VPN or Home Assistant access controls. Do not expose port 8100 directly to the public internet.
