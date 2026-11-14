# Lindenprotokoll 0.4.23 – OCR suggestions and 15-minute graph estimates

- Adds conservative parsing of German/English date strings and labelled daily kWh values from saved Tesseract output.
- Parsed values prefill the existing confirmation form; nothing is saved as confirmed until the user submits the form.
- Extends the existing calibrated graph digitiser with 15-minute linear interpolation between detected geometric candidates.
- Interpolated points are explicitly stored as `estimated-interpolated-15min`; they are estimates, not original sensor readings.
- Existing manual plot calibration remains required. Automatic chart-boundary/axis detection is not included in this update.
- No database schema changes. Existing source points and estimates are replaced atomically on re-digitisation, as before.

Validation: Python syntax and focused parser/resampling tests should be run before deployment. Docker/Home Assistant build must still be verified on the target Supervisor architecture.
