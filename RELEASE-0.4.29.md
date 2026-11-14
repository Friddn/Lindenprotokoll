# Lindenprotokoll 0.4.29 – Screenshot Manager

## Screenshot workflow

- Added a Screenshot Manager section on the heat-pump page.
- Supports uploading multiple screenshots in one operation.
- Added batch OCR for all screenshots without OCR text.
- Added filters for all, not yet OCR-processed, OCR available, digitized and exact duplicates.
- Added per-screenshot deletion with confirmation. Associated graph points are deleted; already confirmed daily totals are retained and detached from the deleted screenshot.
- Removed obsolete mandatory manual calibration fields from the curve digitization form; the current digitizer already invokes automatic calibration.
- OCR, review/confirmation and graph digitization remain separate steps to avoid silently importing unverified OCR results.

## Duplicate detection limitation

Duplicate detection currently identifies byte-identical files using SHA-256. Visually identical screenshots re-encoded or resized are not automatically classified as duplicates in this release.

## Validation

- Python source compilation and ZIP integrity checked.
- Live Home Assistant runtime and batch OCR require validation on the target add-on. OCR batch requires Tesseract and `eng` + `deu` language data in the container.
- Back up `/data` before updating.
