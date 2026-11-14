# Lindenprotokoll 0.4.27

## Import preview reliability

- Import preview data remains stored server-side in `/data/import_previews/`.
- The commit form now carries the opaque preview ID in a hidden field.
- Import commit no longer depends on the browser session cookie retaining the preview reference.
- Preview IDs are validated before filesystem access; preview JSON structure is checked before import.
- The current import type is still checked against the stored preview type.
- Existing import formats and the 0.4.24 OCR/graph and electricity statistics changes are carried forward.

## Deployment note

The screenshots supplied for this issue still show the older import selector (`Essen`, `Krank`, `Strom`, `Wasser`, `Auto`) and do not show the newer `Verbrauch-Export` selector. This strongly suggests the currently running Home Assistant add-on has not loaded the updated source package. After installing 0.4.27, verify that the add-on reports version 0.4.27 and that `Verbrauch-Export` appears.

## Validation

- Python source compilation: checked.
- ZIP archive integrity: checked.
- Full Home Assistant container build and live CSV import against the user's database: not run in this environment. Back up `/data` before updating.
