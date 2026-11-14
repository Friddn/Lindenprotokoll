# 0.4.25 import preview reliability fix

- Import previews are now stored as temporary JSON files in `/data/import_previews/`, not in Flask's browser cookie session.
- The session cookie stores only a random opaque preview identifier.
- Preview files are atomically written and abandoned previews are cleaned after two hours.
- Commit validates the preview identifier and import kind before importing.
- Existing data in `/data` and the database is not modified by preview creation.

Validation: Python syntax and ZIP integrity checked. Full runtime/import integration and Home Assistant add-on build are not verified in this environment.
