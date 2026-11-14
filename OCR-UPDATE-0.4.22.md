# Lindenprotokoll 0.4.22 – OCR dependencies and diagnostics

- Adds Tesseract OCR plus German (`deu`) and English (`eng`) trained data to the add-on image.
- Adds `/settings/diagnostics`, linked in the navigation, to report Tesseract, language data, Pillow, and a real local OCR smoke test.
- OCR remains optional at application runtime: if the binary is missing, the existing screenshot workflow reports the problem instead of crashing.
- No database schema or `/data` files are changed by this update.

Build note: this package has not been built against the user's actual Home Assistant architecture in this environment. Verify the Docker base image and Alpine package availability by building on the target Supervisor before deployment.
