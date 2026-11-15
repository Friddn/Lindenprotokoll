# Lindenprotokoll 0.4.37 – Screenshot-Speichern / SQLite-Migration

## Fehlerbehebung
- SQLite-Migration ergänzt `updated_at` nun ohne unzulässigen `DEFAULT CURRENT_TIMESTAMP`-Ausdruck und füllt bestehende Zeilen anschließend auf.
- Zahlenparser akzeptiert deutsches Dezimalkomma (`22,75`), Dezimalpunkt (`22.75`) sowie deutsche Tausender-/Dezimalschreibweise (`1.234,56`).
- Ungültige Zahleneingaben liefern eine verständliche Validierungsmeldung statt eines unerwarteten Serverfehlers.
- Bestehende Messwerte bleiben erhalten.
