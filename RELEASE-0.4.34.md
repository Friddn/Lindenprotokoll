# Lindenprotokoll 0.4.34 – Korrekturen

## OCR Wärmepumpe
- OCR-Werte werden stärker an die jeweilige Kennzeichnung gebunden.
- Thermische Energie wird nicht mehr als Wärmepumpen-Strom übernommen, wenn keine eindeutige Strom-Kennzeichnung erkannt wurde.
- Zusätzliche Bezeichnungen für elektrische Energie und Wärmemenge werden erkannt.
- OCR-Werte in Wh werden weiterhin in kWh normalisiert.

## Stromstatistik
- Wärmepumpen-Strom wird zwischen tatsächlich vorhandenen Tageswerten linear interpoliert.
- Vor dem ersten und nach dem letzten Wärmepumpen-Messwert wird Wärmepumpen-Strom mit 0 angesetzt.
- Gesamtverbrauch und Verbrauch ohne Wärmepumpe werden über den gesamten ausgewählten Zeitraum aufgebaut.
- Verbrauch ohne Wärmepumpe wird bei rechnerisch negativen Werten auf 0 begrenzt; die betroffenen Tage werden weiterhin als Warnung ausgewiesen.

## Deutsches Zahlenformat
- Dezimalwerte in Statistiken und Diagramm-Tooltips verwenden deutsche Formatierung mit Komma.
- Wärmepumpen-Tageswerte können mit Dezimalkomma eingegeben werden; Tausenderpunkte werden korrekt behandelt.
- Zahlen bleiben intern numerisch.

## Tests
- OCR-Tests: 9 erfolgreich.
- Gesamttests: 14 erfolgreich, 1 übersprungen (realer Screenshot/Tesseract-Test ohne verfügbare Testdatei).
- Python-Kompilierung und JavaScript-Syntaxprüfung erfolgreich.
