# Lindenprotokoll 0.4.38 – Dezimalparser und deutsches Zahlenformat

## Korrekturen
- Regex im Screenshot-Tageswerte-Parser korrigiert: Dezimal-Komma und Dezimal-Punkt werden nun tatsächlich erkannt (vorher wurden die Backslashes doppelt maskiert).
- Akzeptiert `22,75`, `22.75` und `1.234,56`.
- Jinja-Filter `de_number` ergänzt und OCR-vorausgefüllte Felder sowie bestätigte Tageswerte mit deutschem Dezimalkomma formatiert.
- Interne Speicherung und Berechnung bleiben numerisch mit Dezimalpunkt; nur die Darstellung verwendet deutsche Schreibweise.
