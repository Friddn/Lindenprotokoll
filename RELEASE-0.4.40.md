# Lindenprotokoll 0.4.40 – Statistik Strom: Zeitraumgenaue Intervallberechnung

## Änderungen
- Strom-KPIs werden ausschließlich aus den Anteilen der Zählerintervalle innerhalb des ausgewählten Kalenderzeitraums berechnet.
- Eine vor dem Zeitraum liegende Ablesung bleibt technische Berechnungsgrundlage, ihr Verbrauchsanteil außerhalb des Zeitraums wird aber nicht mehr mitgezählt.
- Verbrauchs- und Einspeisungsintervalle werden unabhängig voneinander auf die Kalendertage verteilt.
- Tagesdurchschnitte beziehen sich auf die Kalendertage des ausgewählten Zeitraums; die Abdeckung durch gültige Zählerintervalle wird separat angezeigt.
- Wärmepumpenvergleich und Gesamtverbrauchsreihe verwenden dieselben expliziten Periodengrenzen wie die Stromstatistik.
- Auch Zeiträume ohne Ablesung innerhalb der Grenzen können aus umschließenden Zählerablesungen berechnet werden.

## Zusätzliche Fehlerkorrektur bei der Prüfung
- Integrierte 15-Minuten-Kurvenschätzungen werden im Wärmepumpenvergleich als interpolierte statt als echte Messtage gezählt. Sie bleiben als Stützpunkte für die Interpolation verfügbar.

## Prüfungen
- Python-Compile erfolgreich.
- JavaScript-Syntaxprüfung für das Statistik-Template erfolgreich.
- Tests: 14 bestanden, 1 übersprungen.
