# Lindenprotokoll 0.4.39 – Fix Screenshot-Review HTTP 500

## Ursache
Jinja2-Templates haben `request.args.get("review_id", type=int)` ausgewertet. Der Python-Typ `int` ist im Template-Kontext nicht definiert und verursachte `jinja2.exceptions.UndefinedError`.

## Korrektur
- Das Template verwendet nun den bereits von Flask/Python in `heatpump_analysis()` konvertierten Kontextwert `review_id`.
- Alle HTML-Templates wurden auf weitere `type=int`-Aufrufe geprüft; keine weiteren Treffer.
- Python- und Jinja2-Syntax geprüft.
- Vorherige Korrekturen aus 0.4.38 bleiben enthalten.
